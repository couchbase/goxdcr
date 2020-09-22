package pipeline_svc

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/goxdcr/parts"

	mcc "github.com/couchbase/gomemcached/client"

	"github.com/couchbase/goxdcr/service_def"
	utilities "github.com/couchbase/goxdcr/utils"

	"github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/metadata"

	component "github.com/couchbase/goxdcr/component"

	"github.com/couchbase/cbauth"

	mc "github.com/couchbase/gomemcached"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
)

var (
	// The number of goroutines that takes the merge result and connect to source and send the merged docu
	numConflictManagerWorkers = base.JSEngineWorkersPerNode * base.JSEngineThreadsPerWorker

	// This channel is a buffer before sending to resolverSvc. When the pipeline stop, data here will be discarded
	// and not stuck in the resolverSvc input channel.
	// We may implement resize of this channel by creating a new channel and draining and discard the existing one.
	conflictChannelSize = 1000 // TODO: MB-39063. What is the right size?

	// The channel size for resolverSvc to send back merge result to conflict manager goroutines.
	// TODO: MB-39063. What is the right size?
	// This needs to be bigger than ResolverSvc input channel so that it will not fill up and block ResolverSvc when the pipeline stops
	resultChannelsize = numConflictManagerWorkers * 20

	sourceBucketConnPoolSize = numConflictManagerWorkers
)

type DataMergedEventAdditional struct {
	Seqno          uint64
	Commit_time    time.Duration
	Resp_wait_time time.Duration
	Opcode         mc.CommandCode
	IsExpirySet    bool
	VBucket        uint16
	Req_size       int
	ManifestId     uint64
}

type DataMergeCasChangedEventAdditional struct {
	Seqno       uint64
	IsExpirySet bool
	VBucket     uint16
	ManifestId  uint64
}

type ConflictManager struct {
	*component.AbstractComponent
	pipeline          common.Pipeline
	top_svc           service_def.XDCRCompTopologySvc
	conn_str          string
	result_ch         chan *base.MergeInputAndResult
	conflict_ch       chan *base.ConflictParams
	finish_ch         chan bool
	wait_grp          sync.WaitGroup
	collectionEnabled uint32
	resolverSvc       service_def.ResolverSvcIface
	utils             utilities.UtilsIface
	sourceBucketName  string
	mergeFunction     string
	userAgent         string
}

type SubdocMutationPathSpec struct {
	opcode uint8
	flags  uint8
	path   []byte
	value  []byte
}

func (spec *SubdocMutationPathSpec) size() int {
	// 1B opcode, 1B flags, 2B path len, 4B value len
	return 8 + len(spec.path) + len(spec.value)
}
func NewConflictManager(resolverSvc service_def.ResolverSvcIface, replId string, top_svc service_def.XDCRCompTopologySvc, utils utilities.UtilsIface) *ConflictManager {
	return &ConflictManager{
		AbstractComponent: component.NewAbstractComponentWithLogger(replId, log.NewLogger("ConflictManager", log.DefaultLoggerContext)),
		top_svc:           top_svc,
		resolverSvc:       resolverSvc,
		utils:             utils,
		finish_ch:         make(chan bool, 1),
		collectionEnabled: 1, // TODO: MB-41120. Custom CR collection support. subdoc command will all fail with KEY_ENOENT if this is 0
	}
}

func (c *ConflictManager) Start(settingsMap metadata.ReplicationSettingsMap) (err error) {
	// ConflictManager depends on ResolverSvc to do the merge
	if c.resolverSvc.Started() == false {
		return fmt.Errorf("%v: Cannot start ConflictManager because ResolverSvc is not running.", c.pipeline.FullTopic())
	}
	c.conn_str, err = c.top_svc.MyMemcachedAddr()
	if err != nil {
		c.Logger().Errorf("%v: Failed to start conflictManager because MyMemcachedAddr returned error %v", c.pipeline.FullTopic(), err)
		return err
	}
	c.sourceBucketName = c.pipeline.Specification().GetReplicationSpec().SourceBucketName

	// At replication creation, we verify that the merge function is created. Here we only get its name from setting
	if mergeFunctionMapping, exists := settingsMap[base.MergeFunctionMappingKey]; exists {
		if f, ok := mergeFunctionMapping.(base.MergeFunctionMappingType); ok {
			c.mergeFunction = f[base.BucketMergeFunctionKey]
		} else {
			c.Logger().Errorf("Type of mergeFunctionMapping is %v", reflect.TypeOf(mergeFunctionMapping))
		}
	}
	if c.mergeFunction == "" {
		return fmt.Errorf("%v: Default merge function is not set for the pipeline.", c.pipeline.FullTopic())
	}
	c.result_ch = make(chan *base.MergeInputAndResult, resultChannelsize)
	c.conflict_ch = make(chan *base.ConflictParams, conflictChannelSize)
	for i := 0; i < numConflictManagerWorkers; i++ {
		c.wait_grp.Add(1)
		go c.conflictManagerWorker(i)
	}
	// Start the goroutine that will move data from conflict_ch to Resolver
	c.wait_grp.Add(1)
	go c.sendToResolverSvc()
	c.Logger().Infof("%v: ConflictManager started with merge function %v.", c.pipeline.FullTopic(), c.mergeFunction)
	return nil
}
func (c *ConflictManager) Stop() error {
	// c.result_ch/c.conflict_ch are not closed since ResolverSvc/XMEM could still be sending.
	// c.result_ch will be garbage collected once ResolverSvc is done with the items in its input channel from this pipeline
	// c.conflict_ch will be garbage collected Once XMEM is done
	c.Logger().Infof("%v: ConflictManager Stopping.", c.pipeline.FullTopic())
	close(c.finish_ch)
	c.wait_grp.Wait()
	base.ConnPoolMgr().RemovePool(c.getPoolName())
	c.Logger().Infof("%v: ConflictManager stopped.", c.pipeline.FullTopic())
	return nil
}
func (c *ConflictManager) Attach(pipeline common.Pipeline) error {
	c.Logger().Infof("Attach conflictManager with %v pipeline %v\n", pipeline.Type().String(), pipeline.FullTopic())
	c.Id()
	c.pipeline = pipeline
	c.userAgent = fmt.Sprintf("Goxdcr customCR bucket: %s", c.sourceBucketName)
	return nil
}
func (c *ConflictManager) Detach(pipeline common.Pipeline) error {
	return base.ErrorNotSupported
}

// TODO: MB-41120. Collection support. Make it sharable with backfill pipeline
func (c *ConflictManager) IsSharable() bool {
	return false
}
func (c *ConflictManager) UpdateSettings(settings metadata.ReplicationSettingsMap) error {
	return nil
}
func (c *ConflictManager) ResolveConflict(source *base.WrappedMCRequest, target *base.SubdocLookupResponse, sourceId, targetId []byte) error {
	aConflict := base.ConflictParams{source,
		target,
		sourceId,
		targetId,
		c.mergeFunction,
		c,
	}
	// Send to the larger conflict channel instead of ResolverSvc input channel so XMEM will not block
	select {
	case c.conflict_ch <- &aConflict:
	case <-c.finish_ch:
		return parts.PartStoppedError
	}
	return nil
}
func (c *ConflictManager) sendToResolverSvc() {
	defer c.wait_grp.Done()
	for {
		select {
		case aConflict := <-c.conflict_ch:
			c.resolverSvc.ResolveAsync(aConflict, c.finish_ch)
		case <-c.finish_ch:
			return
		}
	}
}
func (c *ConflictManager) conflictManagerWorker(id int) {
	c.Logger().Debugf("%v: id %v started.", c.pipeline.FullTopic(), id)
	var client *base.XmemClient
	defer func() {
		c.Logger().Debugf("%v: exit id %v", c.pipeline.FullTopic(), id)
		if client != nil {
			client.Close()
		}
		defer c.wait_grp.Done()
	}()
	// initializeClient() calls getClientWithRetry which has exponential backoff
	client, err := c.initializeClient()
	if err != nil {
		c.handleGeneralError(err)
		return
	}
	for {
		select {
		case v := <-c.result_ch:
			if v.Action == base.SetMergeToSource {
				err := v.Err
				result := v.Result
				mergedDoc, ok := result.(string)
				if err != nil || !ok {
					// TODO: Conflict feed
					c.Logger().Errorf("TODO: MB-39033. Custom CR: merge failed with error '%v' and Result %v. Add to conflict feed or stop replication", err, v.Result)
					continue
				}
				// set to source
				if c.Logger().GetLogLevel() >= log.LogLevelDebug {
					c.Logger().Debugf("%v: conflictManagerWorker %v received key %v for format and set", c.pipeline.FullTopic(), id, v.Input.Source.Req.Key)
					continue
				}
				req := c.formatMergedDoc(v.Input, []byte(mergedDoc))
				// TODO (MB-40143): Remove before CC shipping. The req should never be nil unless there are bugs in xattrs format.
				if req == nil {
					sourceMeta, _ := base.FindSourceCustomCRXattr(v.Input.Source.Req, v.Input.SourceId)
					targetMeta, _ := v.Input.Target.FindTargetCustomCRXattr(v.Input.TargetId)
					c.Logger().Errorf("formatMergedDoc Failed failed. err: %v, sourceMeta: %v, targetMeta: %v", err, sourceMeta, targetMeta)
					panic("mergeXattr failed")
				}
				c.sendDocument(id, v.Input, req, client)
			} else if v.Action == base.SetTargetToSource {
				req := c.formatTargetDoc(v.Input)
				c.sendDocument(id, v.Input, req, client)
			}
		case <-c.finish_ch:
			return
		}
	}
}
func (c *ConflictManager) formatTargetDoc(input *base.ConflictParams) *mc.MCRequest {
	bodylen := 0
	var spec SubdocMutationPathSpec
	specs := make([]SubdocMutationPathSpec, 0, 5)
	targetMeta, err := input.Target.FindTargetCustomCRXattr(input.TargetId)
	if err != nil {
		// TODO: MB-40143: Remove before CC shipping
		panic(fmt.Sprintf("error '%v' getting target meta", err))
	}
	// ID path
	id := append([]byte("\""), targetMeta.GetCvId()...)
	id = append(id, '"')
	spec = SubdocMutationPathSpec{uint8(base.SUBDOC_DICT_UPSERT), uint8(base.SUBDOC_FLAG_MKDIR_P | base.SUBDOC_FLAG_XATTR), []byte(base.XATTR_ID_PATH), id}
	specs = append(specs, spec)
	bodylen = bodylen + spec.size()

	// CV path. We use macro expansion to match the new CAS.
	spec = SubdocMutationPathSpec{uint8(base.SUBDOC_DICT_UPSERT), uint8(base.SUBDOC_FLAG_MKDIR_P | base.SUBDOC_FLAG_XATTR | base.SUBDOC_FLAG_EXPAND_MACROS), []byte(base.XATTR_CV_PATH), []byte(base.CAS_MACRO_EXPANSION)}
	specs = append(specs, spec)
	bodylen = bodylen + spec.size()

	// MV path
	mv := targetMeta.GetMv()
	if mv != nil {
		spec = SubdocMutationPathSpec{uint8(base.SUBDOC_DICT_UPSERT), uint8(base.SUBDOC_FLAG_MKDIR_P | base.SUBDOC_FLAG_XATTR), []byte(base.XATTR_MV_PATH), mv}
	} else {
		// TODO: MB-40143: Remove before CC shipping
		panic("Missing MV for setback")
	}
	specs = append(specs, spec)
	bodylen = bodylen + spec.size()
	// PCAS path
	pcas := targetMeta.GetPcas()
	if pcas != nil {
		spec = SubdocMutationPathSpec{uint8(base.SUBDOC_DICT_UPSERT), uint8(base.SUBDOC_FLAG_MKDIR_P | base.SUBDOC_FLAG_XATTR), []byte(base.XATTR_PCAS_PATH), pcas}
		specs = append(specs, spec)
		bodylen = bodylen + spec.size()
	}
	// body path
	body, _ := input.Target.ResponseForAPath("")
	if body != nil {
		spec = SubdocMutationPathSpec{uint8(mc.SET), uint8(0), []byte(""), body}
	} else {
		spec = SubdocMutationPathSpec{uint8(mc.DELETE), uint8(0), []byte(""), nil}
		c.Logger().Errorf("%v: Unexpected empty target document for key %v%v%v. LWW should have been used.", c.pipeline.FullTopic(), base.UdTagBegin, input.Target.Resp.Key, base.UdTagEnd)
		// TODO (MB-40143). Remove before CC shipping
		panic("Empty doc SetBack to source")
	}
	specs = append(specs, spec)
	bodylen = bodylen + spec.size()
	// TODO(MB-41808): data pool
	newbody := make([]byte, bodylen)
	req := c.composeRequestForSubdocMutation(specs, input.Source.Req, newbody, true)
	return req
}

func (c *ConflictManager) formatMergedDoc(input *base.ConflictParams, mergedDoc []byte) *mc.MCRequest {
	mv, pcas, err := c.mergeXattr(input)
	if err != nil {
		return nil
	}
	bodylen := 0
	specs := make([]SubdocMutationPathSpec, 0, 5)
	// ID path
	sourceId := "\"" + string(input.SourceId) + "\""
	spec := SubdocMutationPathSpec{uint8(base.SUBDOC_DICT_UPSERT), uint8(base.SUBDOC_FLAG_MKDIR_P | base.SUBDOC_FLAG_XATTR), []byte(base.XATTR_ID_PATH), []byte(sourceId)}
	bodylen = bodylen + spec.size()
	specs = append(specs, spec)
	// CV path
	spec = SubdocMutationPathSpec{uint8(base.SUBDOC_DICT_UPSERT), uint8(base.SUBDOC_FLAG_MKDIR_P | base.SUBDOC_FLAG_XATTR | base.SUBDOC_FLAG_EXPAND_MACROS), []byte(base.XATTR_CV_PATH), []byte(base.CAS_MACRO_EXPANSION)}
	bodylen = bodylen + spec.size()
	specs = append(specs, spec)
	// MV path
	if len(mv) > 0 {
		spec = SubdocMutationPathSpec{uint8(base.SUBDOC_DICT_UPSERT), uint8(base.SUBDOC_FLAG_MKDIR_P | base.SUBDOC_FLAG_XATTR), []byte(base.XATTR_MV_PATH), mv}
		bodylen = bodylen + spec.size()
		specs = append(specs, spec)
	}
	// PCAS path
	if len(pcas) > 0 {
		spec = SubdocMutationPathSpec{uint8(base.SUBDOC_DICT_UPSERT), uint8(base.SUBDOC_FLAG_MKDIR_P | base.SUBDOC_FLAG_MKDIR_P), []byte(base.XATTR_PCAS_PATH), pcas}
		bodylen = bodylen + spec.size()
		specs = append(specs, spec)
	}
	// body path
	spec = SubdocMutationPathSpec{uint8(mc.SET), uint8(0), nil, mergedDoc}
	bodylen = bodylen + spec.size()
	specs = append(specs, spec)
	// TODO(MB-41808): data pool
	newbody := make([]byte, bodylen)
	req := c.composeRequestForSubdocMutation(specs, input.Source.Req, newbody, true)
	return req
}
func (c *ConflictManager) sendDocument(id int, input *base.ConflictParams, req *mc.MCRequest, client *base.XmemClient) {
retry:
	select {
	case <-c.finish_ch:
		return
	default:
		sent_time := time.Now()
		err := c.sendWithRetry(client, base.XmemMaxRetry, req.Bytes())
		if err != nil {
			high_level_err := "Error writing documents to memcached in source cluster for custom CR."
			c.Logger().Errorf("%v: formatResultAndSend(id %v): %v. err=%v", c.pipeline.FullTopic(), id, high_level_err, err)
			c.handleGeneralError(errors.New(high_level_err))
			return
		}
		// TODO: MB-41122, the response handling below should be shared with XMEM
		response, err, _ := c.readFromClient(client, true)
		if err != nil {
			if err == base.FatalError {
				if response != nil {
					c.Logger().Warnf("%v: formatResultAndSend(id %v): received fatal error from subdoc_multi_mutation client for custom CR. req=%v, seqno=%v, response=%v\n", c.pipeline.FullTopic(), id, req, input.Source.Seqno, response)
				}
				c.handleGeneralError(err)
				return
			} else if err == base.BadConnectionError || err == base.ConnectionClosedError {
				c.Logger().Errorf("%v: formatResultAndSend(id %v): The subdoc_multi_mutation connection for custom CR is ruined. Repair the connection and retry.", c.pipeline.FullTopic(), id)
				c.repairConn(client, err.Error())
				goto retry
			} else {
				// Don't expect to go here.
				client.IncrementBackOffFactor()
				c.Logger().Errorf("%v: formatResultAndSend(id %v): subdoc_multi_mutation received error %v", c.pipeline.FullTopic(), id, err)
				goto retry
			}
		} else if response.Status != mc.SUCCESS {
			// The errors defined in base.IsIgnorableMCResponse() cannot be ignored here
			if base.IsMutationLockedError(response.Status) {
				goto retry
			} else if base.IsTemporaryMCError(response.Status) {
				client.IncrementBackOffFactor()
				c.Logger().Warnf("%v: formatResultAndSend(id %v): Received temporary error in subdoc_multi_mutation response for custom CR. Response status=%v, err=%v, response=%v%v%v\n",
					c.pipeline.FullTopic(), id, response.Status, err, base.UdTagBegin, response, base.UdTagEnd)
				goto retry
			} else if base.IsTopologyChangeMCError(response.Status) {
				vb_err := fmt.Errorf("Received error %v on vb %v\n", base.ErrorNotMyVbucket, req.VBucket)
				c.handleVBError(req.VBucket, vb_err)
			} else if base.IsCollectionMappingError(response.Status) {
				// TODO: MB-41120. Need to call upstreamErrReporter here similar to XMEM
				return
			} else if base.IsEExistsError(response.Status) || base.IsENoEntError(response.Status) {
				// request failed because source Cas changed or document no longer exist. We can ignore it since it will converge to new mutation.
				// TODO (MB-41102): This is cas locking failure. We need to make sure the newer mutation will not get rolled back
				req := input.Source.Req
				isExpirySet := false
				if len(req.Extras) >= 4 {
					isExpirySet = (binary.BigEndian.Uint32(req.Extras[:4]) != 0)
				}
				additionalInfo := DataMergeCasChangedEventAdditional{
					Seqno:       input.Source.Seqno,
					IsExpirySet: isExpirySet,
					VBucket:     req.VBucket,
					ManifestId:  input.Source.GetManifestId(),
				}
				c.RaiseEvent(common.NewEvent(common.MergeCasChanged, nil, c, nil, additionalInfo))
			} else {
				c.Logger().Errorf("%v sendDocument(id %v): received error response from subdoc_multi_mutation client for custom CR. Repairing connection. response status=%v, opcode=%v, seqno=%v, req.Key=%v%v%v, req.Cas=%v, req.Extras=%v\n",
					c.pipeline.FullTopic(), id, response.Status, response.Opcode, input.Source.Seqno, base.UdTagBegin, string(req.Key), base.UdTagEnd, req.Cas, req.Extras)
				err = fmt.Errorf("error response with startus %v from memcached", response.Status)
				c.repairConn(client, err.Error())
				goto retry
			}
		} else {
			req := input.Source.Req
			isExpirySet := false
			if len(req.Extras) >= 4 {
				isExpirySet = (binary.BigEndian.Uint32(req.Extras[:4]) != 0)
			}
			additionalInfo := DataMergedEventAdditional{
				Seqno:          input.Source.Seqno,
				Commit_time:    time.Since(input.Source.Start_time), // time from routing to merged and acknowledged by source cluster
				Resp_wait_time: time.Since(sent_time),
				Opcode:         req.Opcode,
				IsExpirySet:    isExpirySet,
				VBucket:        req.VBucket,
				Req_size:       req.Size(),
				ManifestId:     input.Source.GetManifestId(),
			}
			c.RaiseEvent(common.NewEvent(common.DataMerged, nil, c, nil, additionalInfo))
			if c.Logger().GetLogLevel() >= log.LogLevelDebug {
				c.Logger().Debugf("%v: sendDocument(id %v): Custom CR: sent document (key %s) to source bucket %v. Cas %v. VBucket %v Return status %v",
					c.pipeline.FullTopic(), id, req.Key, c.sourceBucketName, req.Cas, req.VBucket, response.Status)
			}
		}
	}
}

func (c *ConflictManager) mergeXattr(input *base.ConflictParams) (mv, pcas []byte, err error) {
	source := input.Source.Req
	sourceMeta, err := base.FindSourceCustomCRXattr(source, input.SourceId)
	if err != nil {
		c.Logger().Errorf("%v: Custom conflict resolution: mergeXattr() received error '%v' calling findSourceCustomCRXattr for source document body %v%v%v.",
			c.pipeline.FullTopic(), err, base.UdTagBegin, source.Body, base.UdTagEnd)
		return nil, nil, err
	}
	targetMeta, err := input.Target.FindTargetCustomCRXattr(input.TargetId)
	if err != nil {
		c.Logger().Errorf("%v: Custom conflict resolution: mergeXattr() received error '%v' calling findSourceCustomCRXattr for target document body %v%v%v.",
			c.pipeline.FullTopic(), err, base.UdTagBegin, input.Target.Resp.Body, base.UdTagEnd)
		return nil, nil, err
	}
	// "mv":{"<sourceId>":"<B64>","targetId>":"<B64>",...}
	sourceCasB64 := base.Uint64ToBase64(sourceMeta.GetCas())
	mvlen := 2*(len(sourceCasB64)+len(input.SourceId)+6) + 7 + len(sourceMeta.GetMv()) + len(targetMeta.GetMv())
	// TODO (MB-41808): data pool
	mergedMvSlice := make([]byte, mvlen)
	pcaslen := len(sourceMeta.GetPcas()) + len(targetMeta.GetPcas())
	mergedPcasSlice := make([]byte, pcaslen)
	mvlen, pcaslen, err = sourceMeta.MergeMeta(targetMeta, mergedMvSlice, mergedPcasSlice)
	if err != nil {
		c.Logger().Errorf("%v: Custom CR: failed to merge metadata for document key %v, error: %v", c.pipeline.FullTopic(), input.Source.Req.Key, err)
	} else {
		mv = mergedMvSlice[:mvlen]
		pcas = mergedPcasSlice[:pcaslen]
	}
	return
}

func (c *ConflictManager) getClientWithRetry() (mcc.ClientIface, error) {
	getClientOpFunc := func(param interface{}) (interface{}, error) {
		username, password, err := cbauth.GetMemcachedServiceAuth(c.conn_str)
		if err != nil {
			c.Logger().Errorf("%v: Failed to get client to memcached. GetMemcachedServiceAuth() call returned error: %v", c.pipeline.FullTopic(), err)
			return nil, err
		}
		poolName := c.getPoolName()
		pool, err := base.ConnPoolMgr().GetOrCreatePool(poolName, c.conn_str, c.sourceBucketName, username, password, sourceBucketConnPoolSize, true)
		if err != nil {
			c.Logger().Errorf("%v Failed to get client to memcached. GetOrCreatePool() call returned error: %v", c.pipeline.FullTopic(), err)
			return nil, err
		}
		return pool.GetNew(false)
	}

	result, err := c.utils.ExponentialBackoffExecutorWithFinishSignal("conflictManager.getClientWithRetry", base.XmemBackoffTimeNewConn, base.XmemMaxRetryNewConn,
		base.MetaKvBackoffFactor, getClientOpFunc, nil, c.finish_ch)
	if err != nil {
		return nil, err
	}
	client, ok := result.(mcc.ClientIface)
	if !ok {
		// should never get here
		return nil, fmt.Errorf("%v getClientWithRetry returned wrong type of client", c.pipeline.FullTopic())
	}
	return client, nil
}
func (c *ConflictManager) initializeClient() (*base.XmemClient, error) {
	client, err := c.getClientWithRetry()
	if err != nil {
		return nil, err
	}

	// Send HELO
	var features utilities.HELOFeatures
	features.Xattribute = true
	features.Xerror = true
	features.Collections = atomic.LoadUint32(&c.collectionEnabled) != 0
	// No compression since it is local
	_, err = c.utils.SendHELOWithFeatures(client, c.userAgent, base.XmemReadTimeout, base.XmemReadTimeout, features, c.Logger())
	if err != nil {
		client.Close()
		return nil, err
	}
	xmemClient := base.NewXmemClient("conflictManager", base.XmemReadTimeout,
		base.XmemReadTimeout, client,
		base.XmemMaxRetry, base.XmemMaxReadDownTime, c.Logger())
	return xmemClient, nil
}
func (c *ConflictManager) getPoolName() string {
	return c.pipeline.Topic() + "conflictManager/"
}

// If document body is included, it must be specified as the last path in the specs.
// If caslock is true, source.Cas must match the document. Otherwise it will fail with KEY_EEXIST
func (c *ConflictManager) composeRequestForSubdocMutation(specs []SubdocMutationPathSpec, source *mc.MCRequest, bodyslice []byte, caslock bool) *mc.MCRequest {
	// Each path has: 1B Opcode -> 1B flag -> 2B path length -> 4B value length -> path -> value
	pos := 0
	for i := 0; i < len(specs); i++ {
		bodyslice[pos] = specs[i].opcode // 1B opcode
		pos++
		bodyslice[pos] = specs[i].flags // 1B flag
		pos++
		binary.BigEndian.PutUint16(bodyslice[pos:pos+2], uint16(len(specs[i].path))) // 2B path length
		pos = pos + 2
		binary.BigEndian.PutUint32(bodyslice[pos:pos+4], uint32(len(specs[i].value))) // 4B value length
		pos = pos + 4
		n := copy(bodyslice[pos:], specs[i].path)
		pos = pos + n
		n = copy(bodyslice[pos:], specs[i].value)
		pos = pos + n
	}
	var cas uint64 = 0
	if caslock {
		cas = source.Cas
	}
	// We don't need ACCESS_DELETED in this command since we use LWW for deleted docs and the expected source doc should never be deleted here.
	// If user deleted it before we setback, we will get ENOENT error which is correct.
	// Set expiry
	var extras []byte
	if binary.BigEndian.Uint32(source.Extras[4:8]) != 0 {
		extras = source.Extras[4:8]
	}
	req := mc.MCRequest{Opcode: base.SUBDOC_MULTI_MUTATION,
		VBucket: source.VBucket,
		Key:     source.Key,
		Cas:     cas,
		Extras:  extras,
		Body:    bodyslice[:pos]}
	return &req
}

// TODO: MB-41122, consolidate with same in XMEM
func (c *ConflictManager) sendWithRetry(client *base.XmemClient, numOfRetry int, item_byte []byte) error {
	var err error
	for j := 0; j < numOfRetry; j++ {
		err, _ := c.writeToClient(client, item_byte, true)
		if err == nil {
			return nil
		} else if err == base.BadConnectionError {
			err = c.repairConn(client, err.Error())
			if err != nil {
				return err
			}
		}
	}
	return err
}

func (c *ConflictManager) repairConn(client *base.XmemClient, reason string) (err error) {
	if c.pipeline.State() != common.Pipeline_Running {
		c.Logger().Infof("%v is not running, no need to RepairConn", c.pipeline.FullTopic())
		return nil
	}
	c.Logger().Errorf("%v connection %v is broken due to %v, try to repair...", c.pipeline.FullTopic(), client.Name(), reason)
	memClient, err := c.getClientWithRetry()
	if err != nil {
		c.handleGeneralError(err)
		c.Logger().Errorf("%v - Failed to repair connections for %v. err=%v\n", c.pipeline.FullTopic(), client.Name(), err)
		return err
	}
	repaired := client.RepairConn(memClient, client.RepairCount(), c.pipeline.FullTopic(), c.finish_ch)
	if repaired {
		// Send HELO
		var features utilities.HELOFeatures
		features.Xattribute = true
		features.Xerror = true
		features.Collections = atomic.LoadUint32(&c.collectionEnabled) != 0
		// No compression since it is local
		_, err = c.utils.SendHELOWithFeatures(memClient, c.userAgent, base.XmemReadTimeout, base.XmemReadTimeout, features, c.Logger())
		if err != nil {
			client.Close()
			c.handleGeneralError(err)
			return err
		}
	}
	return nil
}

func (c *ConflictManager) writeToClient(client *base.XmemClient, bytes []byte, renewTimeout bool) (err error, rev int) {
	backoffFactor := client.GetBackOffFactor()
	if backoffFactor > 0 {
		time.Sleep(time.Duration(backoffFactor) * base.XmemBackoffWaitTime)
	}

	conn, rev, err := client.GetConn(false, renewTimeout)
	if err != nil {
		return err, rev
	}

	n, err := conn.Write(bytes)

	if err == nil {
		client.ReportOpSuccess()
		return err, rev
	} else {
		c.Logger().Errorf("%v: writeToClient error: %v", c.pipeline.FullTopic(), err)

		// repair connection if
		// 1. received serious net error like connection closed
		// or 2. sent incomplete data to target
		if c.utils.IsSeriousNetError(err) || (n != 0 && n != len(bytes)) {
			c.repairConn(client, err.Error())
		} else if base.IsNetError(err) {
			client.ReportOpFailure(false)
		}

		return err, rev
	}
}

func (c *ConflictManager) readFromClient(client *base.XmemClient, resetReadTimeout bool) (*mc.MCResponse, error, int) {
	// Gets a connection and also set whether or not to reset the readTimeOut
	_, rev, err := client.GetConn(resetReadTimeout, false)
	if err != nil {
		client.Logger().Errorf("%v: Can't read from client %v, failed to get connection, err=%v", client.Name(), c.pipeline.FullTopic(), err)
		return nil, err, rev
	}

	memClient := client.GetMemClient()
	if memClient == nil {
		return nil, errors.New("memcached client is not set"), client.RepairCount()
	}
	response, err := memClient.Receive()

	if err != nil {
		isAppErr := false
		var errMsg string = ""
		if err == response {
			isAppErr = true
		} else {
			errMsg = err.Error()
		}

		if !isAppErr {
			if err == io.EOF {
				return nil, base.ConnectionClosedError, rev
			} else if c.utils.IsSeriousNetError(err) {
				return nil, base.BadConnectionError, rev
			} else if base.IsNetError(err) {
				client.ReportOpFailure(true)
				return response, err, rev
			} else if strings.HasPrefix(errMsg, "bad magic") {
				//the connection to couchbase server is ruined. it is not supposed to return response with bad magic number
				//now the only sensible thing to do is to repair the connection, then retry
				client.Logger().Warnf("%v: xmemClient err=%v\n", c.pipeline.FullTopic(), err)
				return nil, base.BadConnectionError, rev
			}
		} else {
			//response.Status != SUCCESSFUL, in this case, gomemcached return the response as err as well
			//return the err as nil so that caller can differentiate the application error from transport
			//error
			return response, nil, rev
		}
	} else {
		//if no error, reset the client retry counter
		client.ReportOpSuccess()
	}
	return response, err, rev
}

func (c *ConflictManager) handleGeneralError(err error) {
	c.Logger().Errorf("%v Raise error condition %v\n", c.pipeline.FullTopic(), err)
	c.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, c, nil, err))
}

func (c *ConflictManager) handleVBError(vbno uint16, err error) {
	additionalInfo := &base.VBErrorEventAdditional{vbno, err, base.VBErrorType_Source}
	c.RaiseEvent(common.NewEvent(common.VBErrorEncountered, nil, c, nil, additionalInfo))
}

func (c *ConflictManager) NotifyMergeResult(input *base.ConflictParams, mergedResult interface{}, mergeError error) {
	select {
	case c.result_ch <- &base.MergeInputAndResult{base.SetMergeToSource, input, mergedResult, mergeError}:
	case <-c.finish_ch:
	}
}

func (c *ConflictManager) SetBackToSource(input *base.ConflictParams) error {
	select {
	case c.result_ch <- &base.MergeInputAndResult{base.SetTargetToSource, input, nil, nil}:
		return nil
	case <-c.finish_ch:
		return parts.PartStoppedError
	}
}
