// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package parts

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mc "github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
	base "github.com/couchbase/goxdcr/base"
	common "github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
	utilities "github.com/couchbase/goxdcr/utils"
)

const (
	// Developer injection section
	DCP_DEV_MAIN_ROLLBACK_VB     = base.DevMainPipelineRollbackTo0VB
	DCP_DEV_BACKFILL_ROLLBACK_VB = base.DevBackfillRollbackTo0VB
	// end developer injection section

	// start settings key name
	DCP_VBTimestamp           = "VBTimestamps"
	DCP_VBTimestampUpdater    = "VBTimestampUpdater"
	DCP_Connection_Prefix     = "xdcr:"
	EVENT_DCP_DISPATCH_TIME   = "dcp_dispatch_time"
	EVENT_DCP_DATACH_LEN      = "dcp_datach_length"
	DCP_Nozzle_Stats_Interval = "stats_interval"
	DCP_Priority              = "dcpPriority"
	DCP_VBTasksMap            = "VBTaskMap"
	DCP_EnableOSO             = "enableOSO"
)

type DcpStreamState int

const (
	Dcp_Stream_NonInit = iota
	Dcp_Stream_Init    = iota
	Dcp_Stream_Active  = iota
	Dcp_Stream_Closed  = iota
)

var dcp_inactive_stream_check_interval = 30 * time.Second

const checkInactiveStreamsTimeout = 1000 * time.Millisecond

const nonOkVbStreamEndCheckInterval = 300 * time.Second

const source_dead_conn_threshold = 2 * mcc.UPRDefaultNoopIntervalSeconds // threshold for Producer stuckness

var dcp_setting_defs base.SettingDefinitions = base.SettingDefinitions{DCP_VBTimestamp: base.NewSettingDef(reflect.TypeOf((*map[uint16]*base.VBTimestamp)(nil)), false)}

var ErrorEmptyVBList = errors.New("Invalid configuration for DCP nozzle. VB list cannot be empty.")

const (
	dcpFlagIgnoreTombstones uint32 = 0x80
	dcpFlagActiveVBOnly     uint32 = 0x10
)

type vbtsWithLock struct {
	ts   *base.VBTimestamp
	lock *sync.RWMutex
}

type streamStatusWithLock struct {
	state DcpStreamState
	lock  *sync.RWMutex
}

/**
 * DCP Rollback Handshake Helper. See MB-25647 for handshake sequence design
 */
type dcpStreamReqHelper struct {
	// Internal copy of vbno
	vbno uint16
	// Internal interface of dcp nozzle to access outer wrapper methods
	dcp DcpNozzleIface

	// Locks all internals except for currentVersionWell
	lock sync.RWMutex
	// Keeps track of messages sent. Key is version of number, Value is the seqno for the vbucket
	sentMsgs map[uint16]uint64
	// Keeps track of the seqno that has been ACK'ed. Key is the seqno, and value is whether or not it's been ack'ed
	ackedMsgs map[uint64]bool
	// Whether or not this helper is no longer usable
	isDisabled bool

	// Current version counter well - monotonously increasing - use atomics and not lock
	currentVersionWell uint64
}

func (reqHelper *dcpStreamReqHelper) initialize() {
	reqHelper.sentMsgs = make(map[uint16]uint64)
	reqHelper.ackedMsgs = make(map[uint64]bool)
	atomic.StoreUint64(&reqHelper.currentVersionWell, 0)
}

func (reqHelper *dcpStreamReqHelper) isStreamActiveNoLock() bool {
	if reqHelper.isDisabled {
		return false
	}

	status, err := reqHelper.dcp.GetStreamState(reqHelper.vbno)
	if err != nil {
		reqHelper.dcp.Logger().Errorf("Invalid Stream state for vbno: %v even though helper for vb exists.", reqHelper.vbno)
		return false
	}
	return (status == Dcp_Stream_Active)
}

func (reqHelper *dcpStreamReqHelper) getNewVersion() uint16 {
	var newVersion uint64

	newVersion = atomic.AddUint64(&reqHelper.currentVersionWell, 1)

	if newVersion > math.MaxUint16 {
		errStr := fmt.Sprintf("Error: dcpStreamHelper for dcp %v vbno: %v internal version overflow", reqHelper.dcp.Id(), reqHelper.vbno)
		reqHelper.lock.RLock()
		defer reqHelper.lock.RUnlock()
		if !reqHelper.isDisabled {
			// Something is seriously wrong if interal version overflowed
			reqHelper.dcp.Logger().Fatalf(errStr)
			reqHelper.dcp.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, reqHelper.dcp, nil, errStr))
		}
		atomic.StoreUint64(&reqHelper.currentVersionWell, 0)
		newVersion = 0
	}

	return uint16(newVersion)
}

// Gets the number of versions that has not been ack'ed
func (reqHelper *dcpStreamReqHelper) getNumberOfOutstandingReqs() int {
	reqHelper.lock.RLock()
	defer reqHelper.lock.RUnlock()
	// Find number of un-ack'ed msgs
	var count int
	for _, seqno := range reqHelper.sentMsgs {
		if !reqHelper.ackedMsgs[seqno] {
			count++
		}
	}
	return count
}

// Write lock must be held
func (reqHelper *dcpStreamReqHelper) deregisterRequestNoLock(version uint16) {
	seqno, ok := reqHelper.sentMsgs[version]
	if ok {
		// Mark that this seqno has been ack'ed
		reqHelper.ackedMsgs[seqno] = true
	}
}

/**
 * Register a sent request into the map for book-keeping
 */
func (reqHelper *dcpStreamReqHelper) registerRequest(version uint16, seqno uint64) (alreadyAcked bool, helperErr error) {
	reqHelper.lock.Lock()
	defer reqHelper.lock.Unlock()
	if reqHelper.isDisabled {
		helperErr = reqHelper.getDisabledError()
		return
	}
	if reqHelper.isStreamActiveNoLock() {
		alreadyAcked = true
	} else {
		alreadyAcked = reqHelper.ackedMsgs[seqno]
	}

	if !alreadyAcked {
		reqHelper.sentMsgs[version] = seqno
	}
	return
}

/**
 * Processes a rollback response.
 * Returns error if there's any issues with look-ups
 * Returns a bool to represent whether or not to ignore this response
 */
func (reqHelper *dcpStreamReqHelper) processRollbackResponse(version uint16) (ignoreResponse bool, helperErr error) {
	reqHelper.lock.Lock()
	defer reqHelper.lock.Unlock()

	if reqHelper.isDisabled {
		helperErr = reqHelper.getDisabledError()
		return
	}
	// default to not ignore response
	ignoreResponse = false
	var acked bool

	if reqHelper.isStreamActiveNoLock() {
		// If the vb stream is active already, ignore all rollback requests
		ignoreResponse = true
	} else {
		// Check to see if this seqno has been rejected "sent back a ROLLBACK" before. If so, then this is already handled.
		seqno, seqok := reqHelper.sentMsgs[version]
		if !seqok {
			helperErr = errors.New(fmt.Sprintf("Received a ROLLBACK message for vbno=%v with version=%v, but was never sent.",
				reqHelper.vbno, version))
			reqHelper.dcp.Logger().Warnf(helperErr.Error())
			ignoreResponse = true
			return
		}
		acked = reqHelper.ackedMsgs[seqno]

		if acked {
			ignoreResponse = true
		}

		if reqHelper.sentMsgs[version] == 0 {
			// It is weird that we sent out a rollbackseqno of 0 to DCP and it tells us to rollback again.
			// This should not happen. Restart pipeline. It has the same effect of panic where XDCR DCP nozzles restart.
			helperErr = errors.New(fmt.Sprintf("Received a ROLLBACK message for vbno=%v with seqno=%v, last sent was 0",
				reqHelper.vbno, reqHelper.sentMsgs[version]))
		}
	}

	if !acked {
		reqHelper.deregisterRequestNoLock(version)
	}
	return
}

/**
 * When a success is returned, this helper will remove the recorded response and make future
 * rollback operations no-op.
 * All history is reset until a registerRequest is called again
 */
func (reqHelper *dcpStreamReqHelper) processSuccessResponse(version uint16) {
	reqHelper.lock.Lock()
	defer reqHelper.lock.Unlock()

	if reqHelper.isDisabled {
		return
	}

	// reset stats
	reqHelper.initialize()
}

func (reqHelper *dcpStreamReqHelper) disable() {
	reqHelper.lock.Lock()
	defer reqHelper.lock.Unlock()
	reqHelper.isDisabled = true
	reqHelper.dcp = nil
}

func (reqHelper *dcpStreamReqHelper) getDisabledError() error {
	return fmt.Errorf("vbReqHelper %v is disabled", reqHelper.vbno)
}

type DcpNozzleIface interface {
	common.Nozzle
	CollectionEnabled() bool
	GetStreamState(vbno uint16) (DcpStreamState, error)
	PrintStatusSummary()

	// Embedded from AbstractPart
	Logger() *log.CommonLogger
}

/*
***********************************
/* struct DcpNozzle
************************************
*/
type DcpNozzle struct {
	AbstractPart

	// the list of vbuckets that the dcp nozzle is responsible for
	// this allows multiple  dcp nozzles to be created for a kv node
	vbnosLock sync.RWMutex
	vbnos     []uint16

	backfillTasksVbnosParseOnce sync.Once
	backfillTasksVbnos          []uint16

	vb_stream_status map[uint16]*streamStatusWithLock

	// immutable fields
	sourceBucketName string
	targetBucketName string
	client           mcc.ClientIface
	lock_client      sync.Mutex // lock to avoid client connection leak
	uprFeed          mcc.UprFeedIface
	// lock on uprFeed to avoid race condition
	lock_uprFeed sync.RWMutex

	finch        chan bool
	closeFinOnce sync.Once

	bOpen      bool
	lock_bOpen sync.RWMutex

	childrenWaitGrp sync.WaitGroup

	counter_compressed_received uint64
	counter_received            uint64
	counter_sent                uint64

	// the number of check intervals after which dcp still has inactive streams
	// inactive streams will be restarted after this count exceeds MaxCountStreamsInactive
	counter_streams_inactive uint32

	start_time          time.Time
	handle_error        bool
	cur_ts              map[uint16]*vbtsWithLock
	vbtimestamp_updater func(uint16, uint64) (*base.VBTimestamp, error)

	// Each vb stream has its own helper to help with DCP handshaking
	vbHandshakeMap map[uint16]*dcpStreamReqHelper

	xdcr_topology_svc service_def.XDCRCompTopologySvc
	// stats collection interval in milliseconds
	stats_interval              uint32
	stats_interval_change_ch    chan bool
	user_agent                  string
	is_capi                     bool
	utils                       utilities.UtilsIface
	memcachedCompressionSetting base.CompressionType
	uprFeedCompressionSetting   base.CompressionType

	dcpPrioritySetting mcc.PriorityType
	lockSetting        sync.RWMutex

	specificManifestGetter service_def.CollectionsManifestReqFunc

	// Non-StreamID based vb stream will keep track of the top received manifest ID from DCP
	// This is to ensure that the mutations coming in from DCP can be matched with the correct
	// manifest ID
	// DCP will only bump the manifest (via a system event) if all older manifest has been sent
	vbHighestManifestUidArray [base.NumberOfVbs]uint64

	collectionEnabled uint32
	// If backfill pipeline, then could be passed in a vb task map
	specificVBTasks   *metadata.VBTasksMapType
	specificVBTaskLen int // cached for quick look up

	// Datapools for reusing memory
	wrappedUprPool          utilities.WrappedUprPoolIface
	collectionNamespacePool utilities.CollectionNamespacePoolIface

	endSeqnoForDcp                map[uint16]*base.SeqnoWithLock // The last seqno that we have received from DCP
	backfillTaskEndSeqno          map[uint16]*base.SeqnoWithLock // seqno supposed to end for a VB task
	backfillTaskStreamCloseSent   map[uint16]*uint32
	backfillTaskStreamEndReceived map[uint16]*uint32

	osoRequested           bool
	getHighSeqnoOneAtATime chan bool
	vbHighSeqnoMap         map[uint16]*base.SeqnoWithLock
	savedMcReqFeatures     utilities.HELOFeatures
	isCollectionsMigration bool

	devInjectionMainRollbackVb     int // -1 means not enabled
	devInjectionBackfillRollbackVb int // -1 means not enabled

	enablePurgeRollback bool // If user wants DCP to send rollback due to purge moving ahead of ckpt

	nonOKVbStreamEnds    map[uint16]bool
	nonOKVbStreamEndsMtx sync.Mutex
}

func NewDcpNozzle(id string,
	sourceBucketName, targetBucketName string,
	vbnos []uint16,
	xdcr_topology_svc service_def.XDCRCompTopologySvc,
	is_capi bool,
	logger_context *log.LoggerContext,
	utilsIn utilities.UtilsIface,
	specificManifestGetter service_def.CollectionsManifestReqFunc) *DcpNozzle {

	part := NewAbstractPartWithLogger(id, log.NewLogger("DcpNozzle", logger_context))

	dcp := &DcpNozzle{
		sourceBucketName:               sourceBucketName,
		targetBucketName:               targetBucketName,
		vbnosLock:                      sync.RWMutex{},
		vbnos:                          vbnos,
		AbstractPart:                   part, /*AbstractPart*/
		bOpen:                          true, /*bOpen	bool*/
		lock_bOpen:                     sync.RWMutex{},
		childrenWaitGrp:                sync.WaitGroup{}, /*childrenWaitGrp sync.WaitGroup*/
		lock_client:                    sync.Mutex{},
		lock_uprFeed:                   sync.RWMutex{},
		cur_ts:                         make(map[uint16]*vbtsWithLock),
		vb_stream_status:               make(map[uint16]*streamStatusWithLock),
		xdcr_topology_svc:              xdcr_topology_svc,
		stats_interval_change_ch:       make(chan bool, 1),
		is_capi:                        is_capi,
		utils:                          utilsIn,
		vbHandshakeMap:                 make(map[uint16]*dcpStreamReqHelper),
		dcpPrioritySetting:             mcc.PriorityDisabled,
		collectionNamespacePool:        utilities.NewCollectionNamespacePool(),
		specificManifestGetter:         specificManifestGetter,
		endSeqnoForDcp:                 make(map[uint16]*base.SeqnoWithLock),
		getHighSeqnoOneAtATime:         make(chan bool, 1),
		vbHighSeqnoMap:                 make(map[uint16]*base.SeqnoWithLock),
		wrappedUprPool:                 utilities.NewWrappedUprPool(base.NewDataPool()),
		finch:                          make(chan bool),
		devInjectionBackfillRollbackVb: -1,
		devInjectionMainRollbackVb:     -1,
		backfillTaskEndSeqno:           make(map[uint16]*base.SeqnoWithLock),
		backfillTaskStreamCloseSent:    make(map[uint16]*uint32),
		backfillTaskStreamEndReceived:  make(map[uint16]*uint32),
		nonOKVbStreamEnds:              make(map[uint16]bool),
	}

	// Allow one caller the ability to execute
	dcp.getHighSeqnoOneAtATime <- true

	for _, vbno := range vbnos {
		dcp.cur_ts[vbno] = &vbtsWithLock{lock: &sync.RWMutex{}, ts: nil}
		dcp.vb_stream_status[vbno] = &streamStatusWithLock{lock: &sync.RWMutex{}, state: Dcp_Stream_NonInit}
		dcp.endSeqnoForDcp[vbno] = base.NewSeqnoWithLock()
		dcp.vbHighSeqnoMap[vbno] = base.NewSeqnoWithLock()
		dcp.backfillTaskEndSeqno[vbno] = base.NewSeqnoWithLock()
		var seqnoEndForVB uint32
		dcp.backfillTaskStreamCloseSent[vbno] = &seqnoEndForVB
		var streamEndReceived uint32
		dcp.backfillTaskStreamEndReceived[vbno] = &streamEndReceived
	}

	dcp.composeUserAgent()

	dcp.Logger().Debugf("Constructed Dcp nozzle %v with vblist %v\n", dcp.Id(), vbnos)

	return dcp

}

func (dcp *DcpNozzle) composeUserAgent() {
	dcp.user_agent = base.ComposeUserAgentWithBucketNames("Goxdcr Dcp ", dcp.sourceBucketName, dcp.targetBucketName)
}

func (dcp *DcpNozzle) CollectionEnabled() bool {
	return atomic.LoadUint32(&dcp.collectionEnabled) != 0
}

// Given the list of features, if a specific user requested feature is not essential to pipeline uptime
// and may be a cause of the error, respond it back as high-priority so Pipeline can restart without it
func (dcp *DcpNozzle) prioritizeReturnErrorByFeatures(requested mcc.UprFeatures, responded mcc.UprFeatures) error {
	if requested.CompressionType != responded.CompressionType {
		if requested.CompressionType == base.CompressionTypeNone && responded.CompressionType == base.CompressionTypeSnappy {
			dcp.Logger().Warnf(fmt.Sprintf("%v did not request compression, but DCP responded with compression type %v\n",
				dcp.Id(), base.CompressionTypeStrings[responded.CompressionType]))
			/**
			 * The contract with KV team is that in order for DCP to have compression turned on, the underlying Memcached
			 * connection must also have compression enabled. Because XDCR did not request compression, it means that the underlying
			 * memcached connection is *not* compression enabled. By the contract, this DCP connection should be invalidated and not
			 * used, to avoid situations where KV may send compressed data over but not set the SNAPPY data type flag
			 */
			return base.ErrorCompressionDcpInvalidHandshake

		} else if requested.CompressionType == base.CompressionTypeSnappy && responded.CompressionType == base.CompressionTypeNone {
			dcp.Logger().Warnf(fmt.Sprintf("%v requested compression type %v, but DCP responded with compression type %v\n",
				dcp.Id(), base.CompressionTypeStrings[requested.CompressionType], base.CompressionTypeStrings[responded.CompressionType]))
			return base.ErrorCompressionNotSupported
		}
	}
	return nil
}

func (dcp *DcpNozzle) initializeMemcachedClient(settings metadata.ReplicationSettingsMap) error {
	var dcpMcReqFeatures utilities.HELOFeatures
	var respondedFeatures utilities.HELOFeatures
	var err error
	var addr string

	addr, err = dcp.xdcr_topology_svc.MyMemcachedAddr()
	if err != nil {
		return err
	}

	_, disableCollection := settings[ForceCollectionDisableKey]
	if !disableCollection {
		dcpMcReqFeatures.Collections = true
		atomic.StoreUint32(&dcp.collectionEnabled, 1)
	}

	dcpMcReqFeatures.CompressionType = dcp.memcachedCompressionSetting
	dcp.savedMcReqFeatures = dcpMcReqFeatures

	dcp.lock_client.Lock()
	if dcp.state == common.Part_Stopping || dcp.state == common.Part_Stopped {
		// If xmem start is slow, dcp can still be initializing when pipeline start times out and is being stopped. Don't create DCP connection to avoid connection leak
		dcp.lock_client.Unlock()
		return fmt.Errorf("Skipping creating client since DCP is stopping.")
	}
	dcp.client, respondedFeatures, err = dcp.utils.GetMemcachedConnectionWFeatures(addr, dcp.sourceBucketName, dcp.user_agent, base.KeepAlivePeriod, dcpMcReqFeatures, dcp.Logger())
	dcp.lock_client.Unlock()

	if err == nil && (dcp.memcachedCompressionSetting != base.CompressionTypeNone) && (respondedFeatures.CompressionType != dcp.memcachedCompressionSetting) {
		dcp.Logger().Errorf("%v Attempting to send HELO with compression type: %v, but received response with %v",
			dcp.Id(), dcp.memcachedCompressionSetting, respondedFeatures.CompressionType)
		// Let dcp.Stop() take care of client.Close()
		return base.ErrorCompressionNotSupported
	}

	if err == nil && dcpMcReqFeatures.Collections && !respondedFeatures.Collections {
		dcp.Logger().Errorf("Collections not supported")
		return base.ErrorNotSupported
	}

	return err
}

func (dcp *DcpNozzle) initializeUprFeed() error {
	var err error
	dcp.lock_uprFeed.Lock()
	defer dcp.lock_uprFeed.Unlock()

	select {
	case <-dcp.finch:
		return fmt.Errorf("Stop() has already been executed prior to initializeUprFeed")
	default:
		break
	}

	// xdcr will send ack to upr feed
	dcp.uprFeed, err = dcp.client.NewUprFeedWithConfigIface(true /*ackByClient*/)
	if err != nil {
		return err
	}

	randName, err := base.GenerateRandomId(base.LengthOfRandomId, base.MaxRetryForRandomIdGeneration)
	if err != nil {
		return err
	}

	uprFeedName := DCP_Connection_Prefix + dcp.Id() + ":" + randName
	if len(uprFeedName) > base.MaxDcpConnectionNameLength {
		// dcp.Id() looks like dcp_65713400edc789b05d26283428a2af88/B1/B2_127.0.0.1:12000_0
		// and it contains two bucket names up to 100 bytes each. That's the part we will trim so the uprFeedName will be under limit
		// The uprFeedName will still be unique because of the randName part
		trimLen := len(uprFeedName) - base.MaxDcpConnectionNameLength
		id := dcp.Id()
		index := strings.LastIndexByte(id, '/')
		var idPart string
		if index > trimLen {
			// We will trim the end of source bucket name
			idPart = string(id[:index-trimLen]) + string(id[index:])
		} else {
			// This should never happen. But just in case, we will trim the end of the id
			idLen := len(id) - trimLen
			idPart = string(id[:idLen])
		}
		uprFeedName = DCP_Connection_Prefix + idPart + ":" + randName
	}

	if dcp.is_capi {
		// no need to enable features for capi replication
		err = dcp.uprFeed.UprOpen(uprFeedName, uint32(0), base.UprFeedBufferSize)
	} else {
		var uprFeatures mcc.UprFeatures
		// always enable xattr for xmem replication
		// even if target cluster does not support xattr, we still need to get xattr data type from dcp
		// for source side conflict resolution
		uprFeatures.Xattribute = true
		uprFeatures.CompressionType = (int)(dcp.uprFeedCompressionSetting)
		uprFeatures.DcpPriority = dcp.getDcpPrioritySetting()
		uprFeatures.IncludeDeletionTime = true
		uprFeatures.EnableExpiry = true
		uprFeatures.EnableOso = dcp.osoRequested
		uprFeatures.EnableStreamId = false
		uprFeatures.SendStreamEndOnClose = !dcp.isMainPipeline() && !dcp.osoRequested // nozzle could initiate streamClose
		if err = uprFeatures.EnableDeadConnDetection(source_dead_conn_threshold); err != nil {
			return err
		}

		if dcp.uprFeed == nil {
			err = fmt.Errorf("%v uprfeed is nil\n", dcp.Id())
			return err
		}
		featuresErr, activatedFeatures := dcp.uprFeed.UprOpenWithFeatures(uprFeedName, uint32(0) /*seqno*/, base.UprFeedBufferSize, uprFeatures)
		if featuresErr != nil {
			err = featuresErr
			dcp.Logger().Errorf("Trying to activate UPRFeatures received error code: %v", err.Error())
			// We do not know what error code UprOpen() returned. But in the case where compression is not activated,
			// it is something that XDCR can control. So at least override the error so that we can restart without
			// compression enabled, and see if there is still anything else that could have caused errors
			prioritizedErr := dcp.prioritizeReturnErrorByFeatures(uprFeatures, activatedFeatures)
			if prioritizedErr != nil {
				dcp.Logger().Errorf("An enabled feature may have caused the error. Overriding error to: %v", prioritizedErr)
				err = prioritizedErr
			}
		}
	}

	if err != nil {
		dcp.Logger().Errorf("%v upr open failed. err=%v.\n", dcp.Id(), err)
	}

	return err
}

func (dcp *DcpNozzle) initializeCompressionSettings(settings metadata.ReplicationSettingsMap) error {
	compressionVal, ok := settings[SETTING_COMPRESSION_TYPE].(base.CompressionType)
	if !ok {
		// Unusual case
		dcp.Logger().Warnf("%v missing compression type setting. Defaulting to ForceUncompress")
		compressionVal = base.CompressionTypeForceUncompress
	}

	switch compressionVal {
	case base.CompressionTypeNone:
		// For DCP - None means "Enable memcached snappy HELO but don't force DCP to compress"
		dcp.memcachedCompressionSetting = base.CompressionTypeSnappy
		dcp.uprFeedCompressionSetting = base.CompressionTypeNone
	case base.CompressionTypeSnappy:
		// Snappy means force DCP to compress using snappy
		dcp.memcachedCompressionSetting = base.CompressionTypeSnappy
		dcp.uprFeedCompressionSetting = base.CompressionTypeSnappy
	case base.CompressionTypeForceUncompress:
		// This is only used when target cannot receive snappy data
		dcp.memcachedCompressionSetting = base.CompressionTypeNone
		dcp.uprFeedCompressionSetting = base.CompressionTypeNone
	default:
		return base.ErrorCompressionNotSupported
	}
	return nil
}

func (dcp *DcpNozzle) initialize(settings metadata.ReplicationSettingsMap) (err error) {
	err = dcp.initializeCompressionSettings(settings)

	if val, ok := settings[DCP_Priority]; ok {
		dcp.setDcpPrioritySetting(val.(mcc.PriorityType))
	}

	if val, ok := settings[metadata.EnableDcpPurgeRollback].(bool); ok {
		dcp.enablePurgeRollback = val
	}

	dcp.initializeDevInjections(settings)

	dcp.initializeUprHandshakeHelpers()

	err = dcp.initializeMemcachedClient(settings)
	if err != nil {
		return err
	}

	// fetch start timestamp from settings
	dcp.vbtimestamp_updater = settings[DCP_VBTimestampUpdater].(func(uint16, uint64) (*base.VBTimestamp, error))

	if val, ok := settings[DCP_Nozzle_Stats_Interval]; ok {
		dcp.setStatsInterval(uint32(val.(int)))
	} else {
		return errors.New("setting 'stats_interval' is missing")
	}

	if !dcp.CollectionEnabled() {
		if dcp.is_capi {
			dcp.Logger().Warnf("Collections is not supported due to the use of CAPI nozzle - only the default collection on the source will be used for replication")
		}
		err = dcp.checkDefaultCollectionExistence()
	}
	if err != nil {
		return err
	}

	vbTasksRaw, exists := settings[DCP_VBTasksMap]
	if exists {
		dcp.specificVBTasks = vbTasksRaw.(*metadata.VBTasksMapType)
		dcp.specificVBTaskLen = dcp.specificVBTasks.Len()
		var atLeastOneMigrationTask bool
		var allMigrationTasks bool

		outputEndSeqnoMap := make(map[uint16]uint64)
		dcp.specificVBTasks.GetLock().RLock()
		originalVBsSorted := base.SortUint16List(dcp.ResponsibleVBs())
		var replacementVbnos []uint16
		for vb, vbTasks := range dcp.specificVBTasks.VBTasksMap {
			if _, vbIsResponsible := base.SearchUint16List(originalVBsSorted, vb); !vbIsResponsible {
				continue
			}
			if vbTasks != nil && vbTasks.Len() > 0 {
				topTask, exists, unlockFunc := vbTasks.GetRO(0)
				if !exists {
					// odd race
					unlockFunc()
					continue
				}
				endSeqno := topTask.GetEndingTimestampSeqno()
				unlockFunc()
				outputEndSeqnoMap[vb] = endSeqno
				replacementVbnos = append(replacementVbnos, vb)
				vbTasks.GetLock().RLock()
				for _, task := range vbTasks.List {
					requestedCollections, unlockFunc2 := task.RequestedCollections(false)
					for _, colNsMapping := range requestedCollections {
						for srcMapping, _ := range colNsMapping {
							if !atLeastOneMigrationTask && srcMapping.GetType() == metadata.SourceDefaultCollectionFilter {
								atLeastOneMigrationTask = true
								allMigrationTasks = true // until it isn't
							} else if atLeastOneMigrationTask && srcMapping.GetType() == metadata.SourceCollectionNamespace {
								allMigrationTasks = false
							}
						}
					}
					unlockFunc2()
				}
				vbTasks.GetLock().RUnlock()
			}
		}
		replacementSorted := base.SortUint16List(replacementVbnos)
		if !base.AreSortedUint16ListsTheSame(originalVBsSorted, replacementSorted) {
			removed, _, _ := base.ComputeDeltaOfUint16Lists(originalVBsSorted, replacementSorted, false)
			dcp.Logger().Infof("VBTasksMap only has a subset of original VBs. Replaced %v with %v by removing %v", originalVBsSorted, replacementSorted, removed)
			for _, vbRemoved := range removed {
				go dcp.RaiseEvent(common.NewEvent(common.StreamingBypassed, vbRemoved, dcp, nil, nil))
			}
			dcp.setResponsibleVBs(replacementVbnos)
		}
		dcp.specificVBTasks.GetLock().RUnlock()

		if !atLeastOneMigrationTask {
			dcp.Logger().Infof("%v Received backfill tasks for vb to endSeqno: %v", dcp.Id(), outputEndSeqnoMap)
		} else {
			dcp.isCollectionsMigration = true
			if !allMigrationTasks {
				dcp.Logger().Warnf(fmt.Sprintf("Weird mix of VBTasks - some are migration and some are not: %v", dcp.specificVBTasks.DebugString()))
			}
			dcp.Logger().Infof("%v Received non-ending migration tasks", dcp.Id())
		}
	}

	switch base.GlobalOSOSetting {
	case base.GlobalOSOOff:
		dcp.osoRequested = false
	default:
		osoMode, osoExists := settings[DCP_EnableOSO]
		if osoExists {
			dcp.osoRequested = osoMode.(bool)
		}
	}

	if dcp.osoRequested {
		dcp.Logger().Infof("%v with OSO mode requested", dcp.Id())
	}

	err = dcp.initializeUprFeed()
	if err != nil {
		return err
	}
	return
}

func (dcp *DcpNozzle) initializeUprHandshakeHelpers() {
	for _, vb := range dcp.ResponsibleVBs() {
		dcp.vbHandshakeMap[vb] = &dcpStreamReqHelper{vbno: vb, dcp: dcp}
		dcp.vbHandshakeMap[vb].initialize()
	}
}

func (dcp *DcpNozzle) Open() error {
	dcp.lock_bOpen.Lock()
	defer dcp.lock_bOpen.Unlock()
	if !dcp.bOpen {
		dcp.bOpen = true

	}
	return nil
}

func (dcp *DcpNozzle) Close() error {
	dcp.lock_bOpen.Lock()
	defer dcp.lock_bOpen.Unlock()
	if dcp.bOpen {
		dcp.bOpen = false
	}
	return nil
}

/**
 * Start routine initializes the DCP client, gen server, and launches go routines on various
 * monitors.
 */
func (dcp *DcpNozzle) Start(settings metadata.ReplicationSettingsMap) error {
	dcp.Logger().Infof("Dcp nozzle %v starting ....", dcp.Id())

	err := dcp.SetState(common.Part_Starting)
	if err != nil {
		return err
	}

	err = dcp.utils.ValidateSettings(dcp_setting_defs, settings, dcp.Logger())
	if err != nil {
		return err
	}

	dcp.Logger().Infof("%v starting ....\n", dcp.Id())
	err = dcp.initialize(settings)
	if err != nil {
		return err
	}
	dcp.Logger().Infof("%v has been initialized\n", dcp.Id())

	// start gen_server
	dcp.start_time = time.Now()

	uprFeed := dcp.getUprFeed()
	if uprFeed != nil {
		uprFeed.StartFeedWithConfig(base.UprFeedDataChanLength)
	}

	//start datachan length stats collection
	// We start collectDcpDataChanLen after StartFeedWithConfig
	// This is because StartFeedWithConfig initializes the even feed channel
	// which is accessed by collectDcpDataChanLen
	dcp.childrenWaitGrp.Add(1)
	go dcp.collectDcpDataChanLen(settings)

	// start data processing routine
	dcp.childrenWaitGrp.Add(1)
	go dcp.processData()

	// start vbstreams
	dcp.childrenWaitGrp.Add(1)
	go dcp.startUprStreams()

	// check for inactive vbstreams
	dcp.childrenWaitGrp.Add(1)
	go dcp.checkInactiveUprStreams()

	err = dcp.SetState(common.Part_Running)

	if err == nil {
		dcp.Logger().Infof("%v has been started", dcp.Id())
	} else {
		dcp.Logger().Errorf("%v failed to start. err=%v", dcp.Id(), err)
	}

	return err
}

func (dcp *DcpNozzle) Stop() error {
	dcp.Logger().Infof("%v is stopping...\n", dcp.Id())
	err := dcp.SetState(common.Part_Stopping)
	if err != nil {
		return err
	}

	//notify children routines
	dcp.closeFinOnce.Do(func() {
		close(dcp.finch)
	})

	dcp.closeUprStreamsWithTimeout()
	dcp.closeUprFeedWithTimeout()

	// need to lock dcp.client. Even with pipeline updater setup, if pipeline start is timed out and being restarted,
	// DCP nozzle could still be creating the client before it realizes that it needs to stop.
	dcp.lock_client.Lock()
	if dcp.client != nil {
		err = dcp.client.Close()
		if err != nil {
			dcp.Logger().Warnf("%v Error closing dcp client. err=%v\n", dcp.Id(), err)
		} else {
			dcp.Logger().Infof("%v closed client successfully.", dcp.Id())
		}
	} else {
		dcp.Logger().Infof("%v skipping closing client since it is nil.", dcp.Id())
	}
	dcp.lock_client.Unlock()

	dcp.Logger().Debugf("%v received %v items, sent %v items\n", dcp.Id(), dcp.counterReceived(), dcp.counterSent())

	dcp.onExit()

	// Wait for all go-routines to exit before cleaning up helpers
	dcp.cleanUpProcessDataHelpers()

	err = dcp.SetState(common.Part_Stopped)
	if err != nil {
		return err
	}
	dcp.Logger().Infof("%v has been stopped\n", dcp.Id())
	return err

}

func (dcp *DcpNozzle) cleanUpProcessDataHelpers() {
	for _, helper := range dcp.vbHandshakeMap {
		helper.disable()
	}
}

func (dcp *DcpNozzle) closeUprStreamsWithTimeout() {
	// use dcp.childrenWaitGrp to ensure that cleanUpProcessDataHelpers() is called
	// after closeUprStreams() completes.
	// otherwise closeUprStreams(), which accesses dcp.vbHandshakeMap, could panic
	dcp.childrenWaitGrp.Add(1)

	err := base.ExecWithTimeout(dcp.closeUprStreams, base.TimeoutDcpCloseUprStreams, dcp.Logger())
	if err != nil {
		dcp.Logger().Warnf("%v error closing upr streams. err=%v", dcp.Id(), err)
	} else {
		dcp.Logger().Infof("%v closed upr streams successfully", dcp.Id())
	}
}

func (dcp *DcpNozzle) closeUprStreams() error {
	defer dcp.childrenWaitGrp.Done()

	vbsList := dcp.ResponsibleVBs()
	dcp.Logger().Infof("%v Closing dcp streams for vb=%v\n", dcp.Id(), vbsList)
	errMap := make(map[uint16]error)
	var uprFeed mcc.UprFeedIface
	for _, vbno := range vbsList {
		stream_state, err := dcp.GetStreamState(vbno)
		if err != nil {
			errMap[vbno] = err
			continue
		}
		if stream_state == Dcp_Stream_Active {
			uprFeed = dcp.getUprFeed()
			if uprFeed != nil {
				err := uprFeed.CloseStream(vbno, dcp.vbHandshakeMap[vbno].getNewVersion())
				if err != nil {
					errMap[vbno] = err
					continue
				}
			} else {
				// uprFeed could be nil if closeUprFeed() has been called prior,
				// which is possible if closeUprStreamsWithTimeout() timed out
				// abort remaining operations
				dcp.Logger().Infof("%v Aborting closeUprStreams since upr feed has been closed\n", dcp.Id())
				break
			}
		} else {
			dcp.Logger().Infof("%v skip closing of stream for vb %v since there is no active stream\n", dcp.Id(), vbno)
		}
	}

	if len(errMap) > 0 {
		msg := fmt.Sprintf("Failed to close upr streams, err=%v\n", errMap)
		dcp.Logger().Errorf("%v %v", dcp.Id(), msg)
		return errors.New(msg)
	}

	return nil
}

func (dcp *DcpNozzle) closeUprFeedWithTimeout() {
	err := base.ExecWithTimeout(dcp.closeUprFeed, base.TimeoutDcpCloseUprFeed, dcp.Logger())
	if err != nil {
		dcp.Logger().Warnf("%v error closing upr feed. err=%v", dcp.Id(), err)
	} else {
		dcp.Logger().Infof("%v closed upr feed successfully", dcp.Id())
	}
}

func (dcp *DcpNozzle) closeUprFeed() error {
	dcp.lock_uprFeed.Lock()
	defer dcp.lock_uprFeed.Unlock()
	if dcp.uprFeed != nil {
		dcp.Logger().Infof("%v Ask uprfeed to close", dcp.Id())
		//in the process of stopping, no need to report any error to replication manager anymore
		dcp.handle_error = false

		dcp.uprFeed.Close()
		dcp.uprFeed = nil
	} else {
		dcp.Logger().Infof("%v uprfeed is already closed. No-op", dcp.Id())
	}
	return nil
}

func (dcp *DcpNozzle) IsOpen() bool {
	dcp.lock_bOpen.RLock()
	defer dcp.lock_bOpen.RUnlock()
	return dcp.bOpen
}

func (dcp *DcpNozzle) Receive(data interface{}) error {
	// DcpNozzle is a source nozzle and does not receive from upstream nodes
	return nil
}

// Handles any UPR event coming in from the UPR feed channel
func (dcp *DcpNozzle) processData() (err error) {
	dcp.Logger().Infof("%v processData starts..........\n", dcp.Id())
	defer dcp.childrenWaitGrp.Done()
	defer dcp.Logger().Infof("%v processData exits\n", dcp.Id())

	uprFeed := dcp.getUprFeed()
	if uprFeed == nil {
		dcp.Logger().Infof("%v DCP feed has been closed. processData exits\n", dcp.Id())
		return
	}

	// GetUprEventCh() wraps the channel supplied that sends in uprEvents
	// mutch is of type UprEvent, located in gomemcached/client/upr_feed.go
	mutch := uprFeed.GetUprEventCh()
	for {
		select {
		case <-dcp.finch:
			goto done
		case m, ok := <-mutch: // mutation from upstream
			if !ok {
				dcp.Logger().Infof("%v DCP mutation channel has been closed.Stop dcp nozzle now.", dcp.Id())
				dcp.handleGeneralError(errors.New("DCP upr feed has been closed."))
				goto done
			}

			// acknowledge the processing of the mutation to uprFeed, which is necessary for uprFeed flow control to work
			err = uprFeed.ClientAck(m)
			if err != nil {
				// should never get here
				err = fmt.Errorf("%v Received error when trying to send ack to uprFeed. Stop dcp nozzle now. err=%v.", dcp.Id(), err)
				dcp.Logger().Errorf(err.Error())
				dcp.handleGeneralError(err)
				goto done
			}

			if m.Opcode == mc.UPR_STREAMREQ {
				// This is a reply coming back from dcp.uprFeed.UprRequestStream(), which triggers UPR_STREAMREQ to the producer
				// See: https://github.com/couchbaselabs/dcp-documentation/blob/master/documentation/commands/stream-request.md
				if m.Status == mc.NOT_MY_VBUCKET {
					vb_err := fmt.Errorf("Received error %v on vb %v\n", base.ErrorNotMyVbucket, m.VBucket)
					dcp.Logger().Errorf("%v %v", dcp.Id(), vb_err)
					dcp.handleVBError(m.VBucket, vb_err)
				} else if m.Status == mc.ROLLBACK {
					rollbackseq := binary.BigEndian.Uint64(m.Value[:8])
					vbno := m.VBucket

					// Process the rollback message to see if this is something we should ignore
					ignoreResponse, helperErr := dcp.vbHandshakeMap[vbno].processRollbackResponse(m.Opaque)
					if helperErr != nil {
						dcp.RaiseEvent(common.NewEvent(common.ErrorEncountered, m, dcp, nil, helperErr))
					}

					if ignoreResponse {
						dcp.Logger().Infof("%v ignored rollback message for vb %v with version %v and rollbackseqno %v since it has already been acknowledged\n", dcp.Id(), vbno, m.Opaque, rollbackseq)
					} else {
						err2 := dcp.performRollback(vbno, rollbackseq)
						if err2 != nil {
							return err2
						}
					}
				} else if m.Status == mc.SUCCESS {
					vbno := m.VBucket
					_, ok := dcp.vb_stream_status[vbno]
					if ok {
						err = dcp.setStreamState(vbno, Dcp_Stream_Active)
						if err != nil {
							return err
						}
						dcp.RaiseEvent(common.NewEvent(common.StreamingStart, m, dcp, nil, nil))
						dcp.vbHandshakeMap[vbno].processSuccessResponse(m.Opaque)
					} else {
						err = fmt.Errorf("%v Stream for vb=%v is not supposed to be opened\n", dcp.Id(), vbno)
						dcp.handleGeneralError(err)
						return err
					}
				}

			} else if m.Opcode == mc.UPR_STREAMEND {
				// Sent to the consumer to indicate that the producer has no more messages to stream for the specified vbucket.
				// https://github.com/couchbaselabs/dcp-documentation/blob/master/documentation/commands/stream-end.md
				vbno := m.VBucket
				seqno := dcp.endSeqnoForDcp[vbno].GetSeqno()
				err = dcp.handleStreamEnd(vbno, seqno, m.Flags)
				if err != nil {
					return err
				}
			} else if m.IsSystemEvent() {
				dcp.handleSystemEvent(m)
				dcp.RaiseEvent(common.NewEvent(common.SystemEventReceived, m, dcp, nil /*derivedItems*/, nil /*otherInfos*/))
			} else if m.IsSeqnoAdv() {
				dcp.handleSeqnoAdv(m)
			} else if m.IsOsoSnapshot() {
				osoBegins, _ := m.GetOsoBegin()
				var helperItems = make([]interface{}, 2)
				helperItems[0] = m.VBucket
				syncCh := make(chan bool)
				helperItems[1] = syncCh
				dcp.RaiseEvent(common.NewEvent(common.OsoSnapshotReceived, osoBegins, dcp, helperItems, nil))
				select {
				case <-syncCh:
					break
				}
			} else {
				// Regular mutations coming in from DCP stream
				if dcp.IsOpen() {
					switch m.Opcode {
					case mc.UPR_MUTATION, mc.UPR_DELETION, mc.UPR_EXPIRATION:
						// https://github.com/couchbaselabs/dcp-documentation/blob/master/documentation/commands/mutation.md
						// https://github.com/couchbaselabs/dcp-documentation/blob/master/documentation/commands/deletion.md
						// https://github.com/couchbaselabs/dcp-documentation/blob/master/documentation/commands/expiration.md
						start_time := time.Now()
						dcp.incCounterReceived()
						if m.IsSnappyDataType() {
							dcp.incCompressedCounterReceived()
						}
						dcp.RaiseEvent(common.NewEvent(common.DataReceived, m, dcp, nil /*derivedItems*/, nil /*otherInfos*/))
						dcp.endSeqnoForDcp[m.VBucket].SetSeqno(m.Seqno)
						wrappedUpr, err := dcp.composeWrappedUprEvent(m)
						if err != nil {
							dcp.handleGeneralError(err)
							dcp.Logger().Errorf("Composing wrappedUpr had error %v", err)
							return err
						}
						// forward mutation downstream through connector
						if err := dcp.Connector().Forward(wrappedUpr); err != nil {
							dcp.handleGeneralError(err)
							dcp.Logger().Errorf("Connector forward had error %v", err)
							goto done
						}
						dcp.incCounterSent()
						// raise event for statistics collection
						dispatch_time := time.Since(start_time)
						dcp.RaiseEvent(common.NewEvent(common.DataProcessed, m, dcp, nil /*derivedItems*/, dispatch_time.Seconds()*1000000 /*otherInfos*/))
						if !dcp.isMainPipeline() && !dcp.osoRequested && m.Seqno > dcp.backfillTaskEndSeqno[m.VBucket].GetSeqno() {
							dcp.sendStreamCloseIfNecessary(m.VBucket)
						}
					case mc.UPR_SNAPSHOT:
						dcp.RaiseEvent(common.NewEvent(common.SnapshotMarkerReceived, m, dcp, nil /*derivedItems*/, nil /*otherInfos*/))
					default:
						dcp.Logger().Debugf("%v Uprevent OpCode=%v, is skipped\n", dcp.Id(), m.Opcode)
					}
				}
			}
		}
	}
done:
	return
}

func (dcp *DcpNozzle) performRollback(vbno uint16, rollbackseq uint64) error {
	//need to request the uprstream for the vbucket again
	updated_ts, err := dcp.vbtimestamp_updater(vbno, rollbackseq)
	if err != nil {
		err = fmt.Errorf("Failed to request dcp stream after receiving roll-back for vb=%v. err=%v\n", vbno, err)
		dcp.Logger().Errorf("%v %v", dcp.Id(), err)
		dcp.handleGeneralError(err)
		return err
	}
	err = dcp.setTS(vbno, updated_ts, true)
	if err != nil {
		err = fmt.Errorf("Failed to update start seqno for vb=%v. err=%v\n", vbno, err)
		dcp.Logger().Errorf("%v %v", dcp.Id(), err)
		dcp.handleGeneralError(err)
		return err
	}
	dcp.startUprStream(vbno, updated_ts)
	return nil
}

func (dcp *DcpNozzle) handleStreamEnd(vbno uint16, seqno uint64, flag uint32) error {
	var err error
	err_streamend := fmt.Errorf("%v stream for vb=%v is closed by producer with flag=0x%x", dcp.Id(), vbno, flag)
	dcp.Logger().Warnf("%v: seqno: %v %v", dcp.Id(), seqno, err_streamend)
	if dcp.vbStreamEndIsOk(vbno) {
		err = dcp.setStreamState(vbno, Dcp_Stream_Closed)
		if dcp.vbStreamEndShouldRaiseEvt(vbno) {
			startTs, getTsErr := dcp.getTS(vbno, true)
			if getTsErr == nil && startTs != nil && seqno == startTs.Seqno {
				// The streamRequest sent a same start and end and so no data was transferred
				go dcp.RaiseEvent(common.NewEvent(common.StreamingBypassed, vbno, dcp, nil, nil))
			} else {
				go dcp.RaiseEvent(common.NewEvent(common.StreamingEnd, vbno, dcp, nil, nil))
			}
		}
	} else {
		// For main pipeline, keep track of vbs whose stream was ended in Dcp_Stream_Active state
		// so that the pipeline can be restarted soon
		streamStatus, err := dcp.GetStreamState(vbno)
		if err != nil || streamStatus != Dcp_Stream_Active {
			return err
		}
		dcp.nonOKVbStreamEndsMtx.Lock()
		dcp.nonOKVbStreamEnds[vbno] = true
		dcp.nonOKVbStreamEndsMtx.Unlock()
	}
	return err
}

func (dcp *DcpNozzle) handleSystemEvent(event *mcc.UprEvent) {
	if !event.IsSystemEvent() {
		dcp.Logger().Warnf("Event %v%v%v is not a system event", base.UdTagBegin, event, base.UdTagEnd)
		return
	}

	manifestId, err := event.GetManifestId()
	if err != nil {
		dcp.Logger().Infof("Event %v%v%v unable to get manifest ID err: %v", base.UdTagBegin, event, base.UdTagEnd, err)
		return
	}

	vbno := event.VBucket
	atomic.StoreUint64(&dcp.vbHighestManifestUidArray[vbno], manifestId)
}

func (dcp *DcpNozzle) handleSeqnoAdv(event *mcc.UprEvent) {
	if !event.IsSeqnoAdv() {
		dcp.Logger().Warnf("Event %v%v%v is not a system event", base.UdTagBegin, event, base.UdTagEnd)
		return
	}

	dcp.RaiseEvent(common.NewEvent(common.SeqnoAdvReceived, event, dcp, nil /*derivedItems*/, nil /*otherInfos*/))
}

// Only ok situation dcp should receive a streamEnd is if it's a backfill
func (dcp *DcpNozzle) vbStreamEndIsOk(vbno uint16) bool {
	if !dcp.CollectionEnabled() {
		return false
	}

	if dcp.specificVBTasks.IsNil() {
		return false
	}

	tasks, exists, unlockFunc := dcp.specificVBTasks.Get(vbno, false)
	defer unlockFunc()
	if !exists {
		return false
	}

	if tasks == nil || tasks.Len() == 0 {
		return false
	}

	// There is at least one VB task
	return true
}

func (dcp *DcpNozzle) composeWrappedUprEvent(m *mcc.UprEvent) (*base.WrappedUprEvent, error) {
	if m.IsSystemEvent() || !m.IsCollectionType() || !dcp.CollectionEnabled() {
		// Collection not used
		wrappedEvent := dcp.wrappedUprPool.Get()
		wrappedEvent.UprEvent = m
		return wrappedEvent, nil
	}

	var mappedScopeName = base.DefaultScopeCollectionName
	var mappedColName = base.DefaultScopeCollectionName
	var collectionDNE bool

	topManifestUid := atomic.LoadUint64(&dcp.vbHighestManifestUidArray[m.VBucket])
	if m.CollectionId > 0 {
		// NOTE - for future once mirroring mode is turned on, this optimization may be turned off
		// First, check the "last source pulled" manifest" and see if the source collection ID is in there.
		// If it is, then use it. DCP is sequential and won't send down a mutation with a collection ID that has
		// been deleted
		// If it is not there, then force a refresh
		var err error
		var forcePull bool
		manifest, err := dcp.specificManifestGetter(math.MaxUint64)
		if err != nil {
			dcp.Logger().Errorf("Unable to get last pulled source manifest. Forcing a refresh")
			forcePull = true
		} else {
			mappedScopeName, mappedColName, err = manifest.GetScopeAndCollectionName(m.CollectionId)
			if err != nil {
				// The last cached version of source manifest does not contain the mutation's collectionID
				// Which means a later source manifest exists and must be used to figure out the scope and col names
				forcePull = true
			}
		}

		if forcePull {
			// VB events comes in sequence order, so the latest manifest must have the info for this collection data
			manifest, err := dcp.specificManifestGetter(topManifestUid)
			if err != nil {
				if err != PartStoppedError {
					// When replication spec gets deleted, the spec may return this err while DCP is trying to finish up
					dcp.Logger().Errorf("Vbno %v Asking for manifest %v got error: %v\n", m.VBucket, topManifestUid, err)
				}
				return nil, err
			}
			if manifest == nil {
				panic(fmt.Sprintf("Nil manifest when asking for %v", topManifestUid))
			}
			if manifest.Uid() < topManifestUid {
				// This case shouldn't be hit because of the way manifest service is implemented, but have it here in case
				dcp.Logger().Errorf("Vbno %v Asking for manifest %v got %v\n", m.VBucket, topManifestUid, manifest.Uid())
				return nil, err
			}
			mappedScopeName, mappedColName, err = manifest.GetScopeAndCollectionName(m.CollectionId)
			if err != nil {
				if manifest.Uid() > topManifestUid {
					// When we request version X and then the manifest service sent us back version Y, where X < Y
					// then it means that the service couldn't go back in time to give us what should have existed.
					// If the collection ID does not exist in Y, but we think it should exist in X, then it means that
					// the collection ID has been deleted between X and Y. This mutation should be skipped.
					// This should be a very rare race condition but theoretically could occur
					collectionDNE = true
					err = nil
				} else {
					err = fmt.Errorf("Document %v%v%v: vb %v topManifestID: %v manifest: %v asking for collectionId: %v returned err %v",
						base.UdTagBegin, string(m.Key), base.UdTagEnd, m.VBucket, topManifestUid, manifest, m.CollectionId, err)
					return nil, err
				}
			}
		}
	}

	colInfo := dcp.collectionNamespacePool.Get()
	colInfo.ScopeName = mappedScopeName
	colInfo.CollectionName = mappedColName
	if dcp.Logger().GetLogLevel() > log.LogLevelDebug {
		dcp.Logger().Debugf("For doc %v topManifest is %v srcColId: %v namespace is %v:%v", string(m.Key), topManifestUid, m.CollectionId, colInfo.ScopeName, colInfo.CollectionName)
	}

	wrappedEvent := dcp.wrappedUprPool.Get()
	wrappedEvent.UprEvent = m
	wrappedEvent.ColNamespace = colInfo
	if collectionDNE {
		wrappedEvent.Flags.SetCollectionDNE()
	}
	return wrappedEvent, nil
}

func (dcp *DcpNozzle) onExit() {
	dcp.childrenWaitGrp.Wait()

}

func (dcp *DcpNozzle) PrintStatusSummary() {
	var msg string
	msg = fmt.Sprintf("%v received %v items (%v compressed), sent %v items.", dcp.Id(), dcp.counterReceived(), dcp.counterCompressedReceived(), dcp.counterSent())
	streams_inactive := dcp.inactiveDcpStreamsWithState()
	if len(streams_inactive) > 0 {
		msg += fmt.Sprintf(" streams inactive: %v", streams_inactive)
	}
	dcp.Logger().Info(msg)
}

func (dcp *DcpNozzle) handleGeneralError(err error) {

	err1 := dcp.SetState(common.Part_Error)
	if err1 == nil {
		dcp.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, dcp, nil, err))
		dcp.Logger().Errorf("%v Raise error condition %v\n", dcp.Id(), err)
	} else {
		dcp.Logger().Debugf("%v in shutdown process. err=%v is ignored\n", dcp.Id(), err)
	}
}

func (dcp *DcpNozzle) handleVBError(vbno uint16, err error) {
	additionalInfo := &base.VBErrorEventAdditional{vbno, err, base.VBErrorType_Source}
	dcp.RaiseEvent(common.NewEvent(common.VBErrorEncountered, nil, dcp, nil, additionalInfo))
}

// start steam request will be sent when starting seqno is negotiated, it may take a few
func (dcp *DcpNozzle) startUprStreams() error {
	defer dcp.childrenWaitGrp.Done()

	var err error = nil
	vbsList := dcp.ResponsibleVBs()
	dcp.Logger().Infof("%v: startUprStreams for %v...\n", dcp.Id(), vbsList)

	init_ch := make(chan bool, 1)
	init_ch <- true

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-dcp.finch:
			goto done
		case <-init_ch:
			// dcp.ResponsibleVBs() returns a copy so it is safe to use
			vbsList = dcp.ResponsibleVBs()
			err = dcp.startUprStreams_internal(vbsList)
			if err != nil {
				return err
			}
		case <-ticker.C:
			streams_non_init := dcp.nonInitDcpStreams()
			if len(streams_non_init) == 0 {
				goto done
			}
			err = dcp.startUprStreams_internal(streams_non_init)
			if err != nil {
				return err
			}
		}
	}
done:
	dcp.Logger().Infof("%v: all dcp stream have been initialized.\n", dcp.Id())

	return nil
}

/**
 * Once the stream is ready to be started (once seqno is populated from ckptmgr)
 * Do the actual stream start.
 * NOTE: Checkpoint manager's SetVBTimestamps() gets called at a pipeline's start, which goes off
 * and sets the sequence number per vbucket.
 * When the timestamps have been set, the pipeline's settings are updated (See ckmgr.setTimestampForVB())
 * Once the pipeline's settings are updated, this DCP nozzle object's UpdateSettings is called,
 * since it's associated to the pipeline, and its internal data structures are updated as a result.
 * The startUprStreams_internal call here depends on those data
 * structured being updated indirectly from checkpoint manager.
 */
func (dcp *DcpNozzle) startUprStreams_internal(streams_to_start []uint16) error {

	// randomizes the sequence of vbs to start, so that each outnozzle gets roughly even initial load
	base.ShuffleVbList(streams_to_start)

	err := dcp.getHighSeqnosIfNecessary(streams_to_start)
	if err != nil {
		dcp.Logger().Errorf("Getting HighSeqno for %v resulted in %v", streams_to_start, err)
		dcp.handleGeneralError(err)
		return err
	}

	for _, vbno := range streams_to_start {
		vbts, err := dcp.getTS(vbno, true)
		if err == nil && vbts != nil {
			if int(vbts.Vbno) == dcp.devInjectionMainRollbackVb && dcp.isMainPipeline() ||
				int(vbts.Vbno) == dcp.devInjectionBackfillRollbackVb && !dcp.isMainPipeline() {
				dcp.Logger().Infof("Dev injection for vb %v received start seqno %v... forcing a rollback to 0", vbno, vbts.Seqno)
				dcp.devInjectionBackfillRollbackVb = -1
				dcp.devInjectionMainRollbackVb = -1
				injectErr := dcp.performRollback(vbts.Vbno, 0)
				if injectErr != nil {
					dcp.Logger().Errorf("Inject error: %v", injectErr)
				}
				continue
			}

			err = dcp.startUprStream(vbno, vbts)
			if err != nil {
				dcp.Logger().Warnf("%v: startUprStreams errored out, err=%v\n", dcp.Id(), err)
				continue
			}
		}
	}
	return nil
}

// Have an internal so we can control the opaque and version being passed in
func (dcp *DcpNozzle) startUprStreamInner(vbno uint16, vbts *base.VBTimestamp, version uint16) (err error) {
	flags := dcpFlagActiveVBOnly
	if !dcp.enablePurgeRollback {
		flags |= dcpFlagIgnoreTombstones
	}

	seqEnd := base.DcpSeqnoEnd
	var filter *mcc.CollectionsFilter
	// filter for main pipeline if resuming from a checkpoint
	if (dcp.specificVBTasks.IsNil() || dcp.specificVBTasks.Len() == 0) && vbts.Seqno > 0 {
		filter = &mcc.CollectionsFilter{UseManifestUid: true, ManifestUid: vbts.ManifestIDs.SourceManifestId}
	}

	if !dcp.specificVBTasks.IsNil() && dcp.specificVBTasks.Len() > 0 {
		vbTasks, exists, unlockSpecificVBTasksMap := dcp.specificVBTasks.Get(vbno, false)
		if !exists || vbTasks == nil || vbTasks.Len() == 0 {
			dcp.Logger().Infof("vbno %v has no task. Exiting")
			unlockSpecificVBTasksMap()
			return nil
		}
		// Execute the first task
		// Use the latest source manifest to look up collection ID for backfill
		manifest, manifestErr := dcp.specificManifestGetter(math.MaxUint64)
		if manifestErr != nil {
			err = fmt.Errorf("When resuming backfill, unable to retrieve latest manifest. Err - %v", manifestErr)
			dcp.handleGeneralError(err)
			unlockSpecificVBTasksMap()
			return
		}

		if manifest.Uid() < vbts.ManifestIDs.SourceManifestId {
			err = fmt.Errorf("%v experienced manifest rollback - Checkpointed with source manifest ID %v but currently is %v",
				dcp.Id(), vbts.ManifestIDs.SourceManifestId, manifest.Uid())
			// TODO - MB-45435 - quorum failover where source manifest rolls back
			dcp.handleGeneralError(err)
			unlockSpecificVBTasksMap()
			return err
		}

		topTask, _, unlockVbTasks := vbTasks.GetRO(0)
		var toTaskErrs error
		seqEnd, filter, toTaskErrs = topTask.ToDcpNozzleTask(manifest)
		unlockVbTasks()
		unlockSpecificVBTasksMap()

		if toTaskErrs != nil {
			// If ToDcpNozzleTask returned error given "latest" manifest, it means that something changed on the source
			// and that the backfill task passed in to DCP and the "latest" manifest isn't up to date
			// Backfill Manager should be monitoring source side manifest changes, and making sure that the backfill
			// replication's VBTasks are cleaned up correctly if a source collection no longer exists
			// So if this has errors, DCP should just bail and eventually the backfill spec's tasks should only contain
			// source collections that actually exist in the steady-state manifest
			err = fmt.Errorf("Error converting VBTask to DCP Nozzle Task %v", toTaskErrs)
			return err
		}

		// Given a task, we should technically only end at a correct seqno. Check the vbSeqnos to ensure that those seqnos are correct
		checkSeqEnd := dcp.vbHighSeqnoMap[vbno].GetSeqno()
		if checkSeqEnd > 0 && checkSeqEnd < seqEnd {
			seqEnd = checkSeqEnd
		}

		// In rare cases where seqEnd is set to lower than seqBegin, make sure the request is valid
		if vbts.Seqno > seqEnd {
			if seqEnd == 0 {
				vbts.Seqno = 0
				seqEnd = 1
			} else {
				vbts.Seqno = seqEnd - 1
			}
		}

		// In a corner case where startSeq == endSeqno, DCP will not send down a streamEnd and instead just
		// close the connection. Mark the endSeqno here first to check if this is the case
		// Update: streamEnd will be sent but there will be no data, so still need to set endSeqnoForDcp for bypass check
		dcp.endSeqnoForDcp[vbno].SetSeqno(seqEnd)
		dcp.backfillTaskEndSeqno[vbno].SetSeqno(seqEnd)
	}

	dcp.Logger().Debugf("%v starting vb stream for vb=%v, version=%v collectionEnabled=%v endSeqno=%v\n", dcp.Id(), vbno, version, dcp.CollectionEnabled(), seqEnd)

	dcp.lock_uprFeed.RLock()
	defer dcp.lock_uprFeed.RUnlock()
	if dcp.uprFeed != nil {
		statusObj, ok := dcp.vb_stream_status[vbno]
		if ok && statusObj != nil {
			var ignore bool
			ignore, err = dcp.vbHandshakeMap[vbno].registerRequest(version, vbts.Seqno)
			if err != nil {
				dcp.handleGeneralError(err)
				return
			}
			if ignore {
				dcp.Logger().Debugf(fmt.Sprintf("%v ignoring send request for seqno %v since it has already been handled", dcp.Id(), vbts.Seqno))
			} else {
				//This function can be called from start, rollback and timeout states.As this call sets the state to INIT,
				//The uprRequestStream failure should revert the state back to pre set state.
				var prevState DcpStreamState
				prevState, err = dcp.GetStreamState(vbno)
				if err != nil {
					return
				}
				err = dcp.setStreamState(vbno, Dcp_Stream_Init)
				if err != nil {
					return
				}

				// version passed in == opaque, which will be passed back to us
				if !dcp.CollectionEnabled() {
					err = dcp.uprFeed.UprRequestStream(vbno, version, flags, vbts.Vbuuid, vbts.Seqno, seqEnd, vbts.SnapshotStart, vbts.SnapshotEnd)
				} else {
					err = dcp.uprFeed.UprRequestCollectionsStream(vbno, version, flags, vbts.Vbuuid, vbts.Seqno, seqEnd, vbts.SnapshotStart, vbts.SnapshotEnd, filter)
				}
				if err != nil {
					err = fmt.Errorf("UprRequestStream failed for vbno=%v with err=%v", vbno, err)
					dcp.handleGeneralError(err)
					err = dcp.setStreamState(vbno, prevState)
				}
			}
			return
		} else {
			err = fmt.Errorf("%v Try to startUprStream for invalid vbno=%v", dcp.Id(), vbno)
			dcp.handleGeneralError(err)
			return
		}
	}
	return
}

// For a given stream (by vb#), send UPR_STREAMREQ via the uprFeed client method
func (dcp *DcpNozzle) startUprStream(vbno uint16, vbts *base.VBTimestamp) error {
	version := dcp.vbHandshakeMap[vbno].getNewVersion()
	return dcp.startUprStreamInner(vbno, vbts, version)
}

func (dcp *DcpNozzle) getUprFeed() mcc.UprFeedIface {
	dcp.lock_uprFeed.RLock()
	defer dcp.lock_uprFeed.RUnlock()
	return dcp.uprFeed
}

type stateCheckFunc func(state DcpStreamState) bool

func (dcp *DcpNozzle) getDcpStreams(stateCheck stateCheckFunc) []uint16 {
	ret := []uint16{}
	vbsList := dcp.ResponsibleVBs()
	for _, vb := range vbsList {
		state, err := dcp.GetStreamState(vb)
		if err == nil && stateCheck(state) {
			ret = append(ret, vb)
		}
	}
	return ret
}

func (dcp *DcpNozzle) inactiveDcpStreams() []uint16 {
	return dcp.getDcpStreams(inactiveStateCheck)
}

func inactiveStateCheck(state DcpStreamState) bool {
	return state != Dcp_Stream_Active && state != Dcp_Stream_Closed
}

func (dcp *DcpNozzle) initedButInactiveDcpStreams() []uint16 {
	return dcp.getDcpStreams(initedButInactiveStateCheck)
}

func initedButInactiveStateCheck(state DcpStreamState) bool {
	return state == Dcp_Stream_Init
}

func (dcp *DcpNozzle) nonInitDcpStreams() []uint16 {
	return dcp.getDcpStreams(nonInitStateCheck)
}

func nonInitStateCheck(state DcpStreamState) bool {
	return state == Dcp_Stream_NonInit
}

func (dcp *DcpNozzle) activeStreams() []uint16 {
	return dcp.getDcpStreams(activeStateCheck)
}

func activeStateCheck(state DcpStreamState) bool {
	return state == Dcp_Stream_Active
}

func (dcp *DcpNozzle) inactiveDcpStreamsWithState() map[uint16]DcpStreamState {
	ret := make(map[uint16]DcpStreamState)
	vbsList := dcp.ResponsibleVBs()
	for _, vb := range vbsList {
		state, err := dcp.GetStreamState(vb)
		if err == nil && state != Dcp_Stream_Active && state != Dcp_Stream_Closed {
			ret[vb] = state
		}
	}
	return ret
}

// generate a new 16 bit opaque value set as MSB.
func (dcp *DcpNozzle) newOpaqueForClosing() uint16 {
	timeNow := uint64(time.Now().UnixNano())
	// bit 26 ... 42 from UnixNano().
	return uint16((timeNow >> 26) & 0xFFFF)
}

func (dcp *DcpNozzle) UpdateSettings(settings metadata.ReplicationSettingsMap) error {
	ts_obj := dcp.utils.GetSettingFromSettings(settings, DCP_VBTimestamp)
	if ts_obj != nil {
		new_ts, ok := settings[DCP_VBTimestamp].(map[uint16]*base.VBTimestamp)
		if !ok || new_ts == nil {
			panic(fmt.Sprintf("setting %v should have type of map[uint16]*base.VBTimestamp", DCP_VBTimestamp))
		}

		err := dcp.onUpdateStartingSeqno(new_ts)
		if err != nil {
			return err
		}
	}
	if statsInterval, ok := settings[DCP_Nozzle_Stats_Interval]; ok {
		dcp.setStatsInterval(uint32(statsInterval.(int)))
		dcp.stats_interval_change_ch <- true
	}

	if dcpPriority, ok := settings[DCP_Priority]; ok {
		dcp.setDcpPriority(dcpPriority.(mcc.PriorityType))
	}

	return nil
}

func (dcp *DcpNozzle) setDcpPriority(priority mcc.PriorityType) error {
	if !dcp.setDcpPrioritySetting(priority) {
		// no real changes
		return nil
	}

	feed := dcp.getUprFeed()
	if feed == nil {
		dcp.Logger().Infof("%v skipping set dcp priority operation because feed is nil", dcp.Id())
		return nil
	}
	err := feed.SetPriorityAsync(priority)
	if err != nil {
		dcp.Logger().Warnf("%v Error from SetPiority  = %v\n", dcp.Id(), err)
	}
	return err
}

func (dcp *DcpNozzle) getDcpPrioritySetting() mcc.PriorityType {
	dcp.lockSetting.RLock()
	defer dcp.lockSetting.RUnlock()
	return dcp.dcpPrioritySetting
}

// returns true if dcp priority has indeed been set to a different value
func (dcp *DcpNozzle) setDcpPrioritySetting(priority mcc.PriorityType) bool {
	dcp.lockSetting.Lock()
	defer dcp.lockSetting.Unlock()
	if dcp.dcpPrioritySetting == priority {
		dcp.Logger().Infof("%v skipping setting dcp priority to %v since there is no real change\n", dcp.Id(), priority)
		return false
	}

	dcp.Logger().Infof("%v changing dcp priority from %v to %v\n", dcp.Id(), dcp.dcpPrioritySetting, priority)
	dcp.dcpPrioritySetting = priority
	return true
}

func (dcp *DcpNozzle) onUpdateStartingSeqno(new_startingSeqnos map[uint16]*base.VBTimestamp) error {
	for vbno, vbts := range new_startingSeqnos {
		ts_withlock, ok := dcp.cur_ts[vbno]
		if ok && ts_withlock != nil {
			ts_withlock.lock.Lock()
			defer ts_withlock.lock.Unlock()
			if !dcp.isTSSet(vbno, false) {
				//only update the cur_ts if starting seqno has not been set yet
				dcp.Logger().Debugf("%v: Starting dcp stream for vb=%v, len(closed streams)=%v\n", dcp.Id(), vbno, len(dcp.inactiveDcpStreams()))
				dcp.setTS(vbno, vbts, false)
			}
		}
	}
	return nil
}

func (dcp *DcpNozzle) setTS(vbno uint16, ts *base.VBTimestamp, need_lock bool) error {
	ts_entry := dcp.cur_ts[vbno]
	if ts_entry != nil {
		if need_lock {
			ts_entry.lock.Lock()
			defer ts_entry.lock.Unlock()
		}
		ts_entry.ts = ts

		// If the set op is successsful (which it is here), then ensure that the corresponding sourcemanifestID
		// is also restored. This ensures that when dcp sends things down, DCP can lookup the right manifest
		// as the older source manifests are not to be sent
		atomic.StoreUint64(&dcp.vbHighestManifestUidArray[vbno], ts.ManifestIDs.SourceManifestId)

		return nil
	} else {
		err := fmt.Errorf("setTS failed: vbno=%v is not tracked in cur_ts map", vbno)
		dcp.handleGeneralError(err)
		return err
	}
}

func (dcp *DcpNozzle) getTS(vbno uint16, need_lock bool) (*base.VBTimestamp, error) {
	ts_entry := dcp.cur_ts[vbno]
	if ts_entry != nil {
		if need_lock {
			ts_entry.lock.RLock()
			defer ts_entry.lock.RUnlock()
		}
		return ts_entry.ts, nil
	} else {
		err := fmt.Errorf("getTS failed: vbno=%v is not tracked in cur_ts map", vbno)
		dcp.handleGeneralError(err)
		return nil, err
	}
}

// if the vbno is not belongs to this DcpNozzle, return true
func (dcp *DcpNozzle) isTSSet(vbno uint16, need_lock bool) bool {
	ts, err := dcp.getTS(vbno, need_lock)
	if err != nil {
		err := fmt.Errorf("isTSSet failed: vbno=%v is not tracked in cur_ts map", vbno)
		dcp.handleGeneralError(err)
		return true
	}
	return ts != nil
}

func (dcp *DcpNozzle) setStreamState(vbno uint16, streamState DcpStreamState) error {
	statusObj, ok := dcp.vb_stream_status[vbno]
	if ok && statusObj != nil {
		statusObj.lock.Lock()
		defer statusObj.lock.Unlock()
		statusObj.state = streamState
		return nil
	} else {
		err := fmt.Errorf("%v Trying to set stream state to invalid vbno=%v", dcp.Id(), vbno)
		dcp.handleGeneralError(err)
		return err
	}
}

func (dcp *DcpNozzle) GetStreamState(vbno uint16) (DcpStreamState, error) {
	statusObj, ok := dcp.vb_stream_status[vbno]
	if ok && statusObj != nil {
		statusObj.lock.RLock()
		defer statusObj.lock.RUnlock()
		return statusObj.state, nil
	} else {
		err := fmt.Errorf("Try to get stream state to invalid vbno=%v", vbno)
		dcp.handleGeneralError(err)
		return 0, err
	}
}

func (dcp *DcpNozzle) checkInactiveUprStreams() {
	defer dcp.childrenWaitGrp.Done()

	dcp_inactive_stream_check_ticker := time.NewTicker(dcp_inactive_stream_check_interval)
	nonOKVbStreamEndsTicker := time.NewTicker(nonOkVbStreamEndCheckInterval)
	defer dcp_inactive_stream_check_ticker.Stop()
	defer nonOKVbStreamEndsTicker.Stop()

	for {
		select {
		case <-dcp.finch:
			dcp.Logger().Infof("%v checkInactiveUprStreams routine is exiting because dcp nozzle has been stopped\n", dcp.Id())
			return
		case <-dcp_inactive_stream_check_ticker.C:
			if dcp.isFeedClosed() {
				dcp.Logger().Infof("%v checkInactiveUprStreams routine is exiting because upr feed has been closed\n", dcp.Id())
				dcp.handleGeneralError(errors.New("DCP upr feed has been closed."))
				return
			}
			err := base.ExecWithTimeout(dcp.checkInactiveUprStreams_once, checkInactiveStreamsTimeout, dcp.Logger())
			if err != nil {
				// ignore error and continue
				dcp.Logger().Infof("Received error when checking inactive steams for %v. err=%v\n", dcp.Id(), err)
			}
		case <-nonOKVbStreamEndsTicker.C:
			// check if pipeline needs to be restarted if there are VBs with their streams ended in a non-OK manner
			dcp.nonOKVbStreamEndsMtx.Lock()
			if len(dcp.nonOKVbStreamEnds) > 0 {
				errMsg := fmt.Sprintf("%v vbs received non-OK streamEnds", len(dcp.nonOKVbStreamEnds))
				dcp.Logger().Warnf("%v", errMsg)
				dcp.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, dcp, nil, errors.New(errMsg)))
			}
			dcp.nonOKVbStreamEndsMtx.Unlock()
		}
	}
}

// check if feed has been closed
func (dcp *DcpNozzle) isFeedClosed() bool {
	dcp.lock_uprFeed.RLock()
	defer dcp.lock_uprFeed.RUnlock()
	if dcp.uprFeed != nil {
		return dcp.uprFeed.Closed()
	}
	return true
}

// check if inactive streams need to be restarted
/**
 * Called by the monitor (checkInactiveUprStreams) to re-send the current lowest sequence number
 * to UPR. It is sent under 3 conditions:
 * 1. Currently undergoing rollback, and have not started successfully yet.
 * 2. Started successfully, but streamreq has not yet arrived to DCP nozzle (race condition)
 * 	  I/O will flow but the state will remain inactive indefinitely while UPR rejects any further streamreq.
 * 3. DCP has not been able to send SUCCESS back yet. (Rare in a local node environment as comm between
 *    DCP and nozzle is done via TCP locally)
 */
func (dcp *DcpNozzle) checkInactiveUprStreams_once() error {
	streams_inactive := dcp.initedButInactiveDcpStreams()

	if len(streams_inactive) > 0 {
		updated_streams_inactive_count := atomic.AddUint32(&dcp.counter_streams_inactive, 1)
		dcp.Logger().Infof("%v incrementing counter for inactive streams to %v\n", dcp.Id(), updated_streams_inactive_count)
		if updated_streams_inactive_count > uint32(base.MaxCountStreamsInactive) {
			// After a certain amount of time, simply re-send a STREAMREQ to re-initiate.
			dcp.Logger().Infof("%v re-sending STREAMREQ for inactive streams %v\n", dcp.Id(), streams_inactive)
			err := dcp.startUprStreams_internal(streams_inactive)
			if err != nil {
				return err
			}
			atomic.StoreUint32(&dcp.counter_streams_inactive, 0)
		}
	}

	return nil
}

func (dcp *DcpNozzle) counterReceived() uint64 {
	return atomic.LoadUint64(&dcp.counter_received)
}

func (dcp *DcpNozzle) counterCompressedReceived() uint64 {
	return atomic.LoadUint64(&dcp.counter_compressed_received)
}

func (dcp *DcpNozzle) incCounterReceived() {
	atomic.AddUint64(&dcp.counter_received, 1)
}

func (dcp *DcpNozzle) incCompressedCounterReceived() {
	atomic.AddUint64(&dcp.counter_compressed_received, 1)
}

func (dcp *DcpNozzle) counterSent() uint64 {
	return atomic.LoadUint64(&dcp.counter_sent)
}

func (dcp *DcpNozzle) incCounterSent() {
	atomic.AddUint64(&dcp.counter_sent, 1)
}

func (dcp *DcpNozzle) getStatsInterval() uint32 {
	return atomic.LoadUint32(&dcp.stats_interval)
}

func (dcp *DcpNozzle) setStatsInterval(stats_interval uint32) {
	atomic.StoreUint32(&dcp.stats_interval, stats_interval)
}

func (dcp *DcpNozzle) collectDcpDataChanLen(settings metadata.ReplicationSettingsMap) {
	defer dcp.childrenWaitGrp.Done()
	ticker := time.NewTicker(time.Duration(dcp.getStatsInterval()) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-dcp.finch:
			return
		case <-dcp.stats_interval_change_ch:
			ticker.Stop()
			ticker = time.NewTicker(time.Duration(dcp.getStatsInterval()) * time.Millisecond)
		case <-ticker.C:
			dcp.getDcpDataChanLen()
		}
	}

}

func (dcp *DcpNozzle) getDcpDataChanLen() {
	dcp_dispatch_len := 0
	dcp.lock_uprFeed.RLock()
	defer dcp.lock_uprFeed.RUnlock()
	if dcp.uprFeed != nil {
		dcp_dispatch_len = len(dcp.uprFeed.GetUprEventCh())
	}
	// Raise event to keep track of how full DCP is and whether or not DCP is going to be a bottleneck
	dcp.RaiseEvent(common.NewEvent(common.StatsUpdate, nil, dcp, nil, dcp_dispatch_len))

}

// This should be used for externally
func (dcp *DcpNozzle) ResponsibleVBs() []uint16 {
	state := dcp.State()
	if state == common.Part_Initial || state == common.Part_Starting {
		dcp.vbnosLock.RLock()
		retlist := base.DeepCopyUint16Array(dcp.vbnos)
		dcp.vbnosLock.RUnlock()
		return retlist
	} else {
		return dcp.vbnos
	}
}

func (dcp *DcpNozzle) setResponsibleVBs(vbnos []uint16) {
	dcp.vbnosLock.Lock()
	dcp.vbnos = vbnos
	dcp.vbnosLock.Unlock()
}

func (dcp *DcpNozzle) checkDefaultCollectionExistence() error {
	manifest, err := dcp.specificManifestGetter(math.MaxUint64)
	if err != nil {
		dcp.Logger().Errorf("Unable to check default collection existence: %v", err)
		return err
	}

	_, _, err = manifest.GetScopeAndCollectionName(base.DefaultCollectionId)
	if err == base.ErrorNotFound {
		dcp.Logger().Errorf("DCP %v cannot find default collection for spec", dcp.Id())
		err = service_def.ErrorSourceDefaultCollectionDNE
		dcp.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, dcp, nil, err))
	}
	return err
}

func (dcp *DcpNozzle) RecycleDataObj(req interface{}) {
	switch req.(type) {
	case *base.WrappedUprEvent:
		if dcp.wrappedUprPool != nil {
			wrappedReq := req.(*base.WrappedUprEvent)
			wrappedReq.UprEvent = nil
			wrappedReq.ColNamespace = nil
			dcp.wrappedUprPool.Put(req.(*base.WrappedUprEvent))
		}
	case *base.CollectionNamespace:
		if dcp.collectionNamespacePool != nil {
			ns := req.(*base.CollectionNamespace)
			ns.ScopeName = ""
			ns.CollectionName = ""
			dcp.collectionNamespacePool.Put(ns)
		}
	default:
		panic("Coding error")
	}
}

func (dcp *DcpNozzle) GetOSOSeqnoRaiser() func(vbno uint16, seqno uint64) {
	return func(vbno uint16, seqno uint64) {
		m := &mcc.UprEvent{
			VBucket:      vbno,
			SnapstartSeq: seqno,
			SnapendSeq:   seqno,
		}
		dcp.RaiseEvent(common.NewEvent(common.SnapshotMarkerReceived, m, dcp, nil /*derivedItems*/, nil /*otherInfos*/))
	}
}

func (dcp *DcpNozzle) isMainPipeline() bool {
	return dcp.specificVBTasks.IsNil() || dcp.specificVBTaskLen == 0
}

func (dcp *DcpNozzle) getHighSeqnosIfNecessary(vbnos []uint16) error {
	if dcp.isMainPipeline() || len(vbnos) == 0 {
		// Main pipeline, no need to get high Seqno
		// Backfill pipeline with this DCP nozzle not assigned any task, no need to get highseqnos
		return nil
	}

	if dcp.isCollectionsMigration {
		// Collections migration only has default collection so no need to high seqnos
		// Moreover, the savedMCReq features would not have collections enabled
		return nil
	}

	topTasks := dcp.specificVBTasks.GetTopTasksOnlyClone()
	for _, vbno := range vbnos {
		_, exists, unlockFunc := topTasks.Get(vbno, false)
		unlockFunc()
		if !exists {
			return fmt.Errorf("Requesting vb %v but specific VBTasks does not contain such VB", vbno)
		}
	}

	sourceCollectionNamespaces := topTasks.GetDeduplicatedSourceNamespaces()
	if len(sourceCollectionNamespaces) == 0 {
		return fmt.Errorf("Toptasks has no source collection namespaces")
	}

	latestManifest, err := dcp.specificManifestGetter(math.MaxUint64)
	if err != nil {
		return fmt.Errorf("Unable to get latest manifest %v", err)
	}

	var collectionIds []uint32
	for _, sourceNs := range sourceCollectionNamespaces {
		colId, err := latestManifest.GetCollectionId(sourceNs.ScopeName, sourceNs.CollectionName)
		if err != nil {
			dcp.Logger().Warnf("getHighSeqno could not find collection ID for %v", sourceNs.String())
		} else {
			collectionIds = append(collectionIds, colId)
		}
	}

	if len(collectionIds) == 0 {
		return fmt.Errorf("Unable to find any collection ID for getHighSeqno")
	}

	select {
	case <-dcp.getHighSeqnoOneAtATime:
		defer func() { dcp.getHighSeqnoOneAtATime <- true }()

		addr, err := dcp.xdcr_topology_svc.MyMemcachedAddr()
		if err != nil {
			return err
		}

		client, _, err := dcp.utils.GetMemcachedConnectionWFeatures(addr, dcp.sourceBucketName, dcp.user_agent, base.KeepAlivePeriod, dcp.savedMcReqFeatures, dcp.Logger())
		if err != nil {
			return err
		}
		defer client.Close()

		mccContext := &mcc.ClientContext{}
		var vbSeqnoMap map[uint16]uint64

		for _, vb := range dcp.ResponsibleVBs() {
			dcp.vbHighSeqnoMap[vb].SetSeqno(0)
		}

		for _, collectionId := range collectionIds {
			mccContext.CollId = collectionId
			vbSeqnoMap, err = client.GetAllVbSeqnos(vbSeqnoMap, mccContext)
			if err != nil {
				return fmt.Errorf("Unable to GetAllVbSeqnos %v\n", err)
			}

			vblist := dcp.ResponsibleVBs()
			for _, vb := range vblist {
				highSeqno, exists := vbSeqnoMap[vb]
				if !exists {
					dcp.Logger().Warnf("DCP VBHighSeqno for colID %v did not return vb %v", collectionId, vb)
				} else {
					if dcp.vbHighSeqnoMap[vb].GetSeqno() < highSeqno {
						dcp.vbHighSeqnoMap[vb].SetSeqno(highSeqno)
					}
				}
			}
		}
	}
	return nil
}

func (dcp *DcpNozzle) initializeDevInjections(settings metadata.ReplicationSettingsMap) {
	if val, ok := settings[DCP_DEV_MAIN_ROLLBACK_VB]; ok {
		intVal, ok2 := val.(int)
		if ok2 && intVal >= 0 {
			dcp.devInjectionMainRollbackVb = intVal
		}
	}

	if val, ok := settings[DCP_DEV_BACKFILL_ROLLBACK_VB]; ok {
		intVal, ok2 := val.(int)
		if ok2 && intVal >= 0 {
			dcp.devInjectionBackfillRollbackVb = intVal
		}
	}
}

func (dcp *DcpNozzle) sendStreamCloseIfNecessary(vbno uint16) {
	streamState, _ := dcp.GetStreamState(vbno)
	uprFeed := dcp.getUprFeed()
	if uprFeed != nil && streamState == Dcp_Stream_Active &&
		atomic.CompareAndSwapUint32(dcp.backfillTaskStreamCloseSent[vbno], 0, 1) {
		// This could be raceful. It is possible that DCP has already ended the stream
		// and an end event has been passed to the uprFeed's event channel but hasn't gotten to
		// this DCP yet... and this portion of the code is trying to close it thinking that
		// streamEnd has not been sent, even though producer already has no active vb stream anymore
		// In this case, producer will send StreamNotRequested error, which is ignorable
		err := dcp.uprFeed.CloseStream(vbno, dcp.vbHandshakeMap[vbno].getNewVersion())
		if err != nil && !strings.Contains(err.Error(), mcc.StreamNotRequested) {
			msg := fmt.Sprintf("Failed to close upr stream for vb %v, err=%v\n", vbno, err)
			dcp.Logger().Errorf("%v %v", dcp.Id(), msg)
			dcp.handleGeneralError(err)
		}
	}
}

func (dcp *DcpNozzle) vbStreamEndShouldRaiseEvt(vbno uint16) bool {
	if dcp.isMainPipeline() || dcp.osoRequested {
		return true
	}

	// Backfill pipeline with OSO disabled, means that DCP could initiate streamClose
	// if DCP continues to send data past the requested seqnoEnd
	// UprFeed may send one or more UPR_STREAMEND messages down and only the first
	// one should be handled (because UprFeed modifies UPR_CLOSESTREAM opcode to be UPR_STREAMEND opcode)
	if atomic.CompareAndSwapUint32(dcp.backfillTaskStreamEndReceived[vbno], 0, 1) {
		return true
	}
	return false
}
