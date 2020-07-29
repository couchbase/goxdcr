package service_impl

import (
	"time"

	utilities "github.com/couchbase/goxdcr/utils"

	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/functions"
	"github.com/couchbase/goxdcr/log"
)

const EVENTING_FUNCTION_LIB = "xdcr"

var (
	// The number of goroutines that can call js-evaluator to get merge result
	numResolverWorkers = base.JSEngineThreadsPerWorker * base.JSEngineWorkersPerNode
	// The channel size for sending input to resolverWorkers. Keep this channel small so it won't have backup data from pipelines that went away
	inputChanelSize = numResolverWorkers
)

type ResolverSvc struct {
	logger  *log.CommonLogger
	InputCh chan *base.ConflictParams // This accepts conflicting documents from XMEM
}

func NewResolverSvc(utils utilities.UtilsIface) *ResolverSvc {
	return &ResolverSvc{logger: log.NewLogger("ResolverSvc", nil)}
}

func (rs *ResolverSvc) ResolveAsync(aConflict *base.ConflictParams, finish_ch chan bool) {
	select {
	case rs.InputCh <- aConflict:
	case <-finish_ch:
	}
}

func (rs *ResolverSvc) Start() {
	rs.InputCh = make(chan *base.ConflictParams, inputChanelSize)
	for i := 0; i < numResolverWorkers; i++ {
		go rs.resolverWorker(i)
	}
	rs.logger.Infof("ResolverSvc for custom CR is started.")
}

func (rs *ResolverSvc) resolverWorker(threadId int) {
	for {
		rs.resolveOne(threadId)
	}
}

func (rs *ResolverSvc) resolveOne(threadId int) {
	input := <-rs.InputCh
	source := input.Source
	target := input.Target
	var params []interface{}
	var mask uint64 = (1 << 16) - 1
	sourceTime := time.Unix(0, int64(source.Req.Cas & ^mask)).String()
	targetTime := time.Unix(0, int64(target.Cas & ^mask)).String()
	sourceBody := base.FindSourceBodyWithoutXattr(source.Req)
	targetBody := base.FindTargetBodyWithoutXattr(target)

	params = append(params, string(source.Req.Key))
	params = append(params, string(sourceBody))
	params = append(params, sourceTime)
	params = append(params, string(input.SourceId))
	params = append(params, string(targetBody))
	params = append(params, targetTime)
	params = append(params, string(input.TargetId))
	res, err := functions.Execute(EVENTING_FUNCTION_LIB, input.BucketName, params)
	input.ResultNotifier.NotifyMergeResult(input, res, err)
}
