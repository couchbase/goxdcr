package peerToPeer

import (
	"fmt"
	"sync"
	"time"

	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
)

type ManifestsHandler struct {
	*HandlerCommon

	finCh          chan bool
	colManifestSvc service_def.CollectionsManifestSvc

	storageMtx sync.RWMutex
	storage    map[string]*metadata.CollectionsManifestPair
}

func NewManifestsHandler(reqCh []chan interface{}, logger *log.CommonLogger, lifecycleId string,
	cleanupInterval time.Duration, replSpecSvc service_def.ReplicationSpecSvc,
	colManifestSvc service_def.CollectionsManifestSvc) *ManifestsHandler {
	finCh := make(chan bool)
	handler := &ManifestsHandler{
		HandlerCommon:  NewHandlerCommon("ManifestsHandler", logger, lifecycleId, finCh, cleanupInterval, reqCh, replSpecSvc),
		finCh:          finCh,
		colManifestSvc: colManifestSvc,
		storage:        map[string]*metadata.CollectionsManifestPair{},
	}
	return handler
}

func manifestsHandlerKeyGetter(specId, internalId string) string {
	return fmt.Sprintf("%v_%v", specId, internalId)
}

func (m *ManifestsHandler) Start() error {
	m.HandlerCommon.Start()
	go m.handler()
	m.colManifestSvc.SetPeerManifestsGetter(m.getCachedManifest)
	return nil
}

func (m *ManifestsHandler) Stop() error {
	close(m.finCh)
	return nil
}

func (m *ManifestsHandler) handler() {
	for {
		select {
		case <-m.finCh:
			return
		case req := <-m.receiveReqCh:
			manifestsReq, isReq := req.(*ManifestsRequest)
			if isReq {
				m.handleRequest(manifestsReq)
			}
		case resp := <-m.receiveRespCh:
			manifestsResp, isResp := resp.(*ManifestsResponse)
			if isResp {
				m.handleResponse(manifestsResp)
			}
		}
	}

}

func (m *ManifestsHandler) deleteManifests(specId, specInternalId string) {
	key := manifestsHandlerKeyGetter(specId, specInternalId)

	m.storageMtx.Lock()
	defer m.storageMtx.Unlock()
	delete(m.storage, key)

	m.logger.Debugf("ManifestsHandler deleted cached manifests for spec %v internalID %v", specId, specInternalId)
}

func (m *ManifestsHandler) handleRequest(req *ManifestsRequest) {
	if req == nil || req.SpecId == "" || req.SpecInternalId == "" {
		return
	}

	helper := &metadata.ManifestsDoc{}
	manifestsPair, err := helper.DeCompressAndOutputPair(req.CompressedManifests)
	if err != nil {
		m.logger.Errorf("Unable to decompress and output from source %v specID %v specInternal %v err %v",
			req.Sender, req.SpecId, req.SpecInternalId, err)
		return
	}
	specId := req.SpecId
	specInternalId := req.SpecInternalId

	m.storeManifestsPair(specId, specInternalId, manifestsPair)
}

// Temporarily store...
func (m *ManifestsHandler) storeManifestsPair(specId string, specInternalId string, manifestsPair *metadata.CollectionsManifestPair) {
	key := manifestsHandlerKeyGetter(specId, specInternalId)
	m.storageMtx.Lock()
	defer m.storageMtx.Unlock()
	m.storage[key] = manifestsPair
	time.AfterFunc(base.P2PManifestsCacheCleanupInterval, func() {
		m.deleteManifests(specId, specInternalId)
	})
}

func (m *ManifestsHandler) handleResponse(resp *ManifestsResponse) {
	// No need to handleResponse at this point as the requests is not meant to be replied
}

func (m *ManifestsHandler) getCachedManifest(specId, specInternalId string) (*metadata.CollectionsManifestPair, error) {
	key := manifestsHandlerKeyGetter(specId, specInternalId)
	m.storageMtx.RLock()
	defer m.storageMtx.RUnlock()
	if _, found := m.storage[key]; !found {
		return nil, service_def.ErrorNotFound
	}
	return m.storage[key], nil
}
