package metadata_svc

import (
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/service_def"
)

// AgentLifeCycle defines the life cycle methods of a remote agent
type RemoteAgentLifeCycle interface {
	// Start the remote agent with the given reference
	// userInitiated is true if the start is triggered by user action i.e at the time of reference creation,
	// false if it is triggered by system (e.g. from metaKV reads either during startup or during metKV callbacks)
	Start(newRef *metadata.RemoteClusterReference, userInitiated bool) error
	// Gracefully stop the remote agent
	Stop()
	// InitDone indicates whether the remote agent has been fully initialized
	InitDone() bool
	// UpdateReferenceFrom updates the remote agent with the new reference synchronously
	UpdateReferenceFrom(newRef *metadata.RemoteClusterReference, updateMetaKv bool) error
	// UpdateReferenceFromAsync updates the remote agent with the new reference asynchronously
	UpdateReferenceFromAsync(newRef *metadata.RemoteClusterReference, updateMetaKv bool) error
	// DeleteReference clears the reference from the remote agent
	// If delFromMetaKv is true, the reference is also deleted from metaKV
	DeleteReference(delFromMetaKv bool) (*metadata.RemoteClusterReference, error)
}

// AgentRefresh defines the refresh related methods of a remote agent
// A refresh cycle is responsible for monitoring the remote cluster for changes
// and updating the local metadata accordingly
type RemoteAgentRefresh interface {
	// Start a refresh cycle
	Refresh() error
	// ReenableRefresh re-enables refresh if it was previously aborted
	ReenableRefresh()
	// AbortAnyOngoingRefresh aborts any ongoing refresh operation
	// returns true if re-enable is needed after abort
	AbortAnyOngoingRefresh() (needToReEnable bool)
	// ClearBootstrap clears the bootstrap state
	ClearBootstrap()
	// IsBootstrap indicates whether the agent is in bootstrap mode
	IsBootstrap() bool
	// SetBootstrap sets the agent to bootstrap mode
	SetBootstrap()
}

// AgentClusterStatus defines methods for retrieving the status of the remote cluster.
type RemoteAgentClusterStatus interface {
	// ConfigurationHasChanged indicates if there's any change in the remote cluster configuration.
	ConfigurationHasChanged() bool
	// GetConnectivityStatus returns the current connectivity status of the remote cluster.
	GetConnectivityStatus() metadata.ConnectivityStatus
	// GetUnreportedAuthError indicates if there's an unreported authentication error.
	GetUnreportedAuthError() bool
	// ResetConfigChangeState resets the remote configuration change state.
	ResetConfigChangeState()
}

// AgentMetadata defines methods for accessing the remote agent's fields.
type RemoteAgentMetadata interface {
	// Id returns the unique ID of the remote cluster reference.
	Id() string
	// Name returns the name of the remote cluster reference.
	Name() string
	// Uuid returns the UUID of the remote cluster.
	Uuid() string
	// GetReferenceClone returns a clone of the remote cluster reference with an optional refresh.
	GetReferenceClone(refresh bool) (*metadata.RemoteClusterReference, error)
	// GetReferenceAndStatusClone returns a clone of the remote cluster reference with its current connectivity status set.
	GetReferenceAndStatusClone() *metadata.RemoteClusterReference
	// GetCapability returns the cluster compatability of the remote.
	GetCapability() (metadata.Capability, error)
	// GetConnectionStringForCAPIRemoteCluster returns the remote's connection string .
	GetConnectionStringForCAPIRemoteCluster() (string, error)
	// IsSame checks if the given reference is the same as the agent's current reference.
	IsSame(ref *metadata.RemoteClusterReference) bool
	// UsesAlternateAddress indicates whether the address preference set is external.
	UsesAlternateAddress() (bool, error)
}

// ClusterAgentDataProvider defines methods that fetch data from the remote cluster.
type RemoteClusterAgentDataProvider interface {
	// GetMaxCasGetter returns a getter func to fetch maxCas for a given bucket
	GetMaxCasGetter(bucketName string) (service_def.MaxVBCasStatsGetter, error)
	// GetBucketInfoGetter returns a getter func to fetch bucket info for a given bucket from the remote cluster
	GetBucketInfoGetter(bucketName string) (service_def.BucketInfoGetter, error)
	// OneTimeGetRemoteBucketManifest fetches the latest manifest for a given bucket on demand.
	OneTimeGetRemoteBucketManifest(bucketName string) (*metadata.CollectionsManifest, error)
	// GetManifest returns the latest manifest fetched from the remote cluster.
	GetManifest(bucketName string, forceRefresh bool, restAPIQuery bool) (*metadata.CollectionsManifest, error)
}

// CngAgentDataProvider defines methods that fetch data from the CNG.
type RemoteCngAgentDataProvider interface {
	// GetManifest returns the latest manifest fetched from the remote cluster.
	GetManifest(bucketName string, restAPIQuery bool) (*metadata.CollectionsManifest, error)
	// OneTimeGetRemoteBucketManifest fetches the latest manifest for a given bucket on demand.
	OneTimeGetRemoteBucketManifest(bucketName string) (*metadata.CollectionsManifest, error)
}

// AgentConfig defines the setter methods for configuring the remote agent.
type RemoteAgentConfig interface {
	// SetCapability sets the cluster compatibility of the remote cluster.
	SetCapability(capability metadata.Capability)
	// SetMetadataChangeHandlerCallback sets the callback for metadata change events.
	SetMetadataChangeHandlerCallback(callBack base.MetadataChangeHandlerCallback)
	// SetHeartbeatApi sets the heartbeat API for the remote agent.
	SetHeartbeatApi(api service_def.ClusterHeartbeatAPI)
	// SetReplReader sets the replication spec reader for the remote agent.
	SetReplReader(reader service_def.ReplicationSpecReader)
	// RegisterBucketRequest registers the bucket specific getters on the remote agent.
	RegisterBucketRequest(bucketName string) error
	// UnRegisterBucketRefresh unregisters the bucket specific getters on the remote agent.
	UnRegisterBucketRefresh(bucketName string) error
}

// RemoteAgentIface is a composite interface that includes all the methods needed for a remote agent
type RemoteAgentIface interface {
	RemoteAgentLifeCycle
	RemoteAgentRefresh
	RemoteAgentClusterStatus
	RemoteAgentMetadata
	RemoteAgentConfig
}
