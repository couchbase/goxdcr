package metadata_svc

import ()

type ClusterInfoSvc interface {
	GetClusterConnectionStr(ClusterUUID string) (string, error)
	GetMyActiveVBuckets(ClusterUUID string, Bucket string, NodeId string) ([]uint16, error)
	GetServerList(ClusterUUID string, Bucket string) ([]string, error)
	GetServerVBucketsMap(ClusterUUID string, Bucket string) (map[string][]uint16, error)
	IsNodeCompatible(node string, version string) (bool, error)
}
