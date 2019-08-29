# XDCR Developer Tools

Some tools to allow developers quickly get up and running on local cluster_run environments
# Pre-Requisites
The tools require Bash 4 or above. Version 5 is recommended.

# 1-Step Provisioning
The provisioning script of `provision_clusterRun.sh` provide the basic framework for provisioning a 2 1-node cluster. 
It expects:
   - Users to run cluster_run with 2 *clean* nodes setup.

It will:
  - Setup the 2 nodes into 2 1-node clusters.
  - Create bucket(s) on each of the cluster.
  - Create bi-directional replications between them.
  - Run data generator load on the buckets.

# Provisioning Customization
The magic happens all in the `clusterRunProvisioning.shlib`. Users are free to create their own provisioning scripts. See `examples` directory for an example of a customized script.
## Customization Variables
The following globals are used to provision the system:

| Variable Name              | Purpose                                                      | Type              | Key          | Value                     |
|----------------------------|--------------------------------------------------------------|-------------------|--------------|---------------------------|
| DEFAULT_ADMIN              | The username with which to set up the cluster                | String            | N/A          | Specified string          |
| DEFAULT_PW                 | The password with which to set up the cluster                | String            | N/A          | Specified String          |
| CLUSTER_NAME_PORT_MAP      | Maps a cluster name to a port of cluster_run                 | Associative Array | Cluster Name | Port Number               |
| CLUSTER_NAME_XDCR_PORT_MAP | Maps a cluster name to the internal XDCR Port of cluster_run | Associative Array | Cluster Name | XDCR Port number          |
| CLUSTER_NAME_BUCKET_MAP    | Maps a cluster to an array of bucket names that it will have | Associative Array | Cluster Name | Array of Bucket names     |
| BUCKET_NAME_SCOPE_MAP      | Maps a bucket to a list of scope names that it will have.    | Associative Array | Bucket Name  | Array of Scope names      |
| SCOPE_NAME_COLLECTION_MAP  | Maps a scope to one or more collections that it should have  | Associative Array | Scope Name   | Array of Collection names |

### Setting Bucket Properties
Setting bucket properties is a little special since it requires a hack around bash to work with a hash of hashes. There is no one visible global to use, but special functions are used, as follows:
```
declare -A BucketProperty=(["ramQuotaMB"]=100)
declare -A Bucket1Properties=(["ramQuotaMB"]=100 ["CompressionMode"]="Active")
insertPropertyIntoBucketNamePropertyMap "B0" BucketProperty
insertPropertyIntoBucketNamePropertyMap "B1" Bucket1Properties
```
The above snippet shows how to set bucket properties, and the provisioning script will set up the buckets with the appropriate properties.

# Exporting Topologies of Live XDCR-provisioned
The provisioning script provides users a way to import the provisioned set up into the running bash shell's environment variables, to allow further helper scripts to work.
This requires users to execute the export command at the end of the provisioning script, like so:

```
neil.huang@NeilsMacbookPro:~/source/couchbase/goproj/src/github.com/couchbase/goxdcr/tools$ ./provision_clusterRun.sh
...
If needed, run the following export command to enable other helper scripts to load the provisioned configuration:
============================================
export XDCR_Provisioning_VarFile=/var/folders/g3/_9ldw1x543n9fjrql5l3d6cnwd96dq/T/tmp.qPiaw4vz
============================================
```

Once the above export statement is executed manually, then further helper scripts are then enabled.

# Live-XDCR Helper Scripts
The following is a list of helper scripts that are bundled in this tool directory.

## internalSettings.sh
Allows the editing of XDCR internal settings of a provisioned cluster.
```
./internalSettings.sh [-h] -l | -g <ClusterName> | -s <ClusterName> -v "key=val" [-v... ]
	h: This help page
	l: List all available <clusterName> for setting and displaying
	g: Gets all internal settings for cluster <clusterName>
	s: Sets internal setting on cluster <clusterName> with one or more values of "key=val"
```

## defaultReplicationSettings.sh
Edits the default replication settings of a provisioned cluster.
```
./defaultReplicationSettings.sh [-h] -l | -g <ClusterName> | -s <ClusterName> -v "key=val" [-v... ]
	h: This help page
	l: List all available <clusterName> for setting and displaying
	g: Gets all default replication settings for cluster <clusterName>
	s: Sets default replication setting on cluster <clusterName> with one or more values of "key=val"

Example: ./defaultReplicationSettings.sh -s C1 -v "compressionType=None"
And all future replications will have default of compressionType of None for cluster "C1"
```

## replicationSettings.sh
Edits a specific live replication setting. 
```
./replicationSettings.sh [-h] -l | -g <ID> | -s <ID> -v "key=val" [-v...]
	h: This help page
	l: List all ID and configurations
	g: Gets all replication settings for specified <ID>
	s: Sets replication settings for specified <ID> with one or more values of "key=val"

Example: ./replicationSettings.sh -s 1 -v "filterExpression=" -v "filterSkipRestream=true"
```

## collectionsManagement.sh
Provides a quick and easy way to view/create/delete scopes and collections
```
./collectionsManagement.sh [-h] -l | -g <ClusterName> -b <bucketName> | -n/-d <ClusterName> -b <bucketName> -s <ScopeName> [-c <CollectionName>]
	h: This help page
	l: List all available <clusterName> for setting and displaying
	b: Bucket name to operate on
	g: Gets all collections for cluster <clusterName>
	n: Creates a *new* collection or scope on a cluster
	d: Deletes a collection or scope on a cluster
	s: Specifies a scope
	c: (optional) Specifies a collection under a scope
```
