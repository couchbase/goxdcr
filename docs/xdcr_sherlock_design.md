---
title: '<span id="h.bwulhev4m497" class="anchor"></span>XDCR Sherlock Design'
...

  Date         Revision History
  ------------ ------------------------------------------------------------------
  07/30/2014   Initial draft by [*Xiaomei Zhang*](mailto:xiaomei@couchbase.com)
               

> [*1 Project Summary*](#project-summary)
>
> [*1.1 Objectives*](#objectives)
>
> [*1.2 High Level Requirements*](#high-level-requirements)
>
> [*1.3 Scope*](#scope)
>
> [*1.3.1 In scope*](#in-scope)
>
> [*1.3.2 Stretch goal*](#stretch-goal)
>
> [*1.3.3 Out-of-scope*](#out-of-scope)
>
> [*2 Technical Design*](#technical-design)
>
> [*2.1 Metadata*](#metadata)
>
> [*2.2 Component Diagram*](#component-diagram)
>
> [*2.3 Terminology*](#terminology)
>
> [*2.4 Pipeline layout*](#pipeline-layout)
>
> [*2.5 Pipeline Runtime Environment*](#pipeline-runtime-environment)
>
> [*2.5.1 Data Item Tracker*](#data-item-tracker)
>
> [*2.5.2 Checkpoint Manager*](#checkpoint-manager)
>
> [*2.5.3 Pipeline Initializer*](#pipeline-initializer)
>
> [*2.5.4 Statistics Manager*](#statistics-manager)
>
> [*2.5.5 Pipeline supervisor*](#pipeline-supervisor)
>
> [*2.6 Supporting Components*](#supporting-components)
>
> [*2.6.1 Pipeline Factory*](#pipeline-factory)
>
> [*2.6.2 Replication Manager*](#replication-manager)
>
> [*2.6.3 XDCR Rest Server*](#xdcr-rest-server)
>
> [*2.6.4 Erlang XDCR Rest Interface*](#erlang-xdcr-rest-interface)
>
> [*2.7 Use cases*](#use-cases)
>
> [*2.7.1 Create a replication*](#create-a-replication)
>
> [*2.7.2 Target topology change due to
> failover*](#target-topology-change-due-to-failover)
>
> [*2.7.3 Replication specification
> update*](#replication-specification-update)
>
> [*2.7.4 Statistics requested via rest
> api*](#statistics-requested-via-rest-api)
>
> [*2.7.5 Pipeline performance hot spot
> detection*](#pipeline-performance-hot-spot-detection)
>
> [*2.8 Consistency*](#consistency)
>
> [*2.8.1 Assumptions of XDCR’s replication
> strategy*](#assumptions-of-xdcrs-replication-strategy)
>
> [*2.8.2 How inconsistency is
> introduced?*](#how-inconsistency-is-introduced)
>
> [*2.8.3 Conflict Detection and
> Resolution*](#conflict-detection-and-resolution)
>
> [*2.8.3.1 Requirements*](#requirements)
>
> [*2.8.3.2 Proposals*](#proposals)
>
> [*2.8.3.2.1 LWW with client-set timestamp using
> NTP*](#lww-with-client-set-timestamp-using-ntp)
>
> [*2.8.3.2.2 Last-Write-Win With Server-set Timestamp Using
> NTP*](#last-write-win-with-server-set-timestamp-using-ntp)
>
> [*2.8.3.2.3 Last Write Win With HLC (Hybrid Logic
> Clock)*](#last-write-win-with-hlc-hybrid-logic-clock)
>
> [*2.8.3.2.4 Version tree based conflict detection + custom defined
> conflict
> resolution*](#version-tree-based-conflict-detection-custom-defined-conflict-resolution)
>
> [*2.8.3.3 Comparison*](#comparison)
>
> [*2.9 Filtering*](#filtering)
>
> [*2.9.1 Requirements*](#requirements-1)
>
> [*2.9.2 Competitive Analysis*](#competitive-analysis)
>
> [*2.9.3 How specify a filter*](#how-specify-a-filter)
>
> [*2.9.3.1 Option1: As a regular
> expression*](#option1-as-a-regular-expression)
>
> [*2.9.3.2 Option2: As a custom js function per
> replication*](#option2-as-a-custom-js-function-per-replication)
>
> [*2.9.3.3 Option3: Define replication data set for
> replication*](#option3-define-replication-data-set-for-replication)
>
> [*2.9.4 Where in the tech stack filter should be
> supported?*](#where-in-the-tech-stack-filter-should-be-supported)
>
> [*2.9.5 Compare Options*](#compare-options)
>
> [*2.10 Backward Compatibility*](#backward-compatibility)
>
> [*2.10.1 Rolling upgrade*](#rolling-upgrade)
>
> [*2.10.2 Metadata migration*](#metadata-migration)
>
> [*3 Programming Language Choice*](#programming-language-choice)
>
> [*4 External Dependency*](#external-dependency)
>
> [*5 Open Design Issues*](#open-design-issues)
>
> [*6 Appendix A - Past Issues
> Summary*](#appendix-a---past-issues-summary)
>
> [*7 Appendix B - NTP Setup*](#appendix-b---ntp-setup)
>
> [*8 Appendix C - Data Loss Analysis For Conflict Resolution
> Options*](#section-6)

1 Project Summary
=================

1.1 Objectives
--------------

-   Provide stability on the existing features. Address some of the
    > [*issues*](#appendix-a---past-issues-summary) you see in the past
    > with XDCR.

-   Implement\\enhance a few key features PM identified

    -   Conflict resolution

    -   Filter

-   Experiment and evolve the framework and common components which can
    > enable us to quickly build other similar data exchange features in
    > the future

1.2 High Level Requirements
---------------------------

-   Stay backward compatible

-   Maintain high throughput and low latency, improve resource usage
    > efficiency

<!-- -->

-   Improve eventual consistency and reduce lost updates under failures

1.3 Scope
---------

### 1.3.1 In scope

  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  Issue \#1   Description                                                                                                                                                                                  Related Enhancement \#
  ----------- -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- -------------------------------------------------------------
  1           Enhance checkpointing                                                                                                                                                                        [MB-9982](http://www.couchbase.com/issues/browse/MB-9982)
                                                                                                                                                                                                           
              -   make checkpoint metadata visible for all nodes in the cluster                                                                                                                            [MB-10179](http://www.couchbase.com/issues/browse/MB-10179)
                                                                                                                                                                                                           
              -   detect target\\source topology change                                                                                                                                                    
                                                                                                                                                                                                           
                                                                                                                                                                                                           

  2           Support LWW semantics for conflict resolution                                                                                                                                                [MB-10011](http://www.couchbase.com/issues/browse/MB-10011)
                                                                                                                                                                                                           
                                                                                                                                                                                                           [MB-10010](http://www.couchbase.com/issues/browse/MB-10010)

  3           Filtered Replication: selectively replicate a subset of items in bucket                                                                                                                      [MB-9395](http://www.couchbase.com/issues/browse/MB-9395)

  4           Better resource usage                                                                                                                                                                        [MB-10093](http://www.couchbase.com/issues/browse/MB-10093)
                                                                                                                                                                                                           
              -   Multiplexing messages from multiple Vbuckets on the same source node when streaming out data; multiplexing messages for multiple Vbuckets on the same target node when inserting data.   
                                                                                                                                                                                                           
              -   Configurable multiplexing factor.                                                                                                                                                        
                                                                                                                                                                                                           
                                                                                                                                                                                                           
  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

### 1.3.2 Stretch goal

  Issue \#1   Description                                         Related Enhancement \#
  ----------- --------------------------------------------------- -----------------------------------------------------------
  4           Runtime Adaptiveness and self-adjustment based on   [MB-7802](http://www.couchbase.com/issues/browse/MB-7802)

### 1.3.3 Out-of-scope

  ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  Issue \#1                                             Description                                                                                                                                       Related Enhancement \#
  ----------------------------------------------------- ------------------------------------------------------------------------------------------------------------------------------------------------- ----------------------------------------------------------------------------------------------------------------
  <span id="h.ey4yghc7ipo6" class="anchor"></span>1     <span id="h.njsxl96dbots" class="anchor"></span>Data inconsistency introduced when failover happens at source cluster                             <span id="h.njsxl96dbots-1" class="anchor"></span>[MB-10516](http://www.couchbase.com/issues/browse/MB-10516)

  <span id="h.ey4yghc7ipo6-1" class="anchor"></span>2   <span id="h.mrfnb5vwmm5k" class="anchor"></span>XDCR: enable app driven conflict resolution[](https://www.couchbase.com/issues/browse/MB-10517)   <span id="h.njsxl96dbots-1" class="anchor"></span>[MB-10517](https://www.couchbase.com/issues/browse/MB-10517)
                                                                                                                                                                                                          
                                                        <span id="h.njsxl96dbots" class="anchor"></span>                                                                                                  

  <span id="h.ey4yghc7ipo6" class="anchor"></span>3     <span id="h.23cm48pif27g" class="anchor"></span>XDCR: Provide Observe with XDCR                                                                   <span id="h.njsxl96dbots" class="anchor"></span>[MB-7614](https://www.couchbase.com/issues/browse/MB-7614)
  ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

2 Technical Design
==================

2.1 Metadata
------------

-   Replication Specification

  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  Attribute Name                       Type     Description                                                                                                                          New
  ------------------------------------ -------- ------------------------------------------------------------------------------------------------------------------------------------ -----
  source                               String   &lt;Source ClusterUUID&gt;/&lt;Source Bucket Name&gt;                                                                                N

  target                               String   &lt;Target ClusterUUID&gt;/&lt;Target Bucket Name&gt;                                                                                N

  filter\_expression                   String   the filter expression                                                                                                                Y

  checkpoint\_interval                 Number   the interval between two checkpoint                                                                                                  N
                                                                                                                                                                                     
                                                default: 1800 s                                                                                                                      
                                                                                                                                                                                     
                                                range: 60-14400s                                                                                                                     

  batch\_count                         Number   the number of mutations in a batch                                                                                                   N
                                                                                                                                                                                     
                                                default: 500                                                                                                                         
                                                                                                                                                                                     
                                                range: 500-10000                                                                                                                     

  batch\_size                          Number   the size (kb) of a batch                                                                                                             N
                                                                                                                                                                                     
                                                default: 2048                                                                                                                        
                                                                                                                                                                                     
                                                range: 10-10000                                                                                                                      

  failure\_restart\_interval           Number   the number of seconds to wait after failure before restarting                                                                        N
                                                                                                                                                                                     
                                                default: 30                                                                                                                          
                                                                                                                                                                                     
                                                range: 1-300                                                                                                                         

  optimistic\_replication\_threshold   Number   if the document size (in kb) &lt;optimistic\_replication\_threshold, replicate optimistically; otherwise replicate pessimistically   N
                                                                                                                                                                                     
                                                default: 256                                                                                                                         
                                                                                                                                                                                     
                                                range: 0-20\*1024\*1024                                                                                                              

  http\_connection                     Number   number of maximum simultaneous HTTP connections used for REST protocol                                                               N
                                                                                                                                                                                     
                                                default: 20                                                                                                                          
                                                                                                                                                                                     
                                                range: 1-100                                                                                                                         

  source\_nozzle\_per\_node            Number   the number of nozzles can be used for this replication per source cluster node                                                       Y
                                                                                                                                                                                     
                                                This together with target\_nozzle\_per\_node controls the parallism of the replication                                               

  target\_nozzle\_per\_node            Number   the number of nozzles can be used for this replication per target cluster node                                                       Y
                                                                                                                                                                                     
                                                This together with source\_nozzle\_per\_node controls the parallism of the replication                                               

  max\_expected \_replication\_lag     Number   the max replication lag (in ms) that user can tolerant for this replication                                                          Y
                                                                                                                                                                                     
                                                *Note: if the actual replication lag is larger than this value, it is consider as timeout*                                           

  max\_timeout\_percentage\_allowed    Number   the max allowed timeout percentage. Exceed that limit, pipeline would be considered as not healthy                                   Y
  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

\*The deprecated parameters from 3.0 are: **workerProcess**,
**max\_concurrent\_reps**

-   Checkpoint history

Checkpoint history consists of a list of checkpoints. A checkpoint
reflects the replication progress at the time of taking. Conceptually it
is &lt;source\_sequence\_number, target\_sequence\_number&gt;. The
frequency of checkpointing is controlled by “checkpoint\_interval” in
replication specification metadata. Checkpoint history is per
replication per VBucket.

Checkpoint is used for jump-starting the replication after failure or
manual pause, stop, as it remembers the progress of the replication.

The following information are recorded for a checkpoint

  Attribute Name              Type     Description
  --------------------------- -------- -------------------------------------------------------------------------------------------------------------------------------------------------
  target\_sequence\_number    Number   The sequence number of the target VBucket at the time of checkpoint
  source\_sequence\_number    Number   The largest sequence number of data that has been streamed out of the source VBucket and propagated to target VBucket at the time of checkpoint
  dcp\_failover\_uuid         Number   DCP’s failover uuid.
  dcp\_snapshot\_seqno        Number   The start sequence number of DCP’s most recent snapshot on source VBucket
  dcp\_snapshot\_end\_seqno   Number   The end sequence number of DCP’s most recent snapshot on source VBucket

-   Topology

We need Vbucket server map for both source and target cluster as the
input to construct a replication pipeline

2.2 Component Diagram
---------------------

2.3 Terminology
---------------

-   Pipeline

Pipeline abstracts a runnable flow from source node to target nodes. It
consists of a layout and a runtime environment for it to run.

-   Pipeline Layout

> Pipeline layout is a DAG of parts, with at least one incoming nozzle
> and one outgoing nozzle.

-   Pipeline Runtime Environment

> Pipeline runtime environment is made of the components that
> supervises\\facilitates the pipeline’s running

-   Part

> Part abstracts a single data processing step, i.e. filter, transform,
> join etc. Each part can have any number of downstream parts, and any
> number of upstream parts. It receives data from its upstream parts and
> send the processing result to its downstream parts. It can raise a set
> of events, which is abstracted as PartEvent.

-   PartEvent

> There are four types of PartEvent:
>
> *DataReceived* - It signals the data item is received, but has not
> been processed
>
> *DataProcessed* - It signals the data item is processed, but has not
> been sent to downstream
>
> *DataSent* - It signals a data item is sent downstream.
>
> *ErrorEncountered* - It signals the part encounters an error.

-   Nozzle

> Nozzle is a special type of part. It abstracts the parts where the
> data flowing in\\out of the pipeline. It usually doesn’t do any
> processing on the data. It can be opened or closed to allow\\disallow
> the stream in\\out the pipeline.

-   Router

Router is a type of Part. Router has N downstream parts, for any data
item, router dispatches it to M downstream parts with 0&lt;M&lt;=N

2.4 Pipeline layout 
--------------------

XDCR’s replication pipeline consists of the following parts (aka.
processing steps):

-   UPR Nozzle - stream out data from source Couchbase node using DCP

-   Filter - filter the data according to the filter expression in the
    > replication specification

*Note: may not be needed for next release if filtering by key can be
supported at UPR layer. In the future, it may be needed if filter
capability extend to support filtering by value.*

-   Router - route the data to the right path leading to the target
    > Vbucket

-   Queue - buffering between source and target

-   Outgoing Nozzle - send data to the target Couchbase node

    -   XMEM Nozzle - using XMEM to propagate the data to target Vbucket

    -   CAPI Nozzle - using CAPI to propagate the data to
        > target Vbucket. This is needed for backward compatibility.

The following diagram shows a replication request of replicating data in
Bucket A from source cluster (2 nodes) to Bucket AA in target cluster (3
nodes) are implemented by two pipelines parallelly running to replicate
data from each of the source node to target nodes. Each pipeline running
on the source cluster’s node.

2.5 Pipeline Runtime Environment 
---------------------------------

### 2.5.1 Data Item Tracker

Data item tracker is responsible to track the data item enters the
pipeline, and raise “DataExistsPipeline” or “DataTimeOut” event when the
pipeline finishes processing the data item or pipeline failed to finish
processing it in time.

Here is its logic we have in mind.

1.  One tracker routine per incoming nozzle. It tracks the data items
    which are flown in through that nozzle

2.  It subscribes to “DataReceived” event of its incoming nozzle. When a
    data item arrives, it starts the timer for that data item, note down
    the seq\_no of that data item and its arrival time in a hash table

3.  Its time keeper periodically check the hash table to see if any data
    item it tracks has timed out. Raise “DataTimedOut” event for the
    ones do.

4.  Tracker also subscribes to “DataSent” event of all the outgoing
    nozzles, if the event is about an data item it tracks in the hash
    table, it raises “DataExitsPipeline” event for it and remove it
    from hashtable.

### 2.5.2 Checkpoint Manager

Here is the logic how Checkpoint Manager does checkpointing.

1.  When a data item exists the data pipeline, “DataExistsPipeline”
    event is raised

2.  Checkpoint manager subscribes to that event. On that event, get the
    information of VBucketId, source\_sequence\_number,
    upr\_failover\_uuid, upr\_snapshot\_seqno, upr\_snapshot\_end\_seqno
    from the data item and fill them in checkpoint structure for
    the Vbucket.

3.  Check if it is time to do checkpointing, if it is not, loop back to
    1, if it is, start checkpointing (4 -6) for each VBucket.

4.  Ask pipeline to close the corresponding outgoing nozzle for the
    VBucket

5.  Probe the target VBucket’s position by calling rest API -
    \_commit\_for\_checkpoint, which returns the target VBucket’s high
    sequence number. Fill that value in the current checkpoint as
    target\_sequence\_number

6.  Ask pipeline to open the outgoing nozzle for the VBucket

7.  Call Distributed Metadata Store to persist the checkpoint for each
    VBucket in their document.

### 2.5.3 Pipeline Initializer

Pipeline Initializer implementation for XDCR does two things: 1.
configure the pipeline with the setting parameters in Replication
Specification; 2. Determine the starting point for UPR nozzles.

\#1 is a simple task, we will not describe the details in this document.
Instead we will expand a little bit on \#2, although it is nothing new
here. Its logic is almost the same as the current XDCR implementation
except for two things: 1. the current XDCR does not track checkpoint
history, it only tracks the current checkpoint; 2. the current XDCR’s
checkpoint file is not distributed to all nodes in the cluster. Here is
the logic:

1.  For each VBucket, get the checkpoint metadata from the distributed
    metadata store

2.  set current\_checkpoint to be the most recent checkpoint from the
    metadata file

3.  send \_pre\_replication message to target node with
    target\_sequence\_number to see it is accepted. if it is accepted,
    continue; otherwise get the previous checkpoint in the metadata,
    repeat 3

4.  configure UPRNozzle for that VBucket with source\_sequence\_number,
    failover\_uuid, snapshot\_seqno, snapshot\_end\_seqno in the
    checkpoint

5.  if the starting point is accepted by UPR, done with this task,
    return; otherwise get the previous checkpoint in the metadata,
    repeat 3-4

### 2.5.4 Statistics Manager

-   Statistics

  Statistic                  Description                                                                                                   New
  -------------------------- ------------------------------------------------------------------------------------------------------------- -----
  docs\_written              number of documents written to destination cluster via XDCR                                                   
  data\_replicated           size of data replicated in bytes                                                                              
  changes\_left              number of updates still pending replication                                                                   
  docs\_checked              number of documents checkpointed for changes                                                                  
  num\_checkpoints           number of checkpoints issued in replication queue                                                             
  num\_failedckpts           number of checkpoints failed during replication                                                               
  size\_rep\_queue           size of replication queue in bytes                                                                            
  time\_committing           seconds elapsed during replication                                                                            
  bandwidth\_usage           bandwidth used during replication                                                                             
  docs\_latency\_aggr        aggregate time waiting to send changes to destination cluster in milliseconds                                 
  docs\_latency\_wt          weighted average latency for sending replicated changes to destination cluster                                
  docs\_rep\_queue           Number of documents in replication queue                                                                      
  meta\_latency\_aggr        aggregate time to request and receive metadata about documents                                                
  meta\_latency\_wt          weighted average time for requesting document metadata                                                        
  rate\_replication          bytes replicated per second                                                                                   
  docs\_opt\_repd            number of docs sent optimistically                                                                            
  timeout\_percentage\_map   The map of vbucket id and the percentage of timed-out data for this VBucket during this statistics interval   Y

\*The deprecated statistics from 3.0 are: **active\_vbreps,
waiting\_vbreps, time\_working**.

-   Collector

For each statistic in the above table, there will be a collector which
could subscribe to one or more part event to collection the information
it needs to calculate the statistic.

Collectors are stateful and they would keep running as long as the
pipeline is running.

-   Statistics Manager

It manages a list of statistics that need to be collected for the
pipeline and a list of collector. It does the following jobs:

1.  It would start the collectors based on the list of statistics to
    > be collected. And it would stop the corresponding collector if the
    > list of statistics are updated by admin and that statistic is no
    > longer on the list.

2.  It would be the interface for any statistics request for
    > the pipeline.

-   Interface with ns-server

> stats\_collector.erl would get the statistics from the replication
> manager via REST API.

### 2.5.5 Pipeline supervisor

-   Healthcheck the pipeline

Pipeline supervisor sends heartbeat messages periodically to all the
parts in the pipeline to see if any part is broken in the pipeline. If a
broken part is found, it would try to restart the part to fix the
pipeline. It will also periodically check if source\\target side
topology changes. On detecting the topology change, it will declare the
pipeline broken and report it to replication manager.

-   Error handling and propagation

Pipeline supervisor subscribes to ErrorEncoutered event of all parts.
Upon error, it would try to restart the part without lost of the data it
is currently processing. If that is not successful, it will declare the
pipeline is broken.

2.6 Supporting Components
-------------------------

### 2.6.1 Pipeline Factory

Pipeline factory is the component that takes the following metadatas -
replication specification, topology as input to construct a pipeline.

### 2.6.2 Replication Manager

Replication Manager coordinates the involved components at runtime. It
is the heart of the solution.

### 2.6.3 XDCR Rest Server

XDCR Rest Server provides rest interface for customer or other component
to interact with XDCR via REST. It should provide the same capability as
3.0 and 2.5.1. Please refer to [*XDCR REST
API*](http://docs.couchbase.com/couchbase-manual-2.5/cb-rest-api/#xdcr-rest-api)
in Couchbase document.

### 2.6.4 Erlang XDCR Rest Interface 

This provides the following REST APIs.

1.  Expose xdc\_rdoc\_replication\_srv.find\_all\_replication\_docs for
    > Replication Manager to read the pre-existing XDCR settings and
    > checkpoint document so that ReplicationManager can migrate them to
    > Distributed Metadata Store.

2.  Expose remote\_clusters\_info.get\_remote\_bucket\_by\_ref for
    > Replication Manager to get the remote cluster’s information
    > including hostname, username and password.

This lives inside NS-Server and would be implemented in Erlang.

2.7 Use cases
-------------

We will run through a few scenarios to see the interactions between the
components

### 2.7.1 Create a replication

### 2.7.2 Target topology change due to failover

#### 

### 2.7.3 Replication specification update

### 2.7.4 Statistics requested via rest api

### 2.7.5 Pipeline performance hot spot detection

1\. Current pipeline layout is the following

2\. Pipeline Supervisor periodically request statistics from the
pipeline. Pipeline Supervisor on Node 1 see the following statistics
about the Vbuckets on its node.

-   timeout\_percentage\_map

  VBucket Id   timeout percentage
  ------------ --------------------
  1            98%
  2            99%
  3            95%
  4            96%

-   docs\_rep\_queue

  Queue Id   Queue length
  ---------- --------------
  q1         200
  q2         200
  q3         13

3\. Pipeline Supervisor notices VBucket 1, 2, 3, 4’s timeout\_percentage
&gt; timeout\_percentage\_cap,it concludes the pipeline is not healthy.

4\. Pipeline Supervisor starts to diagnose problem.

A.  q1, q2 are full --&gt;XMEM Nozzle 1, XMEM Nozzle 2 are slow

B.  XMEM Nozzle 1 and XMEM Nozzle 2 are streaming out data for VBucket
    > 1, 2, 3

C.  VBucket 1, 4’s UPR nozzle is UPR nozzle 1; VBucket 2, 3’s UPR nozzle
    > is UPR nozzle 2

D.  Data from VBucket 1, 4 shares the same rounter. Because q1 is full,
    > data from VBucket 4 is blocked as well.

5\. Pipeline Supervisor reports pipeline is not healthy and provides
feedback to Replication Manager - replication for VBucket 1 ,2, 3 is
slow.

6\. Replication Manager ask the Pipeline to checkpoint itself, then stop

7\. Replication Manager ask Pipeline Factory to construct a new pipeline
with hint “replication for VBucket 1, 2, 3 is slow”.

8\. Pipeline Factory based on the rule “Fast VBucket and Slow VBucket
should not share the same data passage”, reconstructs the pipeline as
below.

9\. Replication Manager ask the new pipeline to start.

2.8 Consistency
---------------

Keeping source data set and target data set consistent is the goal of
replication. We will discuss the challenges and our proposed solutions
in details below.

### 2.8.1 Assumptions of XDCR’s replication strategy

When replication is started from scratch, XDCR does: 1. replicate all
the documents in the source data set to target data set; 2. after that,
continuous replicate any updates caused by user create\\delete\\update
documents to target data set.

Step 2 assumes that all the changes to the source data set are
introduced by user’s CRUD operation on document.

### 2.8.2 How inconsistency is introduced?

1.  Inconsistency is introduced when there is a node failure in source
    cluster

For example, In uni-directional setup, source cluster has failover and
the data on the new master happens to be not as up-to-date as the remote
replica in remote cluster.

In this case, XDCR would be in the loop of doing step 2. But the
assumption of step 2 is no longer hold any more. As the failover or
update of filter definition introduces the changes to the source data
set, and the change list is not captured by DCP.

2.  Inconsistency is introduced when concurrent write on both source and
    target cluster and conflict is not correctly detected and resolved.

IMO, the above two kind of inconsistencies call for different solutions.
The solution to the inconsistency introduced by failure on source
cluster remains as an open issue, and it is out of the [*scope*](#scope)
of this design. We will be focusing on the inconsistency introduced by
concurrent write for the remaining of this chapter.

### 2.8.3 Conflict Detection and Resolution

#### 2.8.3.1 Requirements

Please refer to Cihan’s [*XDCR Conflict Resolution
Evaluation*](https://docs.google.com/spreadsheets/d/1ju1nOA2ksJ7zZjffUiB_M6bYFC9pQv_jRWqQ2Mprmdc/edit#gid=0)

#### 2.8.3.2 Proposals

Various proposals have been discussed and compared. They are documented
below.

##### 2.8.3.2.1 LWW with client-set timestamp using NTP

This approach’s basic idea is to let client take control of the revision
sequence number, which we use for conflict resolution. Client can set
NTP synchronized local timestamp as the value of revision sequence
number to implement last-write-win semantics. There are two slightly
different implementation flavors.

-   Option \#1

1.  Expand document metadata to add a new flag (auto\_revNum)to indicate
    > if auto-generated revision sequence number is enabled. By default
    > it is enabled.

2.  Allow application to supply their own revision sequence number via
    > Client SDK API.

3.  If the document is flagged with auto\_revNum=false, EP engine would
    > not auto generates revision number.

4.  The conflict resolution algorithm would be:

    a.  If both version has the same value on auto-revNum (both true or
        > both false), we compare revision sequence number, CAS,
        > expiration time and metadata flag in order, the version has
        > bigger value win. If both version has the same value on
        > revision sequence number, CAS, expiration time and metadata
        > flag, the replication would be dropped.

    b.  If one version with auto\_revNum = true, the other version with
        > auto\_revNum = false, the one with auto\_revNum=false
        > (revision sequence number is supplied by application) would
        > win

-   Option \#2

1.  Expand document metadata to add cust\_rev\_no as a new optional
    > metadata field.

2.  Allow application to set cust\_rev\_no via Client SDK API.

3.  EP engine would still auto generates and sets revision sequence
    > number

4.  The conflict resolution algorithm would be:

    a.  If both version has not-null cust\_rev\_no, the version with
        > bigger value win

    b.  If one version’s cust\_rev\_no is not null, the other version’s
        > is null, the version with not null cust\_rev\_no win

    c.  If both version has the same cust\_rev\_no, fall back to the
        > current conflict resolution which is comparing revision
        > sequence number, CAS, expiration time and metadata flag
        > in order.

-   Supported Deployment

It can only be supported if the following deployments criterias are met.

1.  Couchbase server on both source and target cluster has to be on the
    > new version

2.  Both Couchbase server and Couchbase SDK has to be on the new version

##### 2.8.3.2.2 Last-Write-Win With Server-set Timestamp Using NTP

-   Prerequisites

Couchbase nodes in both source and target DC have NTP setup and sync
their clock from NTP servers of the same NTP stratum. (Please see
Appendix B for details about setting up NTP for supported platforms by
Couchbase)

-   Details

1.  Have a new configuration parameter **sys\_rev\_mode** which can be
    > changed by cli and REST API. sys\_rev\_mode=PT indicates using
    > local physical time as revision sequence number;
    > sys\_rev\_mode=Counter or leaving sys\_rev\_mode unspecified
    > indicates using counter as revision sequence number, which is our
    > existing way.

2.  Expand document metadata to add **doc\_rev\_mode** as a new
    > metadata field.

3.  

> if sys\_rev\_mode=PT

a.  EP engine get the high resolution time (in nanosecond) and use it as
    > the revision sequence number;

b.  set doc\_rev\_mode=timestamp

> if sys\_rev\_mode=Counter

a.  EP engine would act with the current behavior, set revision sequence
    > number using a counter;

b.  set doc\_rev\_mode=counter

<!-- -->

4.  The conflict resolution algorithm would be:

<!-- -->

a.  if doc\_rev\_mode is the same, then compare revision sequence
    > number, CAS, expiration time and metadata flag in order, the
    > version has bigger value win. If both version has the same value
    > on revision sequence number, CAS, expiration time and metadata
    > flag, the incoming value would be dropped

b.  if doc\_rev\_mode is different, the incoming value would win.

-   Supported Deployment

1.  We can’t support LWW on windows cluster, as windows doesn’t
    > guarantee “the accuracy of the W32Time service between nodes on
    > a network”.

2.  In rolling upgrade, administrator can’t set sys\_rev\_mode=true
    > until all nodes in the cluster are upgraded to the new version.

3.  Setting up a replication between a cluster with sys\_rev\_mode=true
    > and a cluster sys\_rev\_mode=false is not allowed.

##### 2.8.3.2.3 Last Write Win With HLC (Hybrid Logic Clock)

The idea of HLC is described in [****Logical Physical
Clocks****](http://www.cse.buffalo.edu/tech-reports/2014-04.pdf)

[****and Consistent Snapshots in Globally Distributed
Databases****](http://www.cse.buffalo.edu/tech-reports/2014-04.pdf). It
is also used in
[*Cockroach*](https://github.com/cockroachdb/cockroach/blob/master/hlc/hlc.go).

-   Data structure

HLC is an encoded 64-bit integer, with its first 48 bits representing
the maximum seen physical clock value, last 16 bits representing the
maximum seen logical clock value.

-   Algorithm

-   Prerequisites![](media/image09.png)

The same as the Prerequisites of [*Last Write Win With Server-set
Timestamp Using
NTP*](#last-write-win-with-server-set-timestamp-using-ntp)

-   Details

1.  Have a new configuration parameter **sys\_rev\_mode** which can be
    > changed by cli and REST API. sys\_rev\_mode=HLC indicates using
    > HLC as revision sequence number; sys\_rev\_mode=Counter or leaving
    > sys\_rev\_mode unspecified indicates using counter as revision
    > sequence number (our existing way).

    2.  Expand document metadata to add **doc\_rev\_mode** as a new
        > metadata field.

    3.  if sys\_rev\_mode = HLC

        a.  EP engine would decode the current revision sequence number
            > to l and c

        b.  update l and c according to above algorithm.

        c.  encode updated l and c to 64-bit integer, set it to revision
            > sequence number

        d.  set doc\_rev\_mode=HLC

> if sys\_rev\_mode = Counter

a.  EP engine would act the same as it does in 3.0, set revision
    > sequence number using a counter;

b.  set doc\_rev\_mode=Counter

<!-- -->

4.  The conflict resolution algorithm would be:

<!-- -->

a.  if doc\_rev\_mode is the same, compare revision sequence number,
    > CAS, expiration time and metadata flag in order, the version has
    > the bigger value win. If both version has the same value on
    > revision sequence number, CAS, expiration time and metadata flag,
    > the incoming value would be dropped.

b.  if doc\_rev\_mode is different, the incoming value would win.

##### 2.8.3.2.4 Version tree based conflict detection + custom defined conflict resolution

Alk’s proposal is at
[*https://docs.google.com/document/d/1X7q2BwfWH5vNvxVPo3XTgpqZVzT5NVC3Aol7zjVU8YE/edit?usp=sharing\_eil&invite=CO\_V9MAK*](https://docs.google.com/document/d/1X7q2BwfWH5vNvxVPo3XTgpqZVzT5NVC3Aol7zjVU8YE/edit?usp=sharing_eil&invite=CO_V9MAK)

#### 2.8.3.3 Comparison

  ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
                                                               Current XDCR                                                   LWW with client-set timestamp using NTP                                                                                                      LWW with server-set timestamp using NTP                                                                                                      LWW with server-set timestamp using HLC                                                                                                      Version Tree
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
                                                               (Local counter based)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 
  ------------------------------------------------------------ -------------------------------------------------------------- -------------------------------------------------------------------------------------------------------------------------------------------- -------------------------------------------------------------------------------------------------------------------------------------------- -------------------------------------------------------------------------------------------------------------------------------------------- --------------------------------------------------------------------------------------------------------------------------
  **Consistency**                                              Consistency is compromised for:                                Consistency is compromised for:                                                                                                              Consistency is compromised for:                                                                                                              Consistency is compromised for:                                                                                                              Consistency would not be compromised
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  (See write loss analysis at [*Appendix C*](#section-6))      1\. failure cases; 2.multi-master cases                        1\. time skew                                                                                                                                1\. time skew                                                                                                                                1\. time skew                                                                                                                                
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
                                                                                                                              2\. NTP time is not monotonic                                                                                                                2\. NTP time is not monotonic                                                                                                                                                                                                                                                             
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
                                                                                                                              But it is possible to achieve so-called “Session Consistency” when a single clock is used throughout a session.                                                                                                                                                                                                                                                                                                                        

  **Information Captured**                                     partial ordering                                               physical time of mutation                                                                                                                    physical time of mutation                                                                                                                    partial ordering,                                                                                                                            partial ordering,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                                                                                                                                                                                                        physical time of mutation                                                                                                                    causal relationships

  **Time Complexity**                                          O(1)                                                           O(1)                                                                                                                                         O(1)                                                                                                                                         O(1)                                                                                                                                         It consists of the following parts:
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  **(assume the metadata of the existing doc is in memory)**                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         1\. Time to read the supplementary metadata;
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     2\. Time to compare the version tree (grows with the size of the tre)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     3\. Time to resolve the conflict if in conflict

  **Space Complexity**                                         64 bit in document’s metadata                                  64 bit in document’s metadata                                                                                                                64 bit in document’s metadata                                                                                                                64 bit in document’s metadata                                                                                                                the size of tree (may be bounded if trim) in document’s body

  **Application Adoption Cost**                                No cost                                                        Application has to set the timestamp programmatically for each mutation                                                                      No cost                                                                                                                                      No cost                                                                                                                                      Application has to write the conflict resolution logic in script.
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     If Application would like LWW semantics

  **Flexibility**                                              Not flexible - system decided conflict resolution. No merge.   Not flexible - system decided conflict resolution. No merge                                                                                  Not flexible - system decided conflict resolution. No merge                                                                                  Not flexible - system decided conflict resolution. No merge                                                                                  Flexible - user decided conflict resolution. Can do merge.
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     But because it doesn’t capture the time information of the updates. User can’t easily implement LWW conflict resolution.

  **Backward Compatibility**                                                                                                  Yes with the exception that LWW won’t be supported in mixed deployment mode (some nodes are on new version; some nodes are on old version)   Yes with the exception that LWW won’t be supported in mixed deployment mode (some nodes are on new version; some nodes are on old version)   Yes with the exception that LWW won’t be supported in mixed deployment mode (some nodes are on new version; some nodes are on old version)   Unknown

  **Engineering Cost**                                                                                                        very low                                                                                                                                     low                                                                                                                                          low                                                                                                                                          high
  ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

The current analysis is favoring option **LWW with server-set timestamp
using HLC** given the constraint we have on development time and the
popularity of LWW semantics among applications according to the customer
feedbacks which PM gathered.

2.9 Filtering
-------------

### 2.9.1 Requirements

-   Filter based on key (Document Id) only

-   Filter is updateable at runtime

### 2.9.2 Competitive Analysis

  --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  Competitor                Filter Support                                                   Details
  ------------------------- ---------------------------------------------------------------- -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  Riak                      -   Filter is defined in code                                    Filter via replication hook
                                                                                             
                            -   Filter can’t be changed at runtime                           send\_realtime - This hook controls whether an object replicated in real time should be sent. To send this object, return ok, to prevent the object from being sent return 'cancel', or you can also return a list of riak objects to be replicated immediately before the current object. This is useful for when you have an object that refers to other objects—a chunked file, for example—and want to be sure all the dependency objects are replicated before the dependent object.
                                                                                             
                            -   Support filter based on metadata and value                   recv/1 - When an object is received by the client site, this hook is run. You can use it to update some metadata, or to deny the object.
                                                                                             
                                                                                             

  MongoDB                   None                                                             

  Cassandra                 None                                                             

  Couchbase                 None                                                             

  SQL Server                -   Filter is defined as expression                              [*http://msdn.microsoft.com/en-us/library/ms151775.aspx*](http://msdn.microsoft.com/en-us/library/ms151775.aspx)
                                                                                             
                            -   Filter can be changed at runtime                             -static row filter
                                                                                             
                            -   Filter can be defined at row level as well as column level   -column filter
                                                                                             
                                                                                             -dynamic row filter

  Oracle LDAP Replication   -   Filter is defined as expression                              [*http://docs.oracle.com/cd/B28196\_01/idmanage.1014/b15991/int\_rep.htm\#i1060994*](http://docs.oracle.com/cd/B28196_01/idmanage.1014/b15991/int_rep.htm#i1060994)
                                                                                             
                            -   Filter can be changed at runtime                             
                                                                                             
                                                                                             
  --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

### 2.9.3 How specify a filter

#### 2.9.3.1 Option1: As a regular expression

  Replication Name     Bucket   Remote Cluster          Remote Bucket   Filter
  -------------------- -------- ----------------------- --------------- -------------
  BackupHockeyToWest   A        west-cluster1.abc.com   AA              hockey\_.\*
  BackupHockeyToEast   A        east-cluster4.abc.com   AAA             hockey\_.\*
  BckupSoccerToWest    A        west-cluster2.abc.com   BB              soccor\_.\*

#### 2.9.3.2 Option2: As a custom js function per replication

  ------------------------------------------------------------------------------------------------------
  Replication Name     Bucket   Remote Cluster          Remote Bucket   Filter
  -------------------- -------- ----------------------- --------------- --------------------------------
  BackupHockeyToWest   A        west-cluster1.abc.com   AA              function(key) {
                                                                        
                                                                        if (key.indexOf(“hockey”)== 0)
                                                                        
                                                                        return true;
                                                                        
                                                                        else
                                                                        
                                                                        return false;
                                                                        
                                                                        }

  BackupHockeyToEast   A        east-cluster4.abc.com   AAA             function(key) {
                                                                        
                                                                        if (key.indexOf(“hockey”)== 0)
                                                                        
                                                                        return true;
                                                                        
                                                                        else
                                                                        
                                                                        return false;
                                                                        
                                                                        }

  BackupSoccerToWest   A        west-cluster2.abc.com   BB              function(key) {
                                                                        
                                                                        if (key.indexOf(“soccer”)== 0)
                                                                        
                                                                        return true;
                                                                        
                                                                        else
                                                                        
                                                                        return false;
                                                                        
                                                                        }
  ------------------------------------------------------------------------------------------------------

#### 2.9.3.3 Option3: Define replication data set for replication

-   Define Replication Data Channel for Bucket

1.  Replication data channels doesn’t have overlap;

2.  Categorization Function returns the channel id (integer) given a
    > document id

3.  channel id=0 is the default channel, it contains data that doesn’t
    > included in any named channel.

4.  All bucket has at least one Replication Data Channel - Default
    > channel, which doesn’t need to specifically defined by customer.

  ---------------------------------------------------------------------------------------
  Bucket   Replication Data Channel List           Categorization Function
  -------- --------------------------------------- --------------------------------------
  A        Default, HockeyChannel, SoccerChannel   function(key) {
                                                   
                                                   if (key.indexOf(“hockey”)== 0)
                                                   
                                                   return “A::HockeyChannel”;
                                                   
                                                   else if (key.indexOf(“soccer”) == 0)
                                                   
                                                   return “A::SoccerChannel”;
                                                   
                                                   else
                                                   
                                                   return “A::Default”;
                                                   
                                                   }
  ---------------------------------------------------------------------------------------

-   Define replication specification

  Replication Name         Description                                                   Replication Channels        Remote Cluster          Remote Bucket
  ------------------------ ------------------------------------------------------------- --------------------------- ----------------------- ---------------
  BackupHockeyToWest       replicate documents related to Hockey to west                 A::HockeyChannel            west-cluster1.abc.com   AA
  BackupHokeyToEast        replicate documents related to Hockey to east                 A::HockeyChannel            east-cluster4.abc.com   AAA
  BackupSoccerToWest       replicate documents related to Soccer to west                 A::SoccerChannel            west-cluster2.abc.com   BB
  BackupNonHockeyToEast2   replicated documents of non-hockey related to East2 cluster   A::SoccerChannel, Default   east-cluster2.abc.com   AAAA

### 2.9.4 Where in the tech stack filter should be supported?

1.  filter by key, filter by operation (update, delete or expiration),
    filter by metadata field is best to be supported by DCP (aka. UPR).

2.  XDCR would expose filter in the context of replication, then
    leverage UPR’s filter functionality.

### 2.9.5 Compare Options

  -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
         **As a regular expression**                                                  **As a custom js function per replication**                                  **Define replication data set for replication**
  ------ ---------------------------------------------------------------------------- ---------------------------------------------------------------------------- --------------------------------------------------------------------------
  Pros   1\. Flexible, can slice the bucket in many different ways -                  1\. Flexible, can slice the bucket in many different way -                   > 1\. Provides clearer mental picture on how data in a bucket are
         non-overlapping among partitions or overlapping among partitions.            non-overlapping among partitions or overlapping among partitions.            > categorized to customer
                                                                                                                                                                   >
         2\. Easy for DCP to evaluate without having to embed a script engine.                                                                                     > 2\. Can express non-overlapping partitions easily. This limits one
                                                                                                                                                                   > document can only belong to one channel
                                                                                                                                                                   >
                                                                                                                                                                   > 3\. Still can express overlapping replications by specifying the
                                                                                                                                                                   > overlapping set of data as a channel and include it in the replication
                                                                                                                                                                   > specification

  Cons   > 1\. Have to repeat the expression if customer wants the same set of data   1\. This requires a js script engine to be embedded in EP-Engine, which      1\. This requires a js script engine to be embedded in EP-Engine, which
         > send to two different remote clusters in two different replication         raises safety concern.                                                       raises safety concern.
         > specification.                                                                                                                                          
         >                                                                            > 2\. Have to repeat the function body if customer wants the same set of     2\. Two-step configuration, instead of one-step.
         > 2\. By looking at the configuration, it is not easy for customer to tell   > data send to two different remote clusters in two different replication    
         > what kind of the replication topology it forms. For example, it is hard    > specification.                                                             
         > to tell it is a bi-directional overlapping or nonoverlapping setup.        >                                                                            
         >                                                                            > 3\. By looking at the configuration, it is not easy for customer to tell   
         > 3\. And it leaves to admin to the right thing as we can’t valid if         > what kind of the replication topology it forms. For example, it is hard    
         > different filter expressions would work together.                          > to tell it is a bi-directional overlapping or nonoverlapping setup.        
                                                                                      >                                                                            
                                                                                      > 4\. And it leaves to admin to the right thing as we can’t valid if         
                                                                                      > different filter script would work together.                               
                                                                                      >                                                                            
                                                                                      > 5\. Using a function doesn’t have much advantages over an expression as    
                                                                                      > the only input here is Document Id. Function seems to be too verbose for   
                                                                                      > this purpose.                                                              
  -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Based on the above comparison. Option \#2 and \#3 are not chosen due to
the safety concern of running custom js script inside ep-engine, which
can possibly crash the engine. The current thinking is Option \#1.

2.10 Backward Compatibility
---------------------------

### 2.10.1 Rolling upgrade

1.  New XDCR can coexist with the XDCR in earlier version. In this
    scenario, part of the data would be replicated via new XDCR if the
    node is upgraded, and part of the data would be replicated via XDCR
    in earlier version.

2.  New XDCR replication manager can recognize metadata (replication
    settings file and checkpoint file) format and can migrate the
    content to distributed metadata store

3.  Limitation: during rolling upgrade, admin can’t configure XDCR using
    new features - configurable conflict resolution and filtering.
    Currently we don’t have a good way to guard this, this needs to be
    documented as is.

### 2.10.2 Metadata migration

If a replication specification is created in earlier version, its
metadatas (replication specification, settings, and checkpoint document)
are saved in local db. Those metadata need to be migrated to new format
and saved in distribute metadata store. The following is the migration
process.

1.  xdc\_rdoc\_replication\_srv (the erlang code which manages the
    replication specification and settings) still in erlang and running
    in ns\_server

2.  When Replication Manager starts, it calls Erlang XDCR Rest interface
    to find all the old replication metadatas. It migrates them to new
    format and save them in distributed metadata store.

3.  Replication Manager remembers the migration has done, so the next
    time it restarts, the migration would be skipped.

3 Programming Language Choice
=============================

Golang is chosen as the programming language for this implementation.

4 External Dependency
=====================

  ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  Component                    Requirement Summary                                          Requirement Details                                                                                                                                                                                                                                          Required By
  ---------------------------- ------------------------------------------------------------ ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ -----------------------------------------------------------------------------------------------------
  Distributed Metadata Store   1\. Query metadata with a criteria                           one example is: if the key for Replication Specification is &lt;source cluster id&gt;\_&lt;source bucket name&gt;\_&lt;target cluster id&gt;\_&lt;target bucket name&gt;; and it needs to find all the replications with my cluster as the source cluster.   

                               2\. Get Metadata with key                                    return the metadata for a given key                                                                                                                                                                                                                          

                               3\. Save Metadata                                            save the metadata given the metadata body and key                                                                                                                                                                                                            

                               4\. Delete Metadata                                          delete the metadata for a given key.                                                                                                                                                                                                                         
                                                                                                                                                                                                                                                                                                                                                         
                                                                                            One use case would be when filter expression is changed for a replication specification, the corresponding checkpoint files for the replication need to be deleted                                                                                           

  EP-Engine                    1\. EP-Engine support HLC as revision sequence number                                                                                                                                                                                                                                                                     [*Last-write-win with server-set timestamp using HLC*](#last-write-win-with-hlc-hybrid-logic-clock)

                               2\. DCP filer support (MB-11627                              For XDCR’s next release, we only need filter by key.                                                                                                                                                                                                         [*Filter*](#filtering)
                                                                                                                                                                                                                                                                                                                                                         
                               Expose DCP (UPR) as a public protocol for change tracking)   1\. DCP’s Stream Request protocol allow user to specify a regular                                                                                                                                                                                            
                                                                                            expression (e.g. hockey\_.\*) as “Key Filter”, which means the                                                                                                                                                                                               
                                                                                            protocol’s extra section need to be expanded to include “Key Filter”.                                                                                                                                                                                        
                                                                                            “Key Filter” is a variable length string.                                                                                                                                                                                                                    
                                                                                                                                                                                                                                                                                                                                                         
                                                                                            2\. DCP would apply the regular expression to only stream out the                                                                                                                                                                                            
                                                                                            mutations with matching key if “Key Filter” appear in Stream Request.                                                                                                                                                                                        
  ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

5 Open Design Issues
====================

  ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  <span id="h.lscnesc0c5kr" class="anchor"></span>Issue \#   <span id="h.14do6mo1y1ql" class="anchor"></span>Description
  ---------------------------------------------------------- ----------------------------------------------------------------------------------------------------------------------------------------------------
  <span id="h.ey4yghc7ipo6-1" class="anchor"></span>1        <span id="h.njsxl96dbots-1" class="anchor"></span>Conflict resolution strategy

  <span id="h.ey4yghc7ipo6" class="anchor"></span>2          <span id="h.fyhlmwdkp4n3" class="anchor"></span>Do we need Filter based on operation type (delete or expiration), Filter based on metadata fields?
                                                             
                                                             <span id="h.hi12fildnws" class="anchor"></span>
                                                             
                                                             <span id="h.njsxl96dbots" class="anchor"></span>This came up during our discussion with Matt
  ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

6 Appendix A - Past Issues Summary![](media/image11.png)
========================================================

### 

### 

7 Appendix B - NTP Setup
========================

  ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  **Platform**                          **Deprecated Version **   **Unsupported Version**   **NTP Support**   **Instructions to setup NTP**                                                                                                                                                                                                                                                     **Note**
  ------------------------------------- ------------------------- ------------------------- ----------------- --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  **Red Hat Enterprise Linux 5**        32-bit Linux in 2.5.x     32-bit in 3.0             Y                 [*https://access.redhat.com/documentation/en-US/Red\_Hat\_Enterprise\_Linux/5/html/Deployment\_Guide/s1-dateconfig-ntp.html*](https://access.redhat.com/documentation/en-US/Red_Hat_Enterprise_Linux/5/html/Deployment_Guide/s1-dateconfig-ntp.html)                              

  **Red Hat Enterprise Linux 6**        32-bit Linux in 2.5.x     32-bit in 3.0             Y                 [*https://access.redhat.com/documentation/en-US/Red\_Hat\_Enterprise\_Linux/6/html/Deployment\_Guide/ch-Configuring\_NTP\_Using\_ntpd.html*](https://access.redhat.com/documentation/en-US/Red_Hat_Enterprise_Linux/6/html/Deployment_Guide/ch-Configuring_NTP_Using_ntpd.html)   

  **Red Hat Enterprise Linux on EC2**                                                       Y                 [*http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/set-time.html\#d0e30478*](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/set-time.html#d0e30478)                                                                                                                         

  **CentOS 5**                          3.0                       3.0.x                     Y                 [*http://www.cyberciti.biz/faq/howto-install-ntp-to-synchronize-server-clock/*](http://www.cyberciti.biz/faq/howto-install-ntp-to-synchronize-server-clock/)                                                                                                                      

  **CentOS 6**                                                                              Y                 [*http://www.cyberciti.biz/faq/howto-install-ntp-to-synchronize-server-clock/*](http://www.cyberciti.biz/faq/howto-install-ntp-to-synchronize-server-clock/)                                                                                                                      

  **CentOS 6 on EC2**                                                                       Y                 [*http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/set-time.html\#d0e30478*](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/set-time.html#d0e30478)                                                                                                                         

  **Amazon Linux 2011.09**                                                                  Y                 NTP is setup by default.                                                                                                                                                                                                                                                          
                                                                                                                                                                                                                                                                                                                                                                                                
                                                                                                              [*http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/set-time.html\#d0e30478*](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/set-time.html#d0e30478)                                                                                                                         

  **Ubuntu Linux 10.04**                3.0                       3.0.x                     Y                 [*https://help.ubuntu.com/10.04/serverguide/NTP.html*](https://help.ubuntu.com/10.04/serverguide/NTP.html)                                                                                                                                                                        

  **Ubuntu Linux 12.04**                                                                    Y                 [*https://www.digitalocean.com/community/tutorials/how-to-set-up-time-synchronization-on-ubuntu-12-04*](https://www.digitalocean.com/community/tutorials/how-to-set-up-time-synchronization-on-ubuntu-12-04)                                                                      

  **Ubuntu 12.04 on EC2**                                                                   Y                 [*http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/set-time.html\#d0e30478*](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/set-time.html#d0e30478)                                                                                                                         

  **Windows 2008 R2 with SP1**          32-bit in 2.5.x           32-bit in 3.0             N                                                                                                                                                                                                                                                                                                   “We do not guarantee and we do not support the accuracy of the W32Time service between nodes on a network. The W32Time service is not a full-featured NTP solution that meets time-sensitive application needs. The W32Time service is primarily designed to do the following:
                                                                                                                                                                                                                                                                                                                                                                                                
                                                                                                                                                                                                                                                                                                                                                                                                Make the Kerberos version 5 authentication protocol work.
                                                                                                                                                                                                                                                                                                                                                                                                
                                                                                                                                                                                                                                                                                                                                                                                                Provide loose sync time for client computers.The W32Time service cannot reliably maintain sync time to the range of 1 to 2 seconds. Such tolerances are outside the design specification of the W32Time service.”
                                                                                                                                                                                                                                                                                                                                                                                                
                                                                                                                                                                                                                                                                                                                                                                                                [*http://support.microsoft.com/kb/939322*](http://support.microsoft.com/kb/939322)
                                                                                                                                                                                                                                                                                                                                                                                                
                                                                                                                                                                                                                                                                                                                                                                                                [](http://support.microsoft.com/kb/939322)

  **Windows 2012**                      32-bit in 2.5.x           32-bit in 3.0             N                                                                                                                                                                                                                                                                                                   “We do not guarantee and we do not support the accuracy of the W32Time service between nodes on a network. The W32Time service is not a full-featured NTP solution that meets time-sensitive application needs. The W32Time service is primarily designed to do the following:
                                                                                                                                                                                                                                                                                                                                                                                                
                                                                                                                                                                                                                                                                                                                                                                                                Make the Kerberos version 5 authentication protocol work.
                                                                                                                                                                                                                                                                                                                                                                                                
                                                                                                                                                                                                                                                                                                                                                                                                Provide loose sync time for client computers.The W32Time service cannot reliably maintain sync time to the range of 1 to 2 seconds. Such tolerances are outside the design specification of the W32Time service.”
                                                                                                                                                                                                                                                                                                                                                                                                
                                                                                                                                                                                                                                                                                                                                                                                                [*http://support.microsoft.com/kb/939322*](http://support.microsoft.com/kb/939322)

  **Windows 7**                         32-bit in 2.5.x           32-bit in 3.0             N                                                                                                                                                                                                                                                                                                   “We do not guarantee and we do not support the accuracy of the W32Time service between nodes on a network. The W32Time service is not a full-featured NTP solution that meets time-sensitive application needs. The W32Time service is primarily designed to do the following:
                                                                                                                                                                                                                                                                                                                                                                                                
                                                                                                                                                                                                                                                                                                                                                                                                Make the Kerberos version 5 authentication protocol work.
                                                                                                                                                                                                                                                                                                                                                                                                
                                                                                                                                                                                                                                                                                                                                                                                                Provide loose sync time for client computers.The W32Time service cannot reliably maintain sync time to the range of 1 to 2 seconds. Such tolerances are outside the design specification of the W32Time service.”
                                                                                                                                                                                                                                                                                                                                                                                                
                                                                                                                                                                                                                                                                                                                                                                                                [*http://support.microsoft.com/kb/939322*](http://support.microsoft.com/kb/939322)

  **Windows 8**                         32-bit in 2.5.x           32-bit in 3.0                                                                                                                                                                                                                                                                                                                 “We do not guarantee and we do not support the accuracy of the W32Time service between nodes on a network. The W32Time service is not a full-featured NTP solution that meets time-sensitive application needs. The W32Time service is primarily designed to do the following:
                                                                                                                                                                                                                                                                                                                                                                                                
                                                                                                                                                                                                                                                                                                                                                                                                Make the Kerberos version 5 authentication protocol work.
                                                                                                                                                                                                                                                                                                                                                                                                
                                                                                                                                                                                                                                                                                                                                                                                                Provide loose sync time for client computers.The W32Time service cannot reliably maintain sync time to the range of 1 to 2 seconds. Such tolerances are outside the design specification of the W32Time service.”
                                                                                                                                                                                                                                                                                                                                                                                                
                                                                                                                                                                                                                                                                                                                                                                                                [*http://support.microsoft.com/kb/939322*](http://support.microsoft.com/kb/939322)

  **Mac OS 10.7**                                                                           Yes               [*http://support.apple.com/kb/TA24116*](http://support.apple.com/kb/TA24116)                                                                                                                                                                                                      

  **Mac OS 10.8**                                                                           Yes               [*http://support.apple.com/kb/TA24116*](http://support.apple.com/kb/TA24116)                                                                                                                                                                                                      
  ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

8 Appendix C - Data Loss Analysis For Conflict Resolution Options

![](media/image01.png)
======================

> ![](media/image06.png)
>
> ![](media/image03.png)
>
> ![](media/image10.png)
