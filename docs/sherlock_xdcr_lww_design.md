---
title: |
    <span id="h.lkazmumwmiib" class="anchor"></span>Sherlock XDCR
    Last-Write-Win Design
...

Background
==========

XDCR is used by many customers as an availability solution. Failing over
under cluster wide or partial failures to a secondary cluster is
critical for customers to be able to provide 100% availability on their
database tier. However our existing RevID based model does behave
ideally under these failover conditions.

With Sherlock, we will introduce a new conflict resolution mechanism
option - Last-Write-Win that will target making XDCR an ideal solution
for high availability use cases

Please refer to
[PRD](https://docs.google.com/document/d/172zoJHB0tZ2RbXYE7yl7oB3_EOBjlOWJztsuEQ0-B5U/edit#)
for details

Technical Design
================

Time-keeping
------------

### The ability to order mutations in distributed system is essential for last-write-win conflict resolution. Timestamp (physical, logical or hybrid) is usually used as a criteria to order the mutation. Time-keeping in distributed system refers to the mechanism that keep the timestamp in different nodes are consistent and meaningful to be used as a criteria to order the mutations. Different approaches differs on their choice of timestamp and the time-keeping algorithm across nodes. Below our approach of using hybrid logic clock as timestamp and the way to keep time in-sync is discussed.

### Hybrid logical clock (HLC)

#### Why? 

Our existing RevId is a logical clock timestamp. It posses the monotonic
property on normal case and it can capture Happen-Before causality
relationship via message exchange, but it can’t detect the conflict and
order the mutation across the nodes. Vector clock is another logical
clock choice, it can correctly detect the conflict, but its space
requirement is the order of the nodes in the system. Synchronized
physical time can order mutations across the the nodes, but
time-synchronization mechanism involves clocking adjustment, which makes
synchronized physical time can move back. The lost of monotonicity makes
ordering mutation with it un-deterministic.

Hybrid logic clock is presented as another timestamp choice in paper [
](http://www.cse.buffalo.edu/tech-reports/2014-04.pdf)[****Logical
Physical Clocks and Consistent Snapshots in Globally Distributed
Databases****](http://www.cse.buffalo.edu/tech-reports/2014-04.pdf). It
is an encoded 64-bit integer. Its first 48 bits represents maximum seen
time (up to 0.1 millisecond precision); its last 16 bits represents
logical counter. HLC has some nice properties:

1.  HLC is monotonic per document. Its logical counter will start to
    > increment when either the clock value is smaller or equal to the
    > current maximum seen time, which makes it not suffering the
    > potential moving-backward of wall clock.

2.  HLC also capture Happen-Before relation via message exchange just
    > like logical clock. Because it has physical time component, these
    > message exchange also has the effect of synchronizing time and
    > reflecting them in the timestamp. It can mask time synchronization
    > error to some degree until it is fixed.

3.  HLC is close to physical time. The difference between HLC timestamp
    > and the physical time is proved to be bounded. It means as long as
    > physical time is synchronized, HLC can be used to determine the
    > order of the mutations in distributed system.

4.  HLC is superposition on top of synced physical time. It doesn’t
    > change physical time, which means it would not interfere with time
    > synchronization mechanism. It is time synchronization
    > mechanism agnostic.

HLC is described in detail in the paper[
](http://www.cse.buffalo.edu/tech-reports/2014-04.pdf)[****Logical
Physical Clocks and Consistent Snapshots in Globally Distributed
Databases****](http://www.cse.buffalo.edu/tech-reports/2014-04.pdf).

#### Adopt HLC in CAS

Existing CAS in document metadata is a manipulated physical timestamp
(mostly physical timestamp with some tweaks to deal with insufficient
precision), which is used for optimistic locking. The thinking is to
make CAS using HLC instead of physical timestamp. Given the properties
of HLC, it should be able to satisfy the need tof optimistic locking as
well as ordering mutation for LWW.

The following describes our adoption of HLC. The original HLC algorithm
computes the HLC value based on the existing HLC value and physical
time. Since the existing HLC value is in CAS, it means to retrieve the
document metadata. When document metadata is kept in memory, it is not
an expensive option, but in full eviction mode it could have big
performance impact on the system. In Our adoption of the algorithm, we
try to avoid retrieving document metadata.

-   Maintain max\_cas per vbucket

> A maximum seen CAS value (**max\_cas**) is maintained per vbucket by
> ep-engine. It would be used to calculate HLC in the place of existing
> HLC.

1.  max\_cas is initialized to 0

2.  max\_cas is set if a higher CAS value is observed by ep engine via
    > mutation, xdcr replication or DCP intra-cluster replication.

3.  max\_cas need to be persisted to disk, so in the case of ep-engine
    > restart, the value can be brought back.

-   Algorithm

**cas.T** - refers to the time component in CAS, the high 48 bits

**cas. LC** - refers to the logical counter in CAS, the low 16 bits

**AT** - refers to adjusted time. It is the synchronized time among a
group of nodes

if AT &gt; max\_cas.T

cas.T = AT

cas.LC = 0

else

cas = max\_cas +1

### Time Synchronization Mechanism

The most popular time synchronization mechanism is NTP. Its algorithm is
mature and has been tested in real word, but it has a number of
limitations: 1. setup complexity; 2. limit support on Windows platform
and VM. These limitation would increase the cost of ownership for our
customers, which in turn will make LWW less practical for them.

With that in mind, we are thinking of a tiered solution. Tier one is a
simple time synchronization mechanism provided by couchbase itself,
which is piggy-back on our replication stream (xdcr and intra-cluster
replication). Time synchronized by our built-in time synchronization
would possibly have a bigger time skew window than NTP. But it has no
additional setup required, it would help enabling high availability for
applications whose update concurrency requirement is not high and can
tolerate bigger time skew window. For applications that has higher
update concurrency requirement and demand smaller time skew window, they
can set up NTP in addition. Our built-in time synchronization mechanism
need to be NTP agnostic.

#### Built-in time synchronization

Our built-in time synchronization mechanism doesn’t modify the wall
clock, which makes it independent of NTP. The goal of the
time-synchronization is to agree on a time (not necessarily be the true
time) among involved nodes for a given vbucket. Different vbucket on the
same node could have different agreed-on time. We choose the maximum
adjusted time (please refer to adjusted time definition below) seen as
the agreed-on time. The mechanism maintains a drift counter per vbucket
to record the time difference between the node’s wall clock and the
agreed-on time.

-   Terms:

> **WT** - Wall clock time
>
> **DC** - Drift counter. It records the time difference between the
> current wall clock value and the sender’s adjusted time. It is per
> vbucket and needs to be persisted to disk. It is initialized to the
> smallest possible 48 integer.
>
> **AT** - Adjusted time. It is the agreed-on time among nodes. It can
> be calculated by AT = high 48 bit of WT + DC. It is per vbucket.
>
> **Replication Group** - A group of nodes that exchange replication
> message (xdcr or intra-cluster) for a vbucket. It is per vbucket.

-   Mechanism:

> There are two time synchronization states - unsynchronized,
> synchronized for each node. There are three actions to move states.
> They are:

-   Initial time-sync - It gets all nodes to the same starting
    > adjusted time. This action moves state from unsynchronized
    > to synchronized.

> For each vbucket, do

1.  XDCR to get the initial adjusted time (AT\_0)

-   either from administrator

-   or from getting the adjusted time from an existing member in
    > replication group if it is the case that a new cluster is joining
    > the replication group

> *\* ep-engine expose getAdjustedTime memcached method*

2.  XDCR call setInitialDriftCounters to set the drift counter (DC) for
    > all vbucket in the bucket using DC = AT\_0 - WT

> *\* ep-engine expose setInitialDriftCounters memcached method, which
> accepts vbucket number and the initial drift counter*
>
> ep-engine set per bucket per node metadata **time\_synchronized** to
> true

-   time-sync via replication (inter-cluster and intra cluster) - It
    > maintains the state as synchronized. This action would only be
    > executed when time\_synchronized = true.

1.  When sending replication message, add source vbucket’s AT in
    > replication message to target vbucket

    a.  For XDCR, extend setWithMeta and delWithMeta message body to add
        > source bucket’s AT in Extras

    b.  For intra-cluster replication, extend DCP message body to add
        > source bucket’s AT

2.  When receiving replication message, when ep-engine received
    > replication message (via xdcr or intra-cluster replication) with
    > source vbucket’s AT, it calculates its DC by

> if AT &lt; AT\_received
>
> DC = AT\_received - WT
>
> Note: AT\_received denotes source vbucket’s AT in replication message

-   time-sync unset - it moves the state from synchronized
    > to unsynchronized.

1.  DC is reset to its initial value, which is the smallest possible 48
    > bit integer

2.  per bucket per node metadata **time\_synchronized** to false

-   Limitations

> The mechanism described above has the following limitation

1.  It ignores the network latency when calculating the drift counter.

2.  The time synchronization frequency depends on update frequency due
    > to the fact that we piggy-back time synchronization on
    > replication message. If the update frequency is low, time will be
    > synchronized less frequently. In between the synchronization, time
    > would have a larger skew.

> With those limitations, the rough estimation of time skew in a
> replication group using this time synchronization mechanism in steady
> state can be:
>
> time skew = network latency between two nodes + replication interval
> \* wall clock drift rate

-   Wall clock stepping

> In the case of wall clock is stepped (adjusted forward or backward) on
> one node after initial time-sync (**time\_synchronize**d is set to
> true), the time skew between this node and its peers in replication
> group would be temporarily bigger until the node received the
> replication message via xdcr message or dcp message.
>
> It might be interesting to log it when such stepping happens, but how
> to detect wall clock stepping? -- Open Issue \#5

-   Replication topologies and time-synchronization

> For uni-directional XDCR, the local master broadcast its own time to
> its local replica and remote replica. The result of time
> synchronization make AT\_local-master &lt;= AT\_local-replica and
> AT\_local-master &lt;= AT\_remote-replica.
>
> How does this help with the possible data loss during failover? For
> uni-directional xdcr, as long as we can guarantee that remote replica
> see mutation with monotonic increasing CAS, there will be no data
> loss. That holds true for normal case. In failover case when local
> replica becomes new master, local replica may not have the latest
> mutation, but the remote replica may have the latest mutation.
> AT\_master &lt;= AT\_replica helps that to hold true as well in that
> case.
>
> For bi-directional XDCR, the local master at the same time is also the
> remote replica of the reverse-direction XDCR replication. The result
> of time synchronization would make AT\_local-master =
> AT\_remote-replica and AT\_local-master &lt;= AT\_local-replica
> eventually with time skew.

Document’s LWW eligibility 
---------------------------

Only when the time in CAS is synchronized time in the replication group,
CAS can be used for last-write-win conflict resolution. This design
requires extending document metadata to add one bit - **lww\_eligible**.
This bit flags if the document is eligible for last-write-win conflict
resolution. **lww\_eligible** is an optional metadata, if not set, it is
treated as false. So old document created before Sherlock, would be
considered as lww\_eligible = false.

The rule of setting **lww\_eligible** flag for any mutation is:

> if **time\_synchronized = true**
>
> lww\_eligible = true
>
> else
>
> lww\_eligible = false

Conflict Resolution Algorithm
-----------------------------

-   conflict\_resolution\_mode

**conflict\_resolution\_mode** is a bucket metadata, it is configured by
administrator. Its value can be **RevId** or **LWW**

-   Algorithm

lww\_eligible\_incoming - the value of lww\_eligible flag in incoming
document

lww\_eligible\_existing - the value of lww\_eligible flag in existing
document

> if conflict\_resolution\_mode = LWW
>
> && lww\_eligible\_incoming = true
>
> && lww\_eligible\_existing = true
>
> Compare CAS, RevId, expiration, flag in order, the bigger value win
>
> else
>
> Compare RevId, CAS, expiration, flag in order, the bigger value win

Configuration
-------------

The following configurations are exposed via UI, REST and CLI

-   Enable\\disable time synchronization on Bucket

> Assume it is a cluster-wide per bucket metadata (for the purpose of
> discussion, call it **time\_sync\_enable**), which administrator can
> enable and disable. (Open Issue \#1, need PM review and confirm). This
> flag signals if the nodes in this cluster would broadcast its own
> adjusted time via intra-cluster or XDCR replications

1.  By default, time synchronization is disabled for a bucket
    > (time\_sync\_enabled=false, or doesn’t exist). Administrator has
    > to explicitly enable it.

2.  When enabling\\disabling time synchronization, any existing outgoing
    > replication (only XDCR replication, not including DCP replication)
    > from that bucket would be paused automatically. Administrator has
    > to manually restart the replication.

3.  When time synchronization is disabled, time\_synchronized is set to
    > false via time-sync unset action for the bucket.

4.  

-   Set conflict\_resolution\_mode on Bucket

> **conflict\_resolution\_mode** is a cluster-wide per bucket metadata,
> admin can change its value via UI, REST and CLI.
>
> This flag indicate how incoming conflicts via XDCR replications would
> be resolved. This flag is an intention or best effort attempt for
> conflict\_resolution\_mode=lww. Whether or not an particular document
> can be resolved with lww depends on both document’s lww’s eligibility.

Start time synchronization
--------------------------

-   When replication starts\\restarts

1.  if time synchronization is enabled on source bucket

> call init\_time\_synchronization routine
>
> start replication

2.  else

> start replication

-   The logic of init\_time\_synchronization routine

1.  if source bucket has time\_synchronized = false && target bucket has
    > time\_synchronized = false, it means this is a new replication
    > group to be formed. For each vbucket,

    a.  on source bucket, invoke initial time-sync action with the
        > initial adjusted time to be the wall clock time on vbucket
        > master node

    b.  if target bucket has time\_sync\_enabled = true, invoke initial
        > time-sync action with the initial adjusted time as source
        > bucket’s adjusted time at that time

2.  if source bucket has time\_synchronized = true && target bucket has
    > time\_synchronized = false, it means target bucket is joining an
    > existing replication group.

    a.  For every vbucket in target bucket, initial adjust time need to
        > be get from the existing replication group and set via initial
        > time-sync action

3.  if source bucket has time\_synchronized = false && target bucket has
    > time\_synchronized = true, it means source bucket is joining an
    > existing replication group.

    a.  For every vbucket in source bucket, initial adjust time need to
        > be get from the existing replication group and set via initial
        > time-sync action

4.  if source bucket has time\_synchronized = true && target bucket has
    > time\_synchronized = true && there exists at least one vbucket in
    > source bucket, its adjusted time doesn’t equal to its
    > corresponding target vbucket’s adjusted time. it means source
    > bucket and target bucket each belong to a different replication
    > group before. Their time are each considered to be synchronized
    > within their original replication group, but not synchronized when
    > the two replication group join.

    a.  For each vbucket in target bucket, invoke time-sync unset action
        > to unset time\_synchronized to false.

    b.  Pause any existing outgoing replications from target bucket

    c.  For each vbucket in target bucket, invoke initial time-sync
        > action with initial adjusted time that is got from source
        > vbucket,

    d.  Restart the outgoing replications from target bucket. This would
        > end up recursively call init\_time\_synchronization routine.
        > It will recalibrate time for all nodes in target bucket’s
        > original replication

Supportability
--------------

&lt;TBD&gt; Need PM functional input - Open Issue \#2

Upgrade
-------

1.  Configurations - enable\\disable time synchronization on bucket, set
    > conflict\_resolution\_mode on bucket are only available in
    > Sherlock; For buckets created in version prior to Sherlock, by
    > default, it is not enabled for time synchronization and
    > conflict\_resolution\_mode is RevId.

2.  Administrator need to enable time synchronization on the bucket
    > explicitly and restart the outgoing replication from the bucket to
    > start the built-in time synchronization.

Dependency Summary
==================

  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  Ticket \#                                                        Area              Description
  ---------------------------------------------------------------- ----------------- -----------------------------------------------------------------------------------------------------------------
  [*MB-12692*](http://www.couchbase.com/issues/browse/MB-12692)    ep-engine         [*Adopt HLC in CAS*](#adopt-hlc-in-cas)

  [*MB-12692*](http://www.couchbase.com/issues/browse/MB-12692)    ep-engine         Required by [*Document’s LWW eligibility*](#documents-lww-eligibility)
                                                                                     
                                                                                     1.  set lww\_eligible flag for every mutation
                                                                                     
                                                                                     

  [*MB-12692*](http://www.couchbase.com/issues/browse/MB-12692)    ep-engine         [*Conflict Resolution Algorithm*](#conflict-resolution-algorithm)

  [*MB-12692*](http://www.couchbase.com/issues/browse/MB-12692)    ep-engine         Required by [*Configuration*](#configuration)
                                                                                     
                                                                                     1.  per bucket metadata time\_synchronization\_enabled
                                                                                     
                                                                                     2.  per bucket metadata conflict\_resolution\_mode
                                                                                     
                                                                                     

  [*MB-12772*](https://www.couchbase.com/issues/browse/MB-12772)   ep-engine         Required by [*built-in time synchronization*](#built-in-time-synchronization)
                                                                                     
  Sherlock stretch goal                                                              1.  expose getAdjustedTime memcached method
                                                                                     
                                                                                     2.  expose setInitialDriftCounters memcached method, which accepts vbucket number and the initial drift counter
                                                                                     
                                                                                     3.  extend setWithMeta message to include 48 bit adjusted time in Extra
                                                                                     
                                                                                     4.  extend DCP message to include 48 bit adjusted time
                                                                                     
                                                                                     5.  maintain and persist drift counter per VBucket
                                                                                     
                                                                                     

                                                                   tools             Required by [*Configuration*](#configuration)
                                                                                     
                                                                                     1.  expose time\_synchronization\_enabled on UI
                                                                                     
                                                                                     2.  expose metadata\_conflict\_resolution\_mode on UI
                                                                                     
                                                                                     

                                                                   backup\\restore   Required by [*Document’s LWW eligibility*](#documents-lww-eligibility)
                                                                                     
                                                                                     1.  per document flag lww\_eligible need to be copied when using backup\\restore
                                                                                     
                                                                                     
  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Open Issues
===========

  -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  \#   Type         Description                                                                                                                                                                  Resolution
  ---- ------------ ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- ------------
  1    Functional   Do we make time synchronization optional via configuration? If the customer doesn’t plan to use LWW feature, they don’t have to have the overhead of time synchronization.   
                                                                                                                                                                                                 
                    If Yes, where admin should configure? Is it per cluster, per bucket or per replication?                                                                                      

  2    Functional   Supportability requirements                                                                                                                                                  
                                                                                                                                                                                                 
                    -   stats                                                                                                                                                                    
                                                                                                                                                                                                 
                    -   logs                                                                                                                                                                     
                                                                                                                                                                                                 
                                                                                                                                                                                                 

  3    Functional   Dependency on tools to expose conflict\_resolution\_mode and time\_synchronization\_enabled?                                                                                 

  4    Functional   Dependency on backup\\restore to copy lww\_eligible flag?                                                                                                                    

  5    Technical    How to detect wall clock stepping? It would be helpful to log it when such stepping happens                                                                                  
  -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
