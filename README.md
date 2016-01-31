# multipath-openflow
Multi failsafe routing mechanism for ODL and OvS

The source code used for master's dissertation __Critical Ethernet based on
OpenFlow__

There are 2 versions:

The first one, more naive, only create 2 disjoint paths (can pass on same nodes,
but on different links).

<img src="https://raw.githubusercontent.com/aanm/multipath-openflow/master/images/version1.png" width="300">

On this example the disjoint paths are: 1-2-3 and 1-4-5-3.

The second one, more robust, creates a backup path for each link on the primary
path.

<img src="https://raw.githubusercontent.com/aanm/multipath-openflow/master/images/version2.png" width="300">

On this example the backup path for link 5-3 is 5-4-2-3.
