# Chord_Scala
an implementation of the Chord lookup service in Scala and SBT.

Implement the network join and routing as described in the Chord paper.

To run the code, the format is like below:
sbt "run 200 2"
Input should be: numNodes numRequest

numRequest is the number of requests that each node send after finishing build Chord Ring.
For example, "run 200 2" means after joining "200" node, each node send "2" random data into the ring.
