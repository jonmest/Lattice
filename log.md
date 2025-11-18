# Nov 18
Set up initial data structures. 

A Raft node must have:
1. Identifier
1. Term
1. Log
1. State like leader, follower, candidate
1. Peer list
1. Commit inidex
1. Applied inidex
1. Election timer

We must support the ops:
1. RequestVote
1. AppendEntries
1. InstallSnapshot

I think we will use tonic for Grpc. It seems mature, albeit there may come breaking changes soon as per their documentation. 
