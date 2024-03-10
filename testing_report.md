We designed a comprehensive testing script that includes integration tests as well as unit tests for our RRMQ system. We used the testing framework provided and extend the tests to cover both edge cases and nominal tests. 

Specifically, to test message queue, we devised the following testing situation:

1. Setting a topic
2. Setting the same topic twice
3. Setting non-string type topic
4. Submiting an invalid request
5. A sequence of actions that combines the first 4 actions
6. Retreiving a non-existing topic
7. Retreiving a message of an existing topic but contains no message

Combined with the given message queue tests, this covers all possible errors and successes that requests to each endpoint could hit. A possible shortcoming of this testing approach is that we did not test behavior if there were multiple clients sending requests to nodes, as we followed the given integration test configuration, which sends all the requests from the same client. Although we suspect that the result would not change if there were multiple different clients sending requests, we would need to test that to make sure.

For election testing, beyond the provided tests, we also devised following situations:
1. Nominal test case for verifying a leader exists in a two node and three node swarm
2. Testing that old leader timeout would cause a transfer of leadership and verifies that it restart as a follower
3. Test that when the majority of ndoes die, no leader is available
4. Test that when the leader and 2 follower die in a 5 node swarm, no leader is selected because majority of node died
5. Test that when a leader die in a 2 node swarm, a leader is not selected because majority of node died
6. Test that a 1 node swarm have no leader after the node died
7. Check that if there's a partition (if many nodes got paused), an elected leader will still be leader


In our tests, we tested configurations of 1-9 nodes, which allowed us to check election behavior worked with large and small numbers of nodes, and also allowed us to ensure that election behavior worked as expected with even and odd numbers of nodes. A shortcoming of our testing was that we could not directly cause specific nodes to time  out and start an election (like we could do in the raft visualization on raft.github.io), and we also did not track elections at the granularity of individual votes and terms. We instead hoped that with all of these tests, combined with all of the given tests, we would start seeing failures if there were large issues surrounding the votes being miscounted or nodes winning elections without truly receiving a majority of votes. Another shortcoming was that we were not able to simulate a true network partition and instead relied on the provided pause() and resume() methods to try to mimic some of the behavior in a netowrk partition.

For replication log testing, beyond the provided tests, we also devised following situations:
1. Test if topic is shared when 2 leaders die in succession
2. Test that new topics can be added until there are < majority nodes left.
3. Test that follower cannot respond to client request, even when it has the entry in its state machine
4. Test that when majority of followers die, leader cannot read or write to state machine
5. Test if message (and topics) are shared when 2 leaders die in succession
6. Test if message (and topics) are shared when 2 leaders die in succession, and there's a gap in terms between when a message is set and when it is read

A shortcoming of our testing approach is that we did not explicitly test scenarios where a follower's log could have more entries than the leader's log, in which case the follower would need to delete any conflicting entries and roll back its logs to a point that were consistent with the leader's logs. Another shortcoming is the same shortcoming as in our election testing - we did not track log replication at the level of individual terms and individual append_entries requests. Overall, with our tests and the given replication tests, the behavior of the system looked to be as expected, with fail-stop tolerance.