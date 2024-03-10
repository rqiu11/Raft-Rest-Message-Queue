from test_utils import Swarm, Node, LEADER, FOLLOWER, CANDIDATE

import pytest
import time
import requests
import os
import shutil

# Cleans up the nodes and logs directories (used for persistent storage and logging).
# This isn't strictly necessary to get the tests to pass, but the nodes and logs
# directories end up having a lot of files, so this helps reduce those numbers.
if os.path.exists("nodes"):
    shutil.rmtree("nodes")
if os.path.exists("logs"):
    shutil.rmtree("logs")

PROGRAM_FILE_PATH = "src/node.py"
ELECTION_TIMEOUT = 2.0
NUM_NODES_ARRAY = [5]
TEST_TOPIC = "test_topic"
TEST_MESSAGE = "Test Message"
NUMBER_OF_LOOP_FOR_SEARCHING_LEADER = 3


# Same as the fixtures in message_queue_test.py and election_test.py, re-including this
# code here so our tests can fit the given framework.
@pytest.fixture
def node():
    node = Swarm(PROGRAM_FILE_PATH, 1)[0]
    node.start()
    time.sleep(ELECTION_TIMEOUT)
    yield node
    node.clean()


@pytest.fixture
def swarm(num_nodes):
    swarm = Swarm(PROGRAM_FILE_PATH, num_nodes)
    swarm.start(ELECTION_TIMEOUT)
    yield swarm
    swarm.clean()


# A few more methods that were not included in test_utils
def get_follower(swarm):
    for node in swarm.nodes:
        try:
            response = node.get_status()
            if response.ok and response.json()["role"] == FOLLOWER:
                return node
        except requests.exceptions.ConnectionError:
            continue
    time.sleep(0.5)
    return None


def get_follower_loop(swarm, times: int):
    for _ in range(times):
        leader = get_follower(swarm)
        if leader:
            return leader
    return None


###  MESSAGE QUEUE TESTING  ###


def test_set_topic_sequence(node):
    # Set a topic
    response = node.create_topic("Topic1")
    assert response.json() == {"success": True}
    assert response.status_code == 200

    # Can't set the sme topic twice
    response2 = node.create_topic("Topic1")
    assert response2.json() == {"success": False}
    assert response2.status_code == 400

    # Topic must be a string
    response3 = node.create_topic(1)
    assert response3.json() == {"success": False}
    assert response3.status_code == 400

    # Wrongly formatted request
    incorrectly_formatted_request = {"abc": "123"}
    response4 = requests.put(
        node.address + "/topic", json=incorrectly_formatted_request, timeout=1
    )
    assert response3.json() == {"success": False}
    assert response3.status_code == 400


def test_get_topic_sequence(node):
    # Check that when there are no topics, empty list of topics are returned
    response = node.get_topics()
    assert response.json() == {"success": True, "topics": []}
    assert response.status_code == 200

    # Add in a topic and check that it's in the list of topics
    response2 = node.create_topic("Topic1")
    assert response2.json() == {"success": True}
    assert response2.status_code == 200
    response3 = node.get_topics()
    assert response3.json() == {"success": True, "topics": ["Topic1"]}
    assert response3.status_code == 200

    # Add in another topic and check that it's in the list of topics
    response4 = node.create_topic("Topic2")
    assert response4.json() == {"success": True}
    assert response4.status_code == 200
    response5 = node.get_topics()
    assert response5.json() == {"success": True, "topics": ["Topic1", "Topic2"]}
    assert response5.status_code == 200

    # Incorrectly formatted request - should return False
    incorrectly_formatted_request = {"topic": "topic2"}
    response6 = requests.get(
        node.address + "/topic", json=incorrectly_formatted_request, timeout=1
    )
    assert response6.json() == {"success": False, "topics": []}
    assert response6.status_code == 400


def test_get_message_sequence(node):
    # Fails when topic does not already exist
    response = node.put_message("Topic1", "abc")
    assert response.json() == {"success": False}
    assert response.status_code == 404

    # Add in the topic, should now be able to put the message there
    response2 = node.create_topic("Topic1")
    assert response2.json() == {"success": True}
    assert response2.status_code == 200
    response3 = node.put_message("Topic1", "abc")
    assert response3.json() == {"success": True}
    assert response3.status_code == 200

    # Incorrectly formatted request (does not have message field)
    incorrectly_formatted_request = {"topic": "topic1"}
    response4 = requests.put(
        node.address + "/message", json=incorrectly_formatted_request, timeout=1
    )
    assert response4.json() == {"success": False}
    assert response4.status_code == 400

    # Incorrectly formatted request (does not have topic field)
    incorrectly_formatted_request2 = {"message": "abc"}
    response5 = requests.put(
        node.address + "/message", json=incorrectly_formatted_request2, timeout=1
    )
    assert response5.json() == {"success": False}
    assert response5.status_code == 400

    # Incorrectly formatted request (wrong data types)
    incorrectly_formatted_request3 = {"topic": 123, "message": "abc"}
    response6 = requests.put(
        node.address + "/message", json=incorrectly_formatted_request3, timeout=1
    )
    assert response6.json() == {"success": False}
    assert response6.status_code == 400

    # Topics list is not empty but specific topic is not in it
    response7 = node.put_message("topic2", "abc")
    assert response7.json() == {"success": False}
    assert response7.status_code == 404


def test_get_message_sequence(node):
    # Fails when topic does not already exist
    response = node.get_message("Topic1")
    assert response.json() == {"success": False}
    assert response.status_code == 404

    # Fails when topic exists but there are no messages in the topic
    response2 = node.create_topic("Topic1")
    assert response2.json() == {"success": True}
    assert response2.status_code == 200
    # Add another topic just to make sure these tests work with multiple topics in message queue and topics list
    response3 = node.create_topic("Topic2")
    assert response3.json() == {"success": True}
    assert response3.status_code == 200
    response4 = node.get_message("Topic1")
    assert response4.json() == {"success": False}
    assert response4.status_code == 404

    # Succeeds when there is a message in the topic
    response5 = node.put_message("Topic1", "abc")
    assert response5.json() == {"success": True}
    assert response5.status_code == 200
    response6 = node.get_message("Topic1")
    assert response6.status_code == 200
    assert response6.json() == {"success": True, "message": "abc"}

    # Succeeds and returns first entered message when there are multiple messages in the topic
    # Put multiple messages in the topic
    response7 = node.put_message("Topic1", "def")
    assert response7.json() == {"success": True}
    assert response7.status_code == 200
    response8 = node.put_message("Topic1", "ghi")
    assert response8.json() == {"success": True}
    assert response8.status_code == 200
    response9 = node.put_message("Topic1", "jkl")
    assert response9.json() == {"success": True}
    assert response9.status_code == 200
    # Get first message from the topic
    response10 = node.get_message("Topic1")
    assert response10.status_code == 200
    assert response10.json() == {"success": True, "message": "def"}

    # Get other two messages in Topic1 so the message queue becomes empty
    response11 = node.get_message("Topic1")
    assert response11.status_code == 200
    assert response11.json() == {"success": True, "message": "ghi"}
    response12 = node.get_message("Topic1")
    assert response12.status_code == 200
    assert response12.json() == {"success": True, "message": "jkl"}

    # Now message queue for that topic is empty, fails to get another message
    response13 = node.get_message("Topic1")
    assert response13.status_code == 404
    assert response13.json() == {"success": False}

    # Fails if the request has extra information in it
    incorrectly_formatted_request = {"topic": "Topic1"}
    response14 = requests.get(
        node.address + "/message" + "/" + "Topic1",
        json=incorrectly_formatted_request,
        timeout=1,
    )
    assert response14.status_code == 400
    assert response14.json() == {"success": False}


###  ELECTION TESTING ###


# Test majority needed to vote for leader
# - 2-node case: 2 nodes (candidate and the other node) are both required to vote to elect a leader
# - 3-node case: 2 nodes (candidate and at least one other node) are required to vote to elect a leader
@pytest.mark.parametrize("num_nodes", [2, 3])
def test_majority(swarm: Swarm, num_nodes: int):
    leader = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader != None


# Time-outs from two nodes in close succession and current leader crashes
# - One node times out and sends out a RequestToVote, and quickly after that, another node times out and sends a RequestToVote
# - The first node that timed out gets a majority of votes back before the second one does, so the first node that timed out becomes leader, and the second candidate node gets a heartbeat from the new leader and becomes a follower
@pytest.mark.parametrize("num_nodes", [5])
def test_k2(swarm: Swarm, num_nodes: int):
    leader1 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader1 != None
    leader1.clean(ELECTION_TIMEOUT)
    leader2 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    leader2.clean(ELECTION_TIMEOUT)
    leader3 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    leader1.restart()
    leader2.restart()
    assert leader3 != None
    assert leader3 != leader1
    assert leader3 != leader2


# When 2/3 nodes die, the third node can't become a leader since it won't get a majority
# of votes. Then, when they come alive, there will be a leader. (Try this with
# 3 and 4 nodes total, respectively, as they should have the same behavior)
@pytest.mark.parametrize("num_nodes", [3, 4])
def test_election_after_restart(swarm: Swarm, num_nodes: int):
    leader1 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader1 != None
    leader1.clean(ELECTION_TIMEOUT)
    leader2 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader2 != None
    leader2.clean(ELECTION_TIMEOUT)
    leader3 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader3 == None
    leader1.start()
    leader2.start()
    time.sleep(ELECTION_TIMEOUT)
    leader4 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader4 != None


# First there is a leader, then > 1/2 the nodes (including the leader), so a new
# leader is not elected
@pytest.mark.parametrize("num_nodes", [5])
def test_none_elected(swarm: Swarm, num_nodes: int):
    leader1 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader1 != None
    follower1 = get_follower_loop(swarm, 3)
    follower1.clean(ELECTION_TIMEOUT)
    follower2 = get_follower_loop(swarm, 3)
    follower2.clean(ELECTION_TIMEOUT)
    leader1.clean(ELECTION_TIMEOUT)
    leader2 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader2 == None
    # Check that after a short timeout, there is still no other leader elected
    time.sleep(1.0)
    assert leader2 == None


# First there is a leader, then the leader is killed, but as
# there are only 2 nodes, a majority cannot be reached so a new leader is not elected.
# but the one node that is left keeps trying to get elected and is therefore a candidate.
@pytest.mark.parametrize("num_nodes", [2])
def test_none_elected_two_node_case(swarm: Swarm, num_nodes: int):
    leader1 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader1 != None
    follower1 = get_follower_loop(swarm, 3)
    assert leader1 != follower1
    leader1.clean(ELECTION_TIMEOUT)
    leader2 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader2 == None
    # Check that after a short timeout, there is still no other leader elected
    time.sleep(1.0)
    assert leader2 == None
    assert follower1.get_status().json()["role"] != LEADER


# Trivial case. There's only 1 node and it gets killed so there is no leader.
@pytest.mark.parametrize("num_nodes", [1])
def test_no_nodes_left(swarm: Swarm, num_nodes: int):
    leader1 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    leader1.clean(ELECTION_TIMEOUT)
    leader2 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader2 == None


# Check that if there's a partition, an elected leader will still be leader
@pytest.mark.parametrize("num_nodes", [5])
def test_partition(swarm: Swarm, num_nodes: int):
    leader1 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    leader_index = swarm.nodes.index(leader1)

    # Pause all non-leader nodes
    for i in range(swarm.num_nodes):
        if i != leader_index:
            swarm.nodes[i].pause()

    # Check that original leader is still the leader
    time.sleep(ELECTION_TIMEOUT)
    assert leader1.get_status().json()["role"] == LEADER

    # Check that original leader is still the leader
    time.sleep(ELECTION_TIMEOUT)
    assert leader1.get_status().json()["role"] == LEADER

    # Resume paused nodes
    for i in range(swarm.num_nodes):
        if swarm.nodes[i] != leader1:
            swarm.nodes[i].resume()

    time.sleep(ELECTION_TIMEOUT)
    leader2 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader2 != None


### REPLICATION TESTING ###


# Test if topic is shared when 2 leaders die in succession (reaches max number
# of node failures in the 5 node case, also tests in the 6, 7, and 8 node cases)
@pytest.mark.parametrize("num_nodes", [5, 6, 7, 8])
def test_is_topic_shared_with_3_leaders(swarm: Swarm, num_nodes: int):
    leader1 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)

    assert leader1 != None
    assert leader1.create_topic(TEST_TOPIC).json() == {"success": True}

    leader1.commit_clean(ELECTION_TIMEOUT)
    leader2 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)

    assert leader2 != None
    assert leader2.get_topics().json() == {"success": True, "topics": [TEST_TOPIC]}

    leader2.commit_clean(ELECTION_TIMEOUT)
    leader3 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)

    assert leader3 != None
    assert leader3.get_topics().json() == {"success": True, "topics": [TEST_TOPIC]}


# Test that new topics can be added until there are < majority nodes left.
@pytest.mark.parametrize("num_nodes", [5])
def test_topic_not_shared_when_majority_dies(swarm: Swarm, num_nodes: int):
    leader1 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)

    assert leader1 != None
    assert leader1.create_topic("topic1").json() == {"success": True}

    leader1.commit_clean(ELECTION_TIMEOUT)
    leader2 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)

    assert leader2 != None
    # Leader 2 can create a new topic, also can get the topic from lader 1
    assert leader2.create_topic("topic2").json() == {"success": True}
    assert leader2.get_topics().json() == {
        "success": True,
        "topics": ["topic1", "topic2"],
    }

    leader2.commit_clean(ELECTION_TIMEOUT)
    leader3 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)

    assert leader3 != None
    assert leader3.get_topics().json() == {
        "success": True,
        "topics": ["topic1", "topic2"],
    }
    assert leader3.create_topic("topic3").json() == {"success": True}

    leader3.commit_clean(ELECTION_TIMEOUT)
    leader4 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    # Cant elect a new leader without majority vote
    assert leader4 == None


# Test that follower cannot respond to client request, even when it has the entry in its state machine
@pytest.mark.parametrize("num_nodes", [2])
def test_follower_doesnt_respond_to_requests(swarm: Swarm, num_nodes: int):
    leader1 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader1 != None
    assert leader1.create_topic("topic1").json() == {"success": True}
    assert leader1.get_topics().json() == {"success": True, "topics": ["topic1"]}
    follower1 = get_follower_loop(swarm, 3)
    assert follower1.get_topics().json() == {"success": False}
    assert follower1.create_topic("topic1").json() == {"success": False}

    # Even when the leader is removed, the other node can't win an election and can't become
    # leader so it still should not respond to client requests
    leader1.clean()
    leader2 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader2 == None
    get_topics_response = follower1.get_topics()
    assert get_topics_response.json() == {"success": False}
    assert get_topics_response.status_code == 400
    create_topic_response = follower1.create_topic("topic1")
    assert create_topic_response.json() == {"success": False}
    assert create_topic_response.status_code == 400
    put_message_response = follower1.put_message("topic1", "jkl")
    assert put_message_response.json() == {"success": False}
    assert put_message_response.status_code == 400


# Test that when majority of followers die, leader cannot read or write to state machine
@pytest.mark.parametrize("num_nodes", [3])
def test_leader_cant_commit_to_logs_without_majority(swarm: Swarm, num_nodes: int):
    leader1 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    leader_index = swarm.nodes.index(leader1)

    # Leader can commit and read topics before all followers die
    assert leader1.create_topic("topic1").json() == {"success": True}
    assert leader1.get_topics().json() == {"success": True, "topics": ["topic1"]}

    # Pause all non-leader nodes
    for i in range(swarm.num_nodes):
        if i != leader_index:
            swarm.nodes[i].clean()

    get_topics_response = leader1.get_topics()
    assert get_topics_response.json() == {"success": False}
    assert get_topics_response.status_code == 503
    create_topic_response = leader1.create_topic("topic2")
    assert create_topic_response.json() == {"success": False}
    assert create_topic_response.status_code == 503
    put_message_response = leader1.put_message("topic1", "jkl")
    assert put_message_response.json() == {"success": False}
    assert put_message_response.status_code == 503


# Test if message (and topics) are shared when 2 leaders die in succession (reaches max number
# of node failures in the 5 node case, also tests in the 6, 7, and 8 node cases)
@pytest.mark.parametrize("num_nodes", [5, 6, 7, 8])
def test_is_message_shared_with_3_nodes(swarm: Swarm, num_nodes: int):
    leader1 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)

    assert leader1 != None
    # First leader puts topic and message in the queue
    assert leader1.create_topic(TEST_TOPIC).json() == {"success": True}
    assert leader1.put_message(TEST_TOPIC, TEST_MESSAGE).json() == {"success": True}
    assert leader1.create_topic("test2").json() == {"success": True}

    # First leader dies
    leader1.commit_clean(ELECTION_TIMEOUT)
    leader2 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)

    assert leader2 != None
    # Second leader gets message from first leader's term
    assert leader2.get_message(TEST_TOPIC).json() == {
        "success": True,
        "message": TEST_MESSAGE,
    }
    # Second leader puts topic and message in the queue
    assert leader2.create_topic("abc").json() == {"success": True}
    assert leader2.put_message("abc", "1234").json() == {"success": True}

    # Second leader dies
    leader2.commit_clean(ELECTION_TIMEOUT)
    leader3 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)

    assert leader3 != None
    # Third leader gets message from first and second leader's terms
    assert leader3.get_message(TEST_TOPIC).json() == {
        "success": False,  # Message was already popped from queue
    }
    assert leader3.get_message("abc").json() == {
        "success": True,
        "message": "1234",
    }
    # Third leader puts message into topic that was added during first leader's term
    assert leader3.put_message("test2", "1234").json() == {"success": True}


# Test if message (and topics) are shared when 2 leaders die in succession, and
# there's a gap in terms between when a message is set and when it is read
@pytest.mark.parametrize("num_nodes", [5])
def test_is_message_shared_with_leader_gap(swarm: Swarm, num_nodes: int):
    leader1 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)

    assert leader1 != None
    # First leader puts topic and message in the queue
    assert leader1.create_topic(TEST_TOPIC).json() == {"success": True}
    assert leader1.put_message(TEST_TOPIC, TEST_MESSAGE).json() == {"success": True}

    # First leader dies
    leader1.commit_clean(ELECTION_TIMEOUT)
    leader2 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader2 != None

    leader2.commit_clean(ELECTION_TIMEOUT)
    leader3 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader3 != None

    # Third leader gets message from first leader's term
    assert leader3.get_message(TEST_TOPIC).json() == {
        "success": True,
        "message": TEST_MESSAGE,
    }
