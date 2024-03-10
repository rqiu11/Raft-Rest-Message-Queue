from flask import Flask, jsonify, request
import sys
import os
import json
from threading import Thread, Lock, Event
import time
import random
import requests
import logging


app = Flask(__name__)


# TOPICS (Represented as a list)
topics = []

# MESSAGE QUEUE
# Dict of form {topic_name: [list of messages in topic_name]}
# Use list to act as queue for each topic
# To get item from queue: message_queue.pop(0) (check first that list is not empty)
# To add item to quee: message_queue.append(item)
message_queue = {}

# Logs structure: {'op': operation detail, 'term': term when the leader received the entry, "topic": optional topic, "message": optional message}


leader_id = None
node_id = None
peers = []
state = "Follower"
votes_received = 0
# election_timeout = random.uniform(5, 10)  # For testing purposes, adjust as needed
election_timeout = random.uniform(0.3, 0.5)  # worked
# election_timeout = random.uniform(0.6, 0.1)  # worked
commit_index = 0
last_applied = 0
append_entries_successes = 0
append_entries_failures = 0


# Define a lock for thread-safe access to shared variables
append_entries_lock = Lock()
get_majority_of_heartbeats_lock = Lock()
votes_received_lock = Lock()
state_lock = Lock()
last_applied_lock = Lock()
save_persistent_variables_lock = Lock()
message_queue_lock = Lock()
topic_lock = Lock()
current_term_lock = Lock()
last_heartbeat_time_lock = Lock()
stop_heartbeat = False
next_index_lock = (
    Lock()
)  # Used for both next_index and match_index as they're often updated together
election_lock = Lock()
in_election = False
commit_index_lock = Lock()

logger_lock = Lock()


def debug_print(arg):
    logger_lock.acquire()
    logger.debug(f"{arg}")
    logger_lock.release()
    logger.setLevel(logging.DEBUG)


def election_timeout_checker():
    if len(peers) == 0:  # This expedites the process on startup for one-node systems.
        become_leader()
    global state, current_term, votes_received, last_heartbeat_time
    while True:
        # with last_heartbeat_time_lock:
        if (
            # If the state is a leader, it should not initiate an election.
            # If the state is a candidate, it's already in the initiate_election function, which
            # has its own time out mechanism for restarting an election.
            (state != "Leader" and state != "Candidate")
            and (not in_election)
            and (time.time() - last_heartbeat_time) > election_timeout
        ):
            debug_print(
                f"{time.time()}: Timed out. term {current_term}. Last heartbeat was {last_heartbeat_time}. My election timeout is {election_timeout}"
            )
            initiate_election()
            time.sleep(
                0.001
            )  # Very short sleep before the next iteration to ensure that some time passes befor echecking the current time again


def send_request_to_vote(peer, last_log_index, last_log_term):
    global current_term, node_id, votes_received, voted_for, state
    try:
        full_url = f"http://{peer}/request_vote"
        debug_print(f"{time.time()}: Sending vote request to {peer}")
        response = requests.post(
            full_url,
            json={
                "term": current_term,
                "candidateId": node_id,
                "last_log_index": last_log_index,
                "last_log_term": last_log_term,
            },
        )
        debug_print(
            f"{time.time()}: Received vote response: {current_term}, response from {peer}: {response.json()}"
        )
        if response.status_code == 200 and response.json().get("vote_granted"):
            debug_print(f"{time.time()}: Got a vote from: {peer}")
            with votes_received_lock:
                votes_received += 1
                debug_print(f"Total votes received: {votes_received}")
        elif not response.json().get("vote_granted"):
            if current_term < int(response.json()["term"]):
                with current_term_lock:
                    current_term = int(response.json()["term"])
                with state_lock:
                    debug_print("Became a follower during the elction.")
                    state = "Follower"

    except requests.exceptions.ConnectionError:
        debug_print(
            f"{time.time()}: Failed to get vote from {peer} due to connection error."
        )


def initiate_election():
    with election_lock:
        in_election = True
    global state, voted_for, current_term, votes_received, last_heartbeat_time
    debug_print(f"{time.time()}: Initiating election, old term {current_term}")
    last_log_index = len(logs) - 1
    if last_log_index >= 0:
        last_log_term = logs[last_log_index]["term"]
    else:
        last_log_term = -1
    with state_lock:
        state = "Candidate"
    with current_term_lock:
        current_term += 1
    if current_term in voted_for and voted_for[current_term] != node_id:
        debug_print(
            f"{time.time()}: Did not vote for myself in term {current_term} election (received request to vote from other node first)."
        )
        return
    voted_for[current_term] = node_id
    debug_print(
        f"{time.time()}: In election, term {current_term}. Voted_for {voted_for}."
    )
    votes_received = 1  # Votes for itself
    with last_heartbeat_time_lock:
        last_heartbeat_time = time.time()
    # Send vote requests to peers
    threads = []
    for peer in peers:
        t = Thread(
            target=send_request_to_vote,
            args=(
                peer,
                last_log_index,
                last_log_term,
            ),
        )
        threads.append(t)
        t.start()

    # Periodically check if a majority has been reached
    majority_of_peers = ((len(peers) + 1) // 2) + 1
    for i in range(1000):
        if state != "Candidate":
            debug_print(f"{time.time()} getting out of election because not candidate")
            with election_lock:
                in_election = False
            break
        if time.time() - last_heartbeat_time > election_timeout:
            debug_print(
                f"{time.time()} Election taking too long. Starting a new election"
            )
            time.sleep(random.uniform(0, 0.3))  # Explained in report
            initiate_election()
            break
        with votes_received_lock:
            debug_print(
                f"VOTES RECEIVED is {votes_received}, out of {majority_of_peers} needed. I have {peers} peers"
            )
            if votes_received >= majority_of_peers:
                debug_print(f"VOTES RECEIVED == {votes_received}")
                become_leader()
                with election_lock:
                    in_election = False
                break
        # Sleep for a short interval before checking again
        time.sleep(0.001)  # This interval can be adjusted


def send_append_entries_heartbeat_to_peer(peer):
    global stop_heartbeat_event
    while not stop_heartbeat_event.is_set():
        debug_print(f"Sent heartbeat to {peer} at time: {time.time()}")
        send_append_entries_heartbeat(peer)
        time.sleep(0.01)  # Adjust heartbeat frequency as needed


def send_append_entries_continuously():
    global stop_heartbeat_event
    debug_print(f"{time.time()} Sending heartbeats continuously.")
    threads = []
    for peer in peers:
        t = Thread(
            target=send_append_entries_heartbeat_to_peer,
            args=(peer,),
        )
        threads.append(t)
        t.start()

    stop_heartbeat_event.wait()  # Wait until the stop_heartbeat_event is set
    for t in threads:
        t.join()  # Join all threads once stop_heartbeat_event is set


def exchange_heartbeats_with_majority_of_cluster():
    global current_term, commit_index, node_id, logs, state
    global heartbeats_successes, heartbeats_failures
    heartbeats_successes = 0
    heartbeats_failures = 0
    if state == "Leader":
        if len(peers) == 0:
            return True
        threads = []
        with get_majority_of_heartbeats_lock:
            heartbeats_successes += 1  # Account for the leader itself
        threads = []
        for peer in peers:
            t = Thread(
                target=send_append_entries_heartbeat,
                args=(
                    peer,
                    True,
                ),
            )
            threads.append(t)
            t.start()

        # Periodically check if a majority has been reached
        majority_of_peers = ((len(peers) + 1) // 2) + 1
        for i in range(1000):
            with get_majority_of_heartbeats_lock:
                if heartbeats_successes >= majority_of_peers:
                    debug_print(
                        f"{time.time()} Exchanged heartbeats with majority of cluster. Read-only operation permitted"
                    )
                    return True
                elif heartbeats_failures >= majority_of_peers:
                    debug_print(
                        f"{time.time()} Did not exchange heartbeats successfully with majority of cluster. Read-only operation not permitted"
                    )
                    return False
            # Sleep for a short interval before checking again
            time.sleep(0.001)  # This interval can be adjusted


def become_leader():
    global state, leader_id, next_index, match_index
    global stop_heartbeat_event
    stop_heartbeat_event = Event()

    leader_id = node_id
    with state_lock:
        state = "Leader"
    debug_print(f"{time.time()}: Became leader, term {current_term}")

    heartbeat_thread = Thread(target=send_append_entries_continuously)
    heartbeat_thread.start()

    with next_index_lock:
        next_index = {peer: len(logs) for peer in peers}
        match_index = {peer: 0 for peer in peers}

    while state == "Leader":
        time.sleep(0.01)

    debug_print("Stopping heartbeat")
    stop_heartbeat_event.set()  # Set the event to signal threads to stop sending heartbeats
    heartbeat_thread.join()


def send_append_entries_to_one_peer(peer):
    global current_term, commit_index, node_id, logs, state, append_entries_successes, append_entries_failures, voted_for, next_index
    try:
        with next_index_lock:
            if next_index == {}:
                next_index = {peer: len(logs) for peer in peers}
            debug_print(f"{time.time()} Next_index dict: {next_index}")
            if len(logs) >= next_index[peer]:
                entries = logs[next_index[peer] :]
            else:
                debug_print(
                    f"{time.time()} No entries to send in append_entries request."
                )
            if next_index[peer] > 0:
                prev_log_index = next_index[peer] - 1
                prev_log_term = logs[next_index[peer] - 1]["term"]
            else:
                prev_log_term = -1
                prev_log_index = -1
        response = requests.post(
            f"http://{peer}/append_entries",
            json={
                "leader_id": node_id,
                "term": current_term,
                "prev_log_index": prev_log_index,
                "prev_log_term": prev_log_term,
                "entries": entries,
                "leader_commit": commit_index,
            },
        )
        debug_print(
            f"{time.time()} Sending append entries to peer {peer} {entries}. Next index is {next_index}"
        )
        response_json = response.json()
        if response_json["success"] == True:
            with append_entries_lock:
                append_entries_successes += 1
            with next_index_lock:
                next_index[peer] = len(logs)
                match_index[peer] = len(logs) - 1
            debug_print(
                f"{time.time()}: Successfully sent append entries request to follower {peer}"
            )
        elif int(response_json["term"]) > current_term:
            with current_term_lock:
                current_term = int(response_json["term"])
            # voted_for[current_term] = None
            with state_lock:
                debug_print(
                    f"{time.time()} Became follower by receiving higher term in append_entries request."
                )
                state = "Follower"
            with append_entries_lock:
                append_entries_failures += 1
        else:
            with next_index_lock:
                next_index[peer] = max(next_index[peer] - 1, 0)
            send_append_entries_to_one_peer(peer)

    except requests.exceptions.ConnectionError:
        with append_entries_lock:
            append_entries_failures += 1
        debug_print(
            f"{time.time()}: Cannot send append entries to {peer} due to connection error."
        )


def send_append_entries_heartbeat(peer, update_successful_heartbeats=False):
    global current_term, commit_index, node_id, logs, state, heartbeats_successes, heartbeats_failures, voted_for
    try:
        prev_log_index = len(logs) - 1
        if prev_log_index >= 0:
            prev_log_term = logs[prev_log_index]["term"]
        else:
            prev_log_term = -1
        response = requests.post(
            f"http://{peer}/append_entries",
            json={
                "leader_id": node_id,
                "term": current_term,
                "prev_log_index": prev_log_index,
                "prev_log_term": prev_log_term,
                "leader_commit": commit_index,
            },
        )
        response_json = response.json()
        if response_json["success"] == True:
            if update_successful_heartbeats:
                with get_majority_of_heartbeats_lock:
                    heartbeats_successes += 1
            debug_print(
                f"{time.time()}: Successfully sent heartbeat to follower {peer}"
            )
            return
        else:
            debug_print(f"Failed to send heartbeat to follower {peer}.")
            if int(response_json["term"]) > current_term:
                debug_print(
                    f"Failed to send heartbeat to follower {peer}: Term mismatch. My term is {current_term}. Their term is {response_json['term']}"
                )
                with current_term_lock:
                    current_term = int(response_json["term"])
                with state_lock:
                    state = "Follower"
            debug_print(f"{time.time()} Returning from unsuccessful heartbeat")
            if update_successful_heartbeats:
                with get_majority_of_heartbeats_lock:
                    heartbeats_failures += 1
            return
    except requests.exceptions.ConnectionError:
        debug_print(
            f"{time.time()}: Cannot send heartbeat to {peer} due to connection error."
        )
        if update_successful_heartbeats:
            with get_majority_of_heartbeats_lock:
                heartbeats_failures += 1


def send_append_entries(not_heartbeat=False):
    global current_term, commit_index, node_id, logs, state, append_entries_successes, append_entries_failures
    if state == "Leader":
        if len(peers) == 0:
            return True
        threads = []
        with append_entries_lock:
            append_entries_successes += 1  # Account for the leader
        for peer in peers:
            if not_heartbeat:
                t = Thread(
                    target=send_append_entries_to_one_peer,
                    args=(peer,),
                )
                threads.append(t)
                t.start()
            else:
                return False

        # Periodically check if a majority has been reached
        majority_of_peers = ((len(peers) + 1) // 2) + 1
        for i in range(1000):
            with append_entries_lock:
                debug_print(
                    f"Append entries successes {append_entries_successes}, failures {append_entries_failures}"
                )
                if append_entries_successes >= majority_of_peers:
                    return True
                elif append_entries_failures >= majority_of_peers:
                    return False
            # Sleep for a short interval before checking again
            time.sleep(0.01)  # This interval can be adjusted


# Flask routes for heartbeat and vote request handling are below
@app.route("/append_entries", methods=["POST"])
def handle_append_entries():
    global last_heartbeat_time, current_term, state, leader_id, commit_index, logs, voted_for
    with last_heartbeat_time_lock:
        last_heartbeat_time = time.time()
    data = request.json
    # Reply false if term < current term
    if data["term"] < current_term:
        debug_print(
            f"{time.time()} Got an append entries request from a node with a lower term than mine."
        )
        return jsonify({"success": False, "term": current_term})

    with state_lock:
        debug_print(
            f"{time.time()} Becoming follower in handle_append_entries function"
        )
        state = "Follower"
    with current_term_lock:
        current_term = int(data["term"])
    if not leader_id == data["leader_id"]:
        debug_print(
            f"{time.time()}: Became follower, term {current_term}. New leader is {data['leader_id']}"
        )
    leader_id = data["leader_id"]
    debug_print(f"{time.time()}: Listening to the leader {leader_id}")
    prev_log_index_of_leader = data["prev_log_index"]
    prev_log_term_of_leader = data["prev_log_term"]

    # Reply false if log doesn't contain and entry at prevLogIndex whose term matches prevLogTerm
    if (prev_log_index_of_leader >= 0 and prev_log_term_of_leader >= 0) and (
        prev_log_index_of_leader >= len(logs)
        or (logs[prev_log_index_of_leader]["term"] != prev_log_term_of_leader)
    ):
        debug_print(f"{time.time()}: MY LOGS: {logs}")
        return jsonify({"success": False, "term": current_term})

    # if not 'entries' in data:
    #     debug_print(f'received heartbeat from {leader_id}')

    # If existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it.
    if "entries" in data:
        conflict_idx = None
        for i in range(
            prev_log_index_of_leader + 1,
            min(prev_log_index_of_leader + len(data["entries"]) + 1, len(logs)),
        ):
            if (
                logs[i]["term"]
                != data["entries"][i - prev_log_index_of_leader - 1]["term"]
            ):
                conflict_idx = i
                break
        if conflict_idx is not None:
            logs = logs[:conflict_idx]
        # Append any new entries not already in the log
        for entry in data["entries"]:
            if entry not in logs:
                logs.append(entry)
    else:
        debug_print(f"{time.time()}. Got heartbeat from {data['leader_id']}")
    # If leader commit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
    if data["leader_commit"] > commit_index:
        with commit_index_lock:
            commit_index = min(data["leader_commit"], len(logs))
    debug_print(
        f"NEW LOGS: {logs}, commit_index: {commit_index}, last_applied: {last_applied}, leader_commit_index: {data['leader_commit']}"
    )
    with last_heartbeat_time_lock:
        last_heartbeat_time = time.time()
    return jsonify({"success": True, "term": current_term})


@app.route("/request_vote", methods=["POST"])
def listen_vote():
    global voted_for, current_term, last_heartbeat_time, state
    data = request.json

    candidate_logs_newer = False
    if logs == []:
        candidate_logs_newer = True
    elif data["last_log_index"] == 0:
        candidate_logs_newer = True
    elif int(data["last_log_term"]) > logs[-1]["term"]:
        candidate_logs_newer = True
    elif (int(data["last_log_term"]) == logs[-1]["term"]) and (
        data["last_log_index"] >= len(logs) - 1
    ):
        candidate_logs_newer = True

    if not candidate_logs_newer:
        debug_print(f"{time.time()}: Candidate logs not newer. My logs are {logs}.")

    voted_for_valid = (voted_for.get(int(data["term"]), None) is None) or voted_for.get(
        int(data["term"])
    ) == data["candidateId"]
    if not voted_for_valid:
        debug_print(
            f"{time.time()}: Already voted in this term {data['term']} for {voted_for}"
        )
        return jsonify({"vote_granted": False, "term": current_term})
    if data["term"] >= current_term and (voted_for_valid) and candidate_logs_newer:
        with state_lock:
            debug_print(
                f"{time.time()}: Becoming follower in request votes from {data['candidateId']}"
            )
            state = "Follower"
        voted_for[int(data["term"])] = data["candidateId"]
        current_term = data["term"]
        with last_heartbeat_time_lock:
            last_heartbeat_time = time.time()
        debug_print(f"{time.time()} voting in term {current_term}")
        return jsonify({"vote_granted": True, "term": current_term})
    elif data["term"] < current_term:
        debug_print(
            f"{time.time()}: Not voting yes to vote from {data['candidateId']} as term is lower"
        )
        return jsonify({"vote_granted": False, "term": current_term})
    debug_print(
        f"{time.time()}: Not voting for {data['candidateId']} for other reasons"
    )
    return jsonify({"vote_granted": False, "term": current_term})


def update_commit_index():
    global commit_index, logs, match_index, current_term, state
    while True:
        curr_state = "Follower"  # Set this in case global state isn't set yet
        with state_lock:
            curr_state = state
        if curr_state == "Leader":
            N = len(logs) - 1
            while N >= 0:
                # Check if N > commit_index, a majority of match_index[i] >= N, and log[N].term == currentTerm
                if N > commit_index and logs[N]["term"] == current_term:
                    with next_index_lock:
                        count = sum(
                            1 for peer_index in match_index.values() if peer_index >= N
                        )
                    majority = (len(peers) // 2) + 1
                    if count >= majority:
                        with commit_index_lock:
                            commit_index = N
                        debug_print(f"{time.time()} Updated commit_index to {N}")
                        break
                N -= 1
        time.sleep(0.01)


def apply_log_entries():
    global last_applied
    while True:
        if commit_index > last_applied:
            log_entry_to_apply = logs[last_applied]
            apply_log_entry_to_state_machine(log_entry_to_apply)
        time.sleep(0.001)


def apply_log_entry_to_state_machine(log_entry):
    global topics, message_queue, last_applied
    if log_entry["op"] == "Create topic":
        debug_print(f"{time.time()}: INCREMENTING LAST APPLIED {logs}, {last_applied}")
        with topic_lock:
            topics.append(log_entry["topic"])
        with message_queue_lock:
            message_queue[log_entry["topic"]] = []
        with last_applied_lock:
            last_applied += 1
    elif log_entry["op"] == "Adding message to topic":
        debug_print(f"{time.time()}: INCREMENTING LAST APPLIED {logs}, {last_applied}")
        with message_queue_lock:
            message_queue[log_entry["topic"]].append(log_entry["message"])
        with last_applied_lock:
            last_applied += 1
    elif log_entry["op"] == "Getting message from topic":
        debug_print(f"{time.time()}: INCREMENTING LAST APPLIED {logs}, {last_applied}")
        with message_queue_lock:
            message_queue[log_entry["topic"]].pop(0)
        with last_applied_lock:
            last_applied += 1
    elif log_entry["op"] == "No-op":
        with last_applied_lock:
            last_applied += 1


@app.route("/topic", methods=["PUT"])
def set_topic():
    global topics, message_queue, commit_index, state, append_entries_successes, append_entries_failures

    # Sleep for a very short time in case this method is called right as the program
    # is starting up, and there is no leader yet
    time.sleep(0.01)

    with state_lock:
        is_leader = state == "Leader"
    if is_leader:
        topic_dict = request.get_json()
        if "topic" not in topic_dict:
            return {"success": False}, 400
        elif not isinstance(topic_dict["topic"], str):
            return {"success": False}, 400
        elif topic_dict["topic"] in topics:
            return {"success": False}, 400
        else:
            topic = topic_dict["topic"]
            log_entry = {"op": "Create topic", "term": current_term, "topic": topic}
            debug_print(f"{time.time()} Trying to set topic: {log_entry}")
            logs.append(log_entry)
            index_of_log_added = len(logs)
            with append_entries_lock:
                append_entries_successes = 0
                append_entries_failures = 0
            if send_append_entries(not_heartbeat=True):
                with commit_index_lock:
                    commit_index += 1
                for (
                    i
                ) in range(  # In office hours we discussed that "while True" is not appropriate here since it can run infinitely
                    1000
                ):
                    if last_applied >= index_of_log_added:
                        return {"success": True}, 200
                    else:
                        time.sleep(0.01)
            else:
                return {"success": False}, 503
    else:
        debug_print(
            f"{time.time()} Request could not be processed as this node is not the leader."
        )
        return {
            "success": False
        }, 400  # Project specification says this should return with an error


@app.route("/topic", methods=["GET"])
def get_topic():
    global state
    # Sleep for a very short time in case this method is called right as the program
    # is starting up, and there is no leader yet
    time.sleep(0.01)
    with state_lock:
        is_leader = state == "Leader"
    if is_leader:
        if request.is_json:  # Should not call this endpoint with any arguments
            return {"success": False, "topics": []}, 400
        try:
            # Checked in office hours - this does not need to be put in the logs
            # as the state machine (message queue) is not affected
            if exchange_heartbeats_with_majority_of_cluster():
                return {"success": True, "topics": topics}, 200
            else:
                # Node is not the leader (has been deposed)
                return {"success": False}, 503
        except Exception as e:
            return {"success": False, "topics": []}, 404
    else:
        debug_print(
            f"{time.time()} Request could not be processed as this node is not the leader."
        )
        return {
            "success": False
        }, 400  # Project specification says this should return with an error


@app.route("/message", methods=["PUT"])
def put_message():
    global current_term, state, topics, message_queue, commit_index, state, append_entries_successes, append_entries_failures
    message_dict = request.get_json()
    # Sleep for a very short time in case this method is called right as the program
    # is starting up, and there is no leader yet
    time.sleep(0.01)
    with state_lock:
        is_leader = state == "Leader"
    if is_leader:
        if ("topic" not in message_dict) or ("message" not in message_dict):
            return {"success": False}, 400
        elif (not isinstance(message_dict["topic"], str)) or (
            not isinstance(message_dict["message"], str)
        ):
            return {"success": False}, 400
        elif (
            message_dict["topic"] not in topics
            or message_dict["topic"] not in message_queue
        ):
            return {"success": False}, 404
        else:
            message = message_dict["message"]
            topic = message_dict["topic"]
            log_entry = {
                "term": current_term,
                "op": f"Adding message to topic",
                "message": message,
                "topic": topic,
            }
            logs.append(log_entry)
            index_of_log_added = len(logs)
            with append_entries_lock:
                append_entries_successes = 0
                append_entries_failures = 0
            if send_append_entries(not_heartbeat=True):
                with commit_index_lock:
                    commit_index += 1
                for i in range(1000):
                    if last_applied >= index_of_log_added:
                        debug_print(
                            f"Last applied: {last_applied}, index of log added: {index_of_log_added}"
                        )
                        return {"success": True}, 200
                    else:
                        time.sleep(0.01)
            else:
                return {"success": False}, 503
    else:
        debug_print(
            f"{time.time()} Request could not be processed as this node is not the leader."
        )
        return {
            "success": False
        }, 400  # Project specification says this should return with an error


@app.route("/message/<topic>", methods=["GET"])
def get_message(topic):
    global current_term, state, topics, message_queue, logs, last_applied, commit_index, append_entries_successes, append_entries_failures
    # Sleep for a very short time in case this method is called right as the program
    # is starting up, and there is no leader yet
    time.sleep(0.01)
    with state_lock:
        is_leader = state == "Leader"
    if is_leader:
        if request.is_json:  # Should not call this endpoint with any arguments
            return {"success": False}, 400
        if topic not in topics or not message_queue[topic]:
            return {"success": False}, 404
        elif len(message_queue[topic]) == 0:
            return {"success": False}, 404
        else:
            message = message_queue[topic][0]
            log_entry = {
                "term": current_term,
                "op": "Getting message from topic",
                "topic": topic,
            }
            logs.append(log_entry)
            index_of_log_added = len(logs) - 1
            with append_entries_lock:
                append_entries_successes = 0
                append_entries_failures = 0
            if send_append_entries(not_heartbeat=True):
                with commit_index_lock:
                    commit_index += 1
                for i in range(1000):
                    if last_applied >= index_of_log_added:
                        # Has already exchanged append_entries requests with majority of followers,
                        # so we are safe to directly read from the message queue
                        return {
                            "success": True,
                            "message": message,
                        }, 200
                    else:
                        time.sleep(0.01)
            else:
                return {"success": False}, 503
    else:
        debug_print(
            f"{time.time()} Request could not be processed as this node is not the leader."
        )
        return {
            "success": False
        }, 400  # Project specification says this should return with an error


@app.route("/status", methods=["GET"])
def get_status():
    global state, current_term
    with state_lock:
        return {"role": state, "term": current_term}


def load_persistent_vars_from_file(filename):
    try:
        with save_persistent_variables_lock:
            with open(filename, "r") as file:
                data = json.load(file)
            data_current_term = int(data.get("current_term", 0))
            data_voted_for = data.get("voted_for", None)
            if not data_voted_for:
                data_voted_for = {}
            data_logs = list(data.get("logs", []))
            return data_current_term, data_voted_for, data_logs
    except Exception as e:
        return 0, {}, []


def save_persistant_vars_to_file(filename, current_term, voted_for, logs):
    data = {"current_term": current_term, "voted_for": voted_for, "logs": logs}
    with save_persistent_variables_lock:
        with open(filename, "w") as file:
            json.dump(data, file)


def save_persistant_vars_to_file_thread():
    while True:
        save_persistant_vars_to_file(filename, current_term, voted_for, logs)
        time.sleep(0.01)


def initialize_persistent_vars(filename):
    global current_term, voted_for, logs
    current_term, voted_for, logs = load_persistent_vars_from_file(filename)
    if not os.path.exists("nodes"):
        os.makedirs("nodes")

    # If the file doesn't exist, create a new one
    if not os.path.exists(filename):
        with current_term_lock:
            current_term = 0
        voted_for = {}
        logs = []
        save_persistant_vars_to_file(filename, current_term, voted_for, logs)
    else:
        current_term, voted_for, logs = load_persistent_vars_from_file(filename)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(
            "Error: arguments not specified correctly. Please run this node like: python3 src/node.py <path_to_config> <index>."
        )
        sys.exit(1)
    if not sys.argv[2].isdigit():
        print(
            "Error: arguments not specified correctly. Please run this node like: python3 src/node.py <path_to_config> <index>, with index as an int."
        )
        sys.exit(1)
    config_filename = sys.argv[1]
    config_index = int(sys.argv[2])

    if not os.path.exists(config_filename):
        print(
            "Error: arguments not specified correctly. Please run this node like: python3 src/node.py <path_to_config> <index>, <path_to_config> as a valid json file."
        )
        sys.exit(1)
    elif not config_filename.endswith("json"):
        print(
            "Error: arguments not specified correctly. Please run this node like: python3 src/node.py <path_to_config> <index>, <path_to_config> as a valid json file."
        )
        sys.exit(1)

    try:
        with open(config_filename, "r") as config:
            json_config = json.load(config)
            if not "addresses" in json_config:
                print(
                    "Error: make sure the json config file you have specified has a list of node addresses in it."
                )
                sys.exit(1)
            elif len(json_config["addresses"]) < (config_index + 1):
                print(
                    "Error: make sure the index you have specified is within the json config file's addresses."
                )
                sys.exit(1)
            elif (
                "ip" not in json_config["addresses"][config_index]
                or "port" not in json_config["addresses"][config_index]
            ):
                print(
                    "Error: make sure the json config entry at the index you have specified has an ip and a port entry."
                )
                sys.exit(1)
            else:
                ip = json_config["addresses"][config_index]["ip"].replace(
                    "http://", ""
                )  # Need to take off the http:// at the beginning of ip address for this to work
                port = json_config["addresses"][config_index]["port"]
                print(ip)
                print(port)
                node_id = str(ip) + ":" + str(port)
                peers = []

                for peer in json_config["addresses"]:
                    if (
                        peer["port"] != port
                        or peer["ip"] != json_config["addresses"][config_index]["ip"]
                    ):
                        ip = peer["ip"].replace("http://", "")
                        port_temp = peer["port"]
                        full_ip = str(ip) + ":" + str(port_temp)
                        peers.append(full_ip)
                print(peers)
                global filename
                global logger

                filename = f"nodes/{ip}_{port}.json"
                if not os.path.exists("logs"):
                    os.makedirs("logs")

                log_filename = f"logs/{ip}_{port}.log"
                open(log_filename, "a").close()
                logging.basicConfig(filename=log_filename, level=logging.DEBUG)

                logger = logging.getLogger("RaftLogger")
                global next_index
                next_index = {}
                global match_index
                match_index = {}

                debug_print(f"{time.time()}: getting data from {filename}")
                initialize_persistent_vars(filename)
                debug_print(
                    f"logs: {logs}, voted_for: {voted_for}, current_term: {current_term}"
                )
                global last_heartbeat_time
                with last_heartbeat_time_lock:
                    last_heartbeat_time = time.time()

                # Start the election timeout checker in a background thread
                # Should start the thread after finish initilizing
                Thread(target=election_timeout_checker, daemon=True).start()
                Thread(target=apply_log_entries, daemon=True).start()
                Thread(target=save_persistant_vars_to_file_thread, daemon=True).start()
                Thread(target=update_commit_index, daemon=True).start()

    except Exception as e:
        print(
            "Error retrieving entry from given json config file at the index you specified"
        )
        print(str(e))
        sys.exit(1)

    app.run(host=ip, port=port)
