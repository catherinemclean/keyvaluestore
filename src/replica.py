#!/usr/bin/env python

import decimal
import json
import random
import socket
import time

from src.constants import *


class Replica:
	def __init__(self, id, replica_ids):
		self.id = id
		self.leader_id = 'FFFF'
		self.replica_ids = replica_ids
		self.current_state = FOLLOWER   # FOLLOWER, CANDIDATE, OR LEADER
		self.commit_idx = 0     # index of highest log entry known to be committed (initialized to 0, increases monotonically)
		self.last_applied = 0   # index of highest log entry applied to state machine (initialized to 0, increases monotonically)
		self.state_machine = {} # actual key-value store
		self.votes = 0          # number of votes received if candidate
		
		# Setup socket
		self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
		self.sock.connect(id)

		# Persistent state on all servers (Updated on stable storage before responding to RPCs)
		self.current_term = 0   # latest term server has seen (initialized to 0 on first boot, increases monotonically)
		self.log = [(-1, -1)]   # log of transactions as tuples with the term and the command (as client msg); first index is 1
		self.voted_for = None   # who this replica voted for in this round
		
		# Leader specific vars (only used if leader)
		self.next_idx = {}      # for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
		self.match_idx = {}     # for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
		self.append_last = 0    # time when last append_entry RPC was sent
		
		# Timeout vars
		self.election_timeout = decimal.Decimal(random.randint(150, 300)) / 1000
		self.last = time.time() # time when last append_entry RPC was received from leader


	# Change state to candidate and request votes from other replicas
	def become_candidate(self):
		# reset from last election
		self.votes = 1
		self.leader_id = 'FFFF'
		self.voted_for = self.id
		# change our state to be a candidate
		self.current_state = CANDIDATE
		# increment term
		self.current_term += 1
		# if election timeout, declare yourself the leader
		self.request_vote()


	# Change state to leader and send out initial heartbeat to followers
	def become_leader(self):
		self.current_state = LEADER
		self.leader_id = self.id

		# reset/initialize nextIndex and matchIndex
		for id in self.replica_ids:
			self.next_idx[id] = len(self.log)  # initialized to leader's last log index + 1
			self.match_idx[id] = 0  # initialized to 0

		# send heartbeat
		self.send_append_ent()


	# Change state to follower if candidate and lost election or if leader and received vote request
	def become_follower(self):
		if self.current_state == LEADER:
			# reset leader-specific vars to defaults
			self.next_idx = {}
			self.match_idx = {}
			self.append_last = 0

		self.current_state = FOLLOWER


	# Increment votes and check if election won (assumed current state candidate)
	def tally_votes(self):
		self.votes += 1
		print '[%s] election results: votes = %s, total = %s' % (self.id, self.votes, len(self.replica_ids) + 1)
		if self.votes > (len(self.replica_ids) + 1) / 2:
			self.become_leader()


	# Send out vote requests to replicas after starting new election and declaring self as candidate
	def request_vote(self):
		last_log_idx = len(self.log) - 1
		last_log_term = self.log[len(self.log) - 1][1]
		raw_msg = {'src': self.id, 'dst': 'FFFF', 'leader': 'FFFF', 'type': VOTE_REQ, 'term': self.current_term,
		           'candidate_id': self.id, 'last_log_idx': last_log_idx, 'last_log_term': last_log_term}
		msg = json.dumps(raw_msg)
		if self.sock.send(msg):
			print '[%s] Sent out vote requests' % (self.id)
			self.last = time.time() # reset timeout clock


	# Determine if vote will be granted to candidate and send vote response back to candidate
	def handle_vote_request(self, msg):
		vote = (msg['term'] > self.current_term or
		        (msg['term'] == self.current_term and
		            msg['last_log_idx'] > len(self.log) - 1) or
		        (msg['term'] == self.current_term and
		            msg['last_log_idx'] == (len(self.log) - 1) and
		            msg['last_log_term'] >= self.log[len(self.log) - 1][1]))

		to_vote = vote and (self.voted_for is None or msg['term'] > self.current_term)
		if to_vote:
			self.voted_for = msg['candidate_id']
			self.current_term = msg['term']
			self.last = time.time()

		raw_msg = {'src': self.id, 'dst': msg['src'], 'leader': 'FFFF', 'type': VOTE_REPLY,
		           'term': self.current_term, 'vote': to_vote}
		reply = json.dumps(raw_msg)
		if self.sock.send(reply):
			v = 'Accepted' if self.voted_for else 'Denied'
			print '[%s] %s vote request from %s' % (self.id, v, msg['candidate_id'])


	# AppendEntry RPC receiver implementation
	def recv_append_ent(self, msg):
		# first append_entry_rpc sent out by new leader
		if msg['entries'] == [] and self.voted_for != None:
			print '[%s] New leader: %s' % (self.id, msg['leader'])
			if self.current_state == CANDIDATE:
				self.current_state = FOLLOWER
				
			self.voted_for = None
			self.votes = 0
			self.leader_id = msg['leader']
			self.current_term = msg['term']

		else:  # regular append_entry_rpc
			print '[%s] Received appendEntryRPC from leader %s' % (self.id, self.leader_id)
		# TODO: replicate log stuff

		# reset timeout clock
		self.last = time.time()

		# TODO: implement (for now, always just reply to leader with ok)
		assert(self.leader_id == msg['src'])
		raw = {'src': self.id, 'dst': self.leader_id, 'leader': self.leader_id, 'type': OK, 'term': self.current_term}
		ok = json.dumps(raw)
		if self.sock.send(ok):
			print '[%s] Sent OK to leader %s' % (self.id, self.leader_id)


	# respond to client with redirect message if not leader
	def redirect_client(self, msg):
		reply = {'src': self.id, 'dst': msg['src'], 'leader': self.leader_id, 'type': REDIRECT, 'MID': msg['MID']}
		json_reply = json.dumps(reply)
		if self.sock.send(json_reply):
			print '[%s] Redirect message MID(%s) to leader %s' % (self.id, msg['MID'], self.leader_id)


	# LEADER SPECIFIC METHODS
	
	# add client command to log and send appropriate entries to each replica
	# no msg means send heartbeat
	def send_append_ent(self, msg=None):
		self.append_last = time.time()
		self.last = self.append_last

		prev_log_idx = len(self.log) - 1  # index of log entry immediately preceding the new entry
		prev_log_term = self.log[prev_log_idx][0]

		if msg != None:  # new log entries
			self.log.append((self.current_term, msg))  # add client command to log

			for id in self.replica_ids:
				entries = self.log[self.next_idx[id]:]
				raw_msg = {'src': self.id, 'dst': id, 'leader': self.leader_id, 'type': APPEND_ENT,
				           'term': self.current_term, 'prev_log_idx': prev_log_idx,
				           'prev_log_term': prev_log_term, 'leader_commit': self.commit_idx,
				           'entries': entries}
				app_ent = json.dumps(raw_msg)
				if self.sock.send(app_ent):
					print '[%s] Sent append_entry_rpc to follower %s' % (self.id, id)

		else:  # new leader, initial heartbeat for log replication
			entries = []  # log entries for the replica to store
			raw_msg = {'src': self.id, 'dst': 'FFFF', 'leader': self.leader_id,
			           'type': APPEND_ENT, 'term': self.current_term,
			           'prev_log_idx': prev_log_idx, 'prev_log_term': prev_log_term,
			           'leader_commit': self.commit_idx, 'entries': entries}
			app_ent = json.dumps(raw_msg)
			if self.sock.send(app_ent):
				print '[%s] Sent heartbeat to all replicas' % self.id


	# handle receiving failed message from follower
	def handle_fail(self, msg):
		return 0


	# handle receiving ok message from follower
	def handle_ok(self, msg):
		return 0


	# check for quorum to update commit index and be able to respond to client
	def update_commit_idx(self):
		return 0


	# respond to client command (once quorum reached)
	# @param idx: the log index of the command being responded to
	def respond_to_client(self, idx):
		return 0

	# # Respond to a client's get message
	# def get_response(msg, sock):
	# 	global state_machine
	# 	global my_id
	# 	global leader_id
	# 	value = ""
	# 	if (msg['key'] in state_machine):
	# 		value = state_machine[msg['key']]
	# 	raw_msg = {'src': my_id, 'dst': msg['src'], 'leader': leader_id, 'type': 'ok', 'MID': msg['MID'],
	# 	           'value': value}
	# 	json_msg = json.dumps(raw_msg)
	# 	if sock.send(json_msg):
	# 		print
	# 		'[%s] get response sent to client' % (my_id)
	#
	# # Respond to a client's put message
	# def put_response(msg, sock):
	# 	global state_machine
	# 	global my_id
	# 	global leader_id
	# 	state_machine[msg['key']] = msg['value']
	# 	raw_msg = {'src': my_id, 'dst': msg['src'], 'leader': leader_id, 'type': 'ok', 'MID': msg['MID']}
	# 	json_msg = json.dumps(raw_msg)
	# 	if sock.send(json_msg):
	# 		print
	# 		'[%s] put response sent to client' % (my_id)
