#!/usr/bin/env python

import decimal
import json
import random
import socket
import time
#from statistics import median, median_low
from numpy import median
from constants import *


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
		self.msgs_to_redirect = []

		# Setup socket
		self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
		self.sock.connect(id)

		# Persistent state on all servers (Updated on stable storage before responding to RPCs)
		self.current_term = 0   # latest term server has seen (initialized to 0 on first boot, increases monotonically)
		self.log = [(-1, {})]   # log of transactions as tuples with the term and the command (as client msg); first index is 1
		self.voted_for = None   # who this replica voted for in this round
		
		# Leader specific vars (only used if leader)
		# (next_idx and match_idx do not include self)
		self.next_idx = {}      # for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
		self.match_idx = {}     # for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
		self.append_last = 0    # time when last append_entry RPC was sent
		self.last_heard_from = {} # for each server, time when last response was received

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
		for rid in self.replica_ids:
			self.next_idx[rid] = len(self.log)  # initialized to leader's last log index + 1
			self.match_idx[rid] = 0  # initialized to 0
			self.last_heard_from[rid] = 0 #time.time()

		# send heartbeat
		#self.send_append_ent()

		if self.msgs_to_redirect != []:
			for m in self.msgs_to_redirect:
				# TODO: check for duplicates?
				self.send_append_ent(m)
			self.msgs_to_redirect = []

		# send heartbeat
                self.send_append_ent()

	# Change state to follower if candidate and lost election or if leader and received vote request
	# Assumes term has already been updated (and that is what triggered method call)
	def become_follower(self, term):
		# reset leader-specific vars to defaults
		if self.current_state == LEADER:
			self.next_idx = {}
			self.match_idx = {}
			self.append_last = 0
			self.last_heard_from = {}
			self.voted_for = None
			print "LEADER %s becoming a follower" % (self.id)

		# reset voting vars
		if self.current_state == CANDIDATE:
			self.votes = 0
			self.voted_for = None

		# TODO: must reset leader_id after wherever this is called
		self.current_state = FOLLOWER
		self.current_term = term


	# Increment votes and check if election won
	def tally_votes(self):
		self.votes += 1
		print '[%s] election results: votes = %s, total = %s' % (self.id, self.votes, len(self.replica_ids) + 1)
		if self.votes > (len(self.replica_ids) + 1) / 2:
			self.become_leader()


	# Send out vote requests to replicas after starting new election and declaring self as candidate
	def request_vote(self):
		self.leader_id = 'FFFF'
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
		term = msg['term']

		# send FAIL instead of VOTE_REPLY if candidate has lower term
		if self.current_state == LEADER or term < self.current_term:
			# set leader_id to FFFF if not the leader
			self.leader_id = self.leader_id if self.current_state == LEADER else 'FFFF'
			raw_msg = {'src': self.id, 'dst': msg['src'], 'leader': leader, 'type': FAIL,
					   'term': self.current_term}
			reply = json.dumps(raw_msg)
			if self.sock.send(reply):
				print '[%s] REJECTED/FAILED VOTE REQUEST from %s' % (self.id, msg['src'])
		
		else:
			if self.current_state == CANDIDATE and term > self.current_term:
				# become follower before voting
				print 'Candidate %s becoming follower in handle_vote_request' % self.id
				self.become_follower(term)
			
			# change leader to FFFF (meaning no current leader)
			self.leader_id = 'FFFF'
	
			# grant vote if candidate has >= term and longer log, 
			# or >= term and same length log but last entry has > term than my last entry's term


			vote = ((term >= self.current_term and msg['last_log_idx'] > len(self.log) - 1) or (term >= self.current_term and msg['last_log_idx'] == (len(self.log) - 1) and msg['last_log_term'] >= self.log[len(self.log) - 1][0]))
			# vote = msg['last_log_idx'] > len(self.log) - 1 or (msg['last_log_idx'] == (len(self.log) - 1) and msg['last_log_term'] >= self.log[len(self.log) - 1][0])

			# make sure that replica hasn't already voted this round (based on term and self.voted_for)
			to_vote = vote and (self.voted_for is None or term > self.current_term)
			
			if to_vote:				
				self.voted_for = msg['candidate_id']
				# make sure term was updated
				self.current_term = term
				self.last = time.time()

			raw_msg = {'src': self.id, 'dst': msg['src'], 'leader': 'FFFF', 'type': VOTE_REPLY,
					   'term': self.current_term, 'vote': to_vote}
			reply = json.dumps(raw_msg)
			if self.sock.send(reply):
				v = 'Accepted' if to_vote else 'Denied'
				print '[%s] %s vote request from %s' % (self.id, v, msg['candidate_id'])


	# apply all log entry commands up to leader_commit to the replica's state machine
	def apply_committed(self):
		while self.last_applied < self.commit_idx:
			cmd = self.log[self.last_applied+1][1]
			if cmd['type'] == PUT:
				self.state_machine[cmd['key']] = cmd['value']
			self.last_applied += 1

	# Receiver implementation of AppendEntriesRPC
	# Replica's current state must be FOLLOWER
	def recv_append_ent(self, msg):
		assert(self.current_state == FOLLOWER)
		term = msg['term']

		# all replicas reject RPC if term < current_term, regardless of its current_state
		if term < self.current_term:
			# reject RPC
			raw = {'src': self.id, 'dst': msg['src'], 'leader': self.leader_id,
			       'type': FAIL, 'term': self.current_term, 'last_log_idx': len(self.log)-1}
			reply = json.dumps(raw)
			if self.sock.send(reply):
				print '[%s] Rejected AppendEntryRPC from %s\n\n' % (self.id, msg['src'])

			return

		# update term
		self.current_term = term

		# heartbeat
		if msg['entries'] == []:

			# redirect any queued messages received during election
			if self.msgs_to_redirect != []:
				for m in self.msgs_to_redirect:
					self.redirect_client(m)
				self.msgs_to_redirect = []

			# received initial heartbeat from new leader
			if self.voted_for != None:
				print '[%s] New leader: %s' % (self.id, msg['leader'])

				# reset voting info
				self.voted_for = None
				self.votes = 0
				# set leader_id
				self.leader_id = msg['leader']
				# reset timeout
				self.last = time.time()

			# regular heartbeat from established leader
			else:
				if self.leader_id != msg['leader']:
					print '[%s] Received heartbeat from %s but leader = %s' % (self.id, msg['leader'], self.leader_id)
					self.leader_id = msg['leader']

				# reset timeout
				self.last = time.time()


		# regular append entries
		else:
			if self.leader_id == 'FFFF':
				self.leader_id = msg['leader']
				print '[%s] Received entries from %s but leader still set to FFFF' % (self.id, msg['src'])

			prev_log_idx = msg['prev_log_idx']

			# prev_log_idx throws index error
			if prev_log_idx > len(self.log) - 1:
				reply_type = FAIL

			# entry at prev_log_idx doesn't match leader's entry
			elif self.log[prev_log_idx][0] != msg['prev_log_term']:
				# remove incorrect entry at prev_log_idx and any entries after
				self.log = self.log[:prev_log_idx]
				reply_type = FAIL

			# logs match
			else:
				# only keep entries up to what is matched with leader
				self.log = self.log[:prev_log_idx + 1]
				# append entries not already in log
				for entry in msg['entries']:
					e = tuple(entry)
					if e not in self.log:
						self.log.append(e)

				# update commit_idx
				if msg['leader_commit'] > self.commit_idx:
					# set to min of leader's commit_idx and index of last new log entry
					self.commit_idx = min(msg['leader_commit'], len(self.log)-1)
					# apply newly committed entries to state machine
					self.apply_committed()

				reply_type = OK

			# send response to leader (no reply to heartbeats)
			raw = {'src': self.id, 'dst': self.leader_id, 'leader': self.leader_id,
			       'type': reply_type, 'term': self.current_term, 'last_log_idx': len(self.log) - 1}
			reply = json.dumps(raw)
			self.sock.send(reply)

			# reset timeout clock
			self.last = time.time()


	# TODO: make msgs_to_redirect dict to prevent duplicates more easily?
	# respond to client with redirect message if not leader
	def redirect_client(self, msg):
		if self.leader_id == 'FFFF':
			self.msgs_to_redirect.append(msg)
			return
		
		if self.leader_id == self.id:
			print '[%s] TRYING TO REDIRECT TO SELF' % self.id
			exit(1)		

		reply = {'src': self.id, 'dst': msg['src'], 'leader': self.leader_id, 'type': REDIRECT, 'MID': msg['MID']}
		json_reply = json.dumps(reply)
		if self.sock.send(json_reply):
			print '[%s] Redirect message MID(%s) to leader %s' % (self.id, msg['MID'], self.leader_id)


	# LEADER SPECIFIC METHODS

	# check if have received response from at least half of followers within past .35 s
	def check_stale_followers(self):
		stale = 0
		now = time.time()
		for t in self.last_heard_from.values():
			if now - t >= .35:
				stale+=1

		print '[%s] STALE followers: %s/%s' % (self.id, stale, (len(self.replica_ids)//2))
		return stale > (len(self.replica_ids)//2)
	
	# add client command to log and send appropriate entries to each replica
	# no msg means send heartbeat
	def send_append_ent(self, msg=None):
		if self.leader_id != self.id:
			print '[%s] WUT TRYING TO SEND_APPEND_ENT AS LEADER BUT LEADER_ID = %s' % (self.id, self.leader_id)

		# update timeouts
		self.append_last = time.time()

		# send heartbeat
		if msg == None:
			raw_msg = {'src': self.id, 'dst': 'FFFF', 'leader': self.leader_id,
					   'type': APPEND_ENT, 'term': self.current_term,
					   'prev_log_idx': len(self.log)-1, 'prev_log_term': self.log[len(self.log)-1][0],
					   'leader_commit': self.commit_idx, 'entries': []}
			app_ent = json.dumps(raw_msg)
			if self.sock.send(app_ent):
				print '[%s] Term %s: Sent heartbeat to all replicas' % (self.id, self.current_term)

		# send entries
		else:
			all_entries = [entry[1] for entry in self.log]

			# old/duplicate client request
			if msg in all_entries:
				msg_idx = all_entries.index(msg)

				# already applied to state machine and sent response to client (may not have received response?)
				if msg_idx <= self.last_applied:
					# resend response
					if msg['type'] == PUT:
						raw_msg = {'src': self.id, 'dst': msg['src'], 'leader': self.leader_id, 'type': 'ok',
								   'MID': msg['MID']}
					else:
						value = self.state_machine.get(self.state_machine[msg['key']], '')
						raw_msg = {'src': self.id, 'dst': msg['src'], 'leader': self.leader_id, 'type': 'ok',
								   'MID': msg['MID'], 'value': value}

					json_msg = json.dumps(raw_msg)
					if self.sock.send(json_msg):
						print '[%s] Resent response to client %s request at idx = %s' % (self.id, msg['type'], msg_idx)

			# new client request
			else:
				# add client command to log
				self.log.append((self.current_term, msg))


			# send appropriate entries to all followers
			for rid in self.replica_ids:
				prev_log_idx = self.next_idx[rid] - 1
				prev_log_term = self.log[prev_log_idx][0]

				# send maximum of 50 entries at once
				num_entries = min(len(self.log), self.next_idx[rid]+51)
				entries = self.log[self.next_idx[rid]:num_entries]

				raw_msg = {'src': self.id, 'dst': rid, 'leader': self.leader_id, 'type': APPEND_ENT,
						   'term': self.current_term, 'prev_log_idx': prev_log_idx,
						   'prev_log_term': prev_log_term, 'leader_commit': self.commit_idx,
						   'entries': entries}
				app_ent = json.dumps(raw_msg)
				self.sock.send(app_ent)


	# handle receiving failed message from follower
	def handle_fail(self, msg):
		term = msg['term']

		# step down from leader if receive response with higher term
		if term > self.current_term and time.time():
			# reset leader_id to prepare for election
			print '[%s] Becoming follower because %s had better term' % (self.id, msg['src'])
			self.become_follower(term)
			self.leader_id = 'FFFF'
			return

		follower_id = msg['src']
		# update next_idx to be min next_idx-1 or replica's last_log_idx +1
		self.next_idx[follower_id] = min(self.next_idx[follower_id]-1, msg['last_log_idx']+1)
		follower_next = self.next_idx[follower_id]
		# send max of 50 entries at once
		num_entries = min(len(self.log), follower_next+51)

		entries = self.log[follower_next:num_entries]
		raw_msg = {'src': self.id, 'dst': follower_id, 'leader': self.leader_id, 'type': APPEND_ENT,
		           'term': self.current_term, 'prev_log_idx': follower_next - 1,
		           'prev_log_term': self.log[follower_next - 1][0], 'leader_commit': self.commit_idx,
		           'entries': entries}
		app_ent = json.dumps(raw_msg)
		# if len(app_ent) > 20000:
		# 	print "FAIL: ASSUME FAILURE of %s" % (follower_id)
		if self.sock.send(app_ent):
			print '[%s] HANDLED FAIL: Sent append_entry rpc to %s' % (self.id, follower_id)

		self.last_heard_from[follower_id] = time.time()


	# handle receiving ok message from follower
	def handle_ok(self, msg):
		# update follower's next_idx and match_idx
		follower_id = msg['src']
		self.next_idx[follower_id] = msg['last_log_idx'] + 1
		self.match_idx[follower_id] = msg['last_log_idx']

		if msg['last_log_idx'] >= len(self.log):
			print 'NEVER: FOLLOWERS LOG LONGER THAN LEADER LOG (lastlogidx=%s >= %s)' % (msg['last_log_idx'], len(self.log))

		# if len(self.log) - self.match_idx[follower_id] > 25:
		# replica's log still doesn't have all committed entries
		if self.commit_idx > self.match_idx[follower_id]:
			# log replication (replica many log entries behind leader)
			follower_next = self.next_idx[follower_id]
			num_entries = min(len(self.log), follower_next + 51)

			entries = self.log[follower_next:num_entries]
			raw_msg = {'src': self.id, 'dst': follower_id, 'leader': self.leader_id, 'type': APPEND_ENT,
			           'term': self.current_term, 'prev_log_idx': follower_next - 1,
			           'prev_log_term': self.log[follower_next - 1][0], 'leader_commit': self.commit_idx,
			           'entries': entries}
			app_ent = json.dumps(raw_msg)
			if self.sock.send(app_ent):
				print '[%s] SENT MORE ENTRIES FOR LOG REPLICATION TO %s' % (self.id, follower_id)

		self.last_heard_from[follower_id] = time.time()
		# check for quorum of new entries
		self.update_commit_idx()

	# higher median value
	def median_high(self, values):
		n = len(values)
		return sorted(values)[n//2]

	# check for quorum to update commit index and be able to respond to client
	def update_commit_idx(self):
		# median if even number total replicas b/c match_idx will be odd (doesn't include self)
		# median_low if odd number total replicas b/c match_idx will be even
		# (ex: [1, 3, 5, 7] leader already agrees, so only need 2/4 for quorum)

		if len(self.replica_ids)%2 == 0: # odd number of total replicas
			# the highest log entry index replicated on majority of servers
			highest_quorum_idx = self.median_high(self.match_idx.values())
		else:
			highest_quorum_idx = median(self.match_idx.values())

		# new log entry has been committed
		while self.commit_idx < highest_quorum_idx:
			self.commit_idx += 1
			self.respond_to_client(self.commit_idx)


	# apply command to state machine and send response to client
	# @param idx: the log index of the command being responded to
	def respond_to_client(self, idx):
		print 'respond_to_client(idx=%s)' % idx
		msg = self.log[idx][1]
		if (msg['type'] == GET):
			self.get_response(msg)
		elif (msg['type'] == PUT):
			self.put_response(msg)

	# Respond to a client's get message
	def get_response(self, msg):
		value = ""
		if (msg['key'] in self.state_machine):
			value = self.state_machine[msg['key']]
		raw_msg = {'src': self.id, 'dst': msg['src'], 'leader': self.leader_id, 'type': 'ok', 'MID': msg['MID'],
				   'value': value}
		json_msg = json.dumps(raw_msg)
		if self.sock.send(json_msg):
			print '[%s] get response sent to client' % (self.id)
		self.last_applied += 1

	# Respond to a client's put message
	def put_response(self, msg):
		self.state_machine[msg['key']] = msg['value']
		self.last_applied += 1
		raw_msg = {'src': self.id, 'dst': msg['src'], 'leader': self.leader_id, 'type': 'ok', 'MID': msg['MID']}
		json_msg = json.dumps(raw_msg)
		if self.sock.send(json_msg):
			print '[%s] put response sent to client' % (self.id)
