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

		if self.msgs_to_redirect != []:
			for m in self.msgs_to_redirect:
				self.send_append_ent(m)
			self.msgs_to_redirect = []

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
		vote = (msg['term'] > self.current_term or
				(msg['term'] == self.current_term and
					msg['last_log_idx'] > len(self.log) - 1) or
				(msg['term'] == self.current_term and
					msg['last_log_idx'] == (len(self.log) - 1) and
					msg['last_log_term'] >= self.log[len(self.log) - 1][1]))

		self.leader_id = 'FFFF'
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
		if msg['entries'] == []:
			# initial heartbeat from new leader
			if self.voted_for != None:
				if msg['term'] >= self.current_term:
					# acceptable new leader
					print '[%s] New leader: %s' % (self.id, msg['leader'])
					if self.current_state == CANDIDATE:
						self.current_state = FOLLOWER

					self.voted_for = None
					self.votes = 0
					self.leader_id = msg['leader']
					self.current_term = msg['term']
					self.last = time.time()
					if self.msgs_to_redirect != []:
						for m in self.msgs_to_redirect:
							self.redirect_client(m)
						self.msgs_to_redirect = []

				else:
					# reject new leader, remain candidate
					raw = {'src': self.id, 'dst': msg['src'], 'leader': self.leader_id,
						   'type': FAIL, 'term': self.current_term, 'last_log_idx': None}
					reply = json.dumps(raw)
					if self.sock.send(reply):
						print '[%s] Rejected leader %s\n\n' % (self.id, msg['src'])

			# regular heartbeat
			else:
				self.last = time.time()
				reply_type = OK

		# regular append_entry_rpc
		else:
			print '[%s] Received appendEntryRPC' % (self.id)
			print '[%s] Received entries: %s' % (self.id, msg['entries'])
			prev_log_idx = msg['prev_log_idx']

			if msg['term'] < self.current_term or prev_log_idx > len(self.log)-1:
				reply_type = FAIL
				print '[%s] ****FAIL TO LEADER: lterm=%s < cur_term=%s ? OR prevlogidx=%s >= len(self.log)=%s ?' % (self.id, msg['term'], self.current_term, prev_log_idx, len(self.log))

			#elif len(self.log)-1 < prev_log_idx:
			#	reply_type = FAIL

			# TODO this gives an index out of bounds error (only when for some other reason prev_log_idx becomes -1)
			elif self.log[prev_log_idx][0] != msg['prev_log_term']:
				self.current_term = msg['term']
				# remove incorrect entry at prev_log_idx and any entries after
				self.log = self.log[:prev_log_idx]
				reply_type = FAIL
			#	print '[%s] ****FAIL TO LEADER: self.log[pli][0]=%s != msg[prevlogterm]=%s' % (self.id, self.log[prev_log_idx][0], msg['prev_log_term'])

			else: # logs match
				self.current_term = msg['term']
				# only keep entries up to what is matched with leader
				self.log = self.log[:prev_log_idx+1]
				for entry in msg['entries']:
					e = tuple(entry)
					if e not in self.log:
						self.log.append(tuple(entry))


				# TODO: remove log field from msg (for now, just used for checking leader log and replica logs match exactly)
				#leader_log = []
				#for e in msg['log']:
					#leader_log.append(tuple(e))

				#if (self.log != leader_log):
					#print 'LOGS DO NOT MATCH: [%s] = %s  ----- leader log = %s' % (self.id, self.log, leader_log)

				#print 'LOGS MATCHHHH!!!!'
				#print '[%s] before log: %s, entries: %s, prevlogidx=%s' % (self.id, self.log, entries, prev_log_idx)
				#print '[%s] self.log[:prev_log_idx+1] = %s' % (self.id, self.log[:prev_log_idx+1])
				# self.log = self.log[:prev_log_idx+1] + entries
				#print '[%s] after log: %s' % (self.id, self.log)
				reply_type = OK


			if reply_type == OK:
				if msg['leader_commit'] > self.commit_idx:
					self.commit_idx = min(msg['leader_commit'], len(self.log) - 1)

				if self.commit_idx > self.last_applied:
					#for ii in range(self.last_applied+1, self.commit_idx+1):
					while self.last_applied+1 < self.commit_idx+1:
						next_apply = self.last_applied+1
						if len(self.log)-1 < next_apply:
							print "bananas ID: %s, lastapplied: %s, commitidx: %s,length log: %s" % (self.id, self.last_applied, self.commit_idx, len(self.log))
							break
						else:
							print "ID: %s, log entry: %s" % (self.id, self.log[next_apply])
						cmd = self.log[next_apply][1]
						self.last_applied = next_apply
						if cmd['type'] == PUT:
							self.state_machine[cmd['key']] = cmd['value']


			raw = {'src': self.id, 'dst': self.leader_id, 'leader': self.leader_id,
					 'type': reply_type, 'term': self.current_term, 'last_log_idx': len(self.log) - 1}
			reply = json.dumps(raw)
			#self.sock.send(reply)
			if self.sock.send(reply):
				print '[%s] Sent %s to leader %s' % (self.id, reply_type, self.leader_id)

			# reset timeout clock
			self.last = time.time()

	# TODO: queue client requests if no leader or election occurring, and then respond to requests with redirects
	# respond to client with redirect message if not leader
	def redirect_client(self, msg):
		if self.leader_id == 'FFFF':
			self.msgs_to_redirect.append(msg)
			return
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

		#prev_log_idx = len(self.log) - 1  # index of log entry immediately preceding the new entry
		#prev_log_term = self.log[prev_log_idx][0]

		if msg != None and msg not in [entry[1] for entry in self.log]:  # new log entry
			self.log.append((self.current_term, msg))  # add client command to log
			# print 'LEADER LOG: %s' % self.log

			for id in self.replica_ids:
				prev_log_idx = self.next_idx[id] - 1
				prev_log_term = self.log[prev_log_idx][0]
				entries = self.log[self.next_idx[id]:]
				#print 'SENDING ENTRIES: %s' % entries
				# TODO: remove log field
				raw_msg = {'src': self.id, 'dst': id, 'leader': self.leader_id, 'type': APPEND_ENT,
						   'term': self.current_term, 'prev_log_idx': prev_log_idx,
						   'prev_log_term': prev_log_term, 'leader_commit': self.commit_idx,
						   'entries': entries}
				app_ent = json.dumps(raw_msg)
				#self.sock.send(app_ent)
				if len(app_ent) > 10000:
					print "ASSUME FAILURE of %s" % (id)
				elif self.sock.send(app_ent):
					print '[%s] Sent append_entry_rpc to follower %s' % (self.id, id)

		else:  # new leader, initial heartbeat for log replication
			entries = []  # log entries for the replica to store
			raw_msg = {'src': self.id, 'dst': 'FFFF', 'leader': self.leader_id,
					   'type': APPEND_ENT, 'term': self.current_term,
					   'prev_log_idx': None, 'prev_log_term': None,
					   'leader_commit': self.commit_idx, 'entries': entries}
			app_ent = json.dumps(raw_msg)
			#if len(app_ent) > 30000:
				#print "ASSUME FAILURE of %s" % (follower_id)
			if self.sock.send(app_ent):
				print '[%s] Sent heartbeat to all replicas' % self.id


	# handle receiving failed message from follower
	def handle_fail(self, msg):
		if msg['term'] > self.current_term:
			self.current_term = msg['term']
			self.become_follower()
			return

		# decrement and retry sending append_ent msg to that follower
		follower_id = msg['src']
		self.next_idx[follower_id] -= 1
		follower_next = self.next_idx[follower_id]
		while follower_next < len(self.log):
			a = min(follower_next+10, len(self.log))
			entries = self.log[follower_next:a]

			raw_msg = {'src': self.id, 'dst': follower_id, 'leader': self.leader_id, 'type': APPEND_ENT,
					   'term': self.current_term, 'prev_log_idx': follower_next - 1,
					   'prev_log_term': self.log[follower_next - 1][0], 'leader_commit': self.commit_idx,
				  	 'entries': entries}
			app_ent = json.dumps(raw_msg)
			if len(app_ent) > 10000:
				print "FAIL: ASSUME FAILURE of %s" % (follower_id)
			elif self.sock.send(app_ent):
				print '[%s] HANDLE FAIL: Sent append_entry rpc to %s' % (self.id, follower_id)
			follower_next = a


	# handle receiving ok message from follower
	def handle_ok(self, msg):
		# update follower's next_idx and match_idx
		follower_id = msg['src']
		self.next_idx[follower_id] = msg['last_log_idx'] + 1
		self.match_idx[follower_id] = msg['last_log_idx']

		if msg['last_log_idx'] >= len(self.log):
			print 'FOLLOWERS LOG LONGER THAN LEADER LOG (lastlogidx=%s >= %s)' % (msg['last_log_idx'], len(self.log))

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
		if highest_quorum_idx > self.commit_idx:
			print 'commit_idx=%s, highest_quorum_idx=%s, so now range(commitidx+1=%s, hqi+1=%s)' % (self.commit_idx, highest_quorum_idx, self.commit_idx+1, highest_quorum_idx+1)
			print 'next_idx=%s, match_idx= %s' % (self.next_idx, self.match_idx)
			#print 'LEADER LOG: %s' % self.log
			for ii in range(self.commit_idx+1, highest_quorum_idx+1):
				self.respond_to_client(ii)

			self.commit_idx = highest_quorum_idx

	# apply command to state machine and send response to client
	# @param idx: the log index of the command being responded to
	def respond_to_client(self, idx):
		print 'respond_to_client(idx=%s)' % idx
		msg = self.log[idx][1]
		if (msg['type'] == GET):
			self.get_response(msg)
		elif (msg['type'] == PUT):
			self.put_response(msg)
		#return 0

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

	# Respond to a client's put message
	def put_response(self, msg):
		self.state_machine[msg['key']] = msg['value']
		raw_msg = {'src': self.id, 'dst': msg['src'], 'leader': self.leader_id, 'type': 'ok', 'MID': msg['MID']}
		json_msg = json.dumps(raw_msg)
		if self.sock.send(json_msg):
			print '[%s] put response sent to client' % (self.id)
