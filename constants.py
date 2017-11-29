

# STATES
FOLLOWER = 'follower'
CANDIDATE = 'candidate'
LEADER = 'leader'


# MESSAGE TYPES
APPEND_ENT = 'append_entry_rpc'
REDIRECT = 'redirect'
# client msg types
PUT = 'put'
GET = 'get'
# follower response types to leader append_entry_rpc()
OK = 'ok'
FAIL = 'fail'
# voting/leader election
VOTE_REQ = 'vote_request_rpc'
VOTE_REPLY = 'vote_reply'

