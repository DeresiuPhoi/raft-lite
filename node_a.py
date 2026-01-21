from flask import Flask, jsonify, request
import time
import threading
import requests

app = Flask(__name__)

NODE_ID = "A"
PORT = 8000
PEERS = [
    {"ip": "172.31.18.70", "port": 8001, "id": "B"},
    {"ip": "172.31.16.112", "port": 8002, "id": "C"}
]

# Raft state
state = "follower"
current_term = 0
voted_for = None
log = []
last_heartbeat = time.time()
votes_received = 0
is_leader = False

def election_timer():
    global state, last_heartbeat, votes_received
    while True:
        time.sleep(0.5)
        if state == "follower" and (time.time() - last_heartbeat) > 2:
            start_election()

def start_election():
    global state, current_term, voted_for, votes_received
    state = "candidate"
    current_term += 1
    voted_for = NODE_ID
    votes_received = 1  # Vote for self
    last_heartbeat = time.time()
    
    print(f"\n[Node {NODE_ID}] ⏰ ELECTION TIMEOUT!")
    print(f"[Node {NODE_ID}] ➤ CANDIDATE (term={current_term})")
    
    # Request votes from peers
    for peer in PEERS:
        threading.Thread(target=request_vote_from, args=(peer,)).start()

def request_vote_from(peer):
    global votes_received, state, is_leader
    try:
        url = f"http://{peer['ip']}:{peer['port']}/request_vote"
        data = {'term': current_term, 'candidate_id': NODE_ID}
        response = requests.post(url, json=data, timeout=1)
        
        if response.json().get('vote_granted'):
            print(f"[Node {NODE_ID}] ✅ Vote GRANTED by {peer['id']}")
            votes_received += 1
            
            # Check for majority (2 out of 3)
            if votes_received >= 2 and state == "candidate":
                become_leader()
    except:
        print(f"[Node {NODE_ID}] ❌ Cannot reach {peer['id']}")

def become_leader():
    global state, is_leader
    state = "leader"
    is_leader = True
    
    print(f"\n{'='*50}")
    print(f"🎉 [Node {NODE_ID}] LEADER ELECTED!")
    print(f"   Term: {current_term}")
    print(f"{'='*50}\n")
    
    threading.Thread(target=send_heartbeats).start()

def send_heartbeats():
    while is_leader:
        time.sleep(1)
        print(f"[Leader {NODE_ID}] 💓 Sending heartbeats...")
        for peer in PEERS:
            try:
                url = f"http://{peer['ip']}:{peer['port']}/append_entries"
                data = {'term': current_term, 'leader_id': NODE_ID}
                requests.post(url, json=data, timeout=0.5)
            except:
                pass

@app.route('/request_vote', methods=['POST'])
def handle_request_vote():
    global voted_for, last_heartbeat, current_term, state
    data = request.json
    
    print(f"\n[Node {NODE_ID}] 📥 Vote request from {data['candidate_id']}")
    
    if data['term'] > current_term:
        current_term = data['term']
        state = "follower"
        voted_for = None
    
    if voted_for is None or voted_for == data['candidate_id']:
        voted_for = data['candidate_id']
        last_heartbeat = time.time()
        print(f"✅ [Node {NODE_ID}] VOTED for {data['candidate_id']}")
        return jsonify({'vote_granted': True, 'term': current_term})
    else:
        print(f"❌ [Node {NODE_ID}] Already voted for {voted_for}")
        return jsonify({'vote_granted': False, 'term': current_term})

@app.route('/append_entries', methods=['POST'])
def handle_append_entries():
    global last_heartbeat, state
    data = request.json
    
    if data['term'] >= current_term:
        last_heartbeat = time.time()
        if state != "follower":
            state = "follower"
            print(f"[Node {NODE_ID}] ➤ Becoming FOLLOWER")
    
    return jsonify({'success': True, 'term': current_term})

@app.route('/client_command', methods=['POST'])
def client_command():
    if not is_leader:
        return jsonify({'success': False, 'error': 'Not leader'})
    
    data = request.json
    command = data['command']
    
    print(f"\n📝 [Leader {NODE_ID}] CLIENT COMMAND")
    print(f"   Command: {command}")
    print(f"   Term: {current_term}")
    
    log.append({'term': current_term, 'command': command})
    
    return jsonify({
        'success': True,
        'leader_id': NODE_ID,
        'log_index': len(log)-1
    })

@app.route('/kill', methods=['POST'])
def kill():
    global is_leader, state
    is_leader = False
    state = "follower"
    
    print(f"\n💀 [Node {NODE_ID}] LEADER CRASH SIMULATED")
    return jsonify({'status': 'killed', 'node': NODE_ID})

@app.route('/status')
def status():
    return jsonify({
        'id': NODE_ID,
        'state': state,
        'term': current_term,
        'voted_for': voted_for,
        'is_leader': is_leader,
        'log_length': len(log)
    })

@app.route('/')
def home():
    return f"Node {NODE_ID} - {state.upper()} - Term: {current_term}"

if __name__ == '__main__':
    timer_thread = threading.Thread(target=election_timer)
    timer_thread.daemon = True
    timer_thread.start()
    
    print(f"\n{'='*50}")
    print(f"🚀 STARTING NODE {NODE_ID}")
    print(f"📡 Port: {PORT}")
    print(f"{'='*50}\n")
    
    app.run(host='0.0.0.0', port=PORT, debug=False, threaded=True)
