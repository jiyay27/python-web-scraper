import os
import socket
import threading
import json
import sys

HEAD_SERVER_HOST = 'localhost'
HEAD_SERVER_PORT = 9000

STORAGE_NODES = {
    'node1': {'host': 'localhost', 'port': 9001, 'capacity': 1000, 'used': 0},
    'node2': {'host': 'localhost', 'port': 9002, 'capacity': 500, 'used': 0}
}
FILE_INDEX = {}  # filename: node_id
rr_index = 0

STORAGE_ALGO = os.environ.get('STORAGE_ALGO', 'round_robin').lower()
print(f"[HEAD SERVER] Using algorithm: {STORAGE_ALGO}")

def handle_client(conn, addr):
    global rr_index
    try:
        data = conn.recv(4096).decode()
        command = data.strip().split()
        if not command:
            return

        action = command[0].upper()

        if action == 'LIST':
            conn.send(json.dumps(list(FILE_INDEX.keys())).encode())

        elif action == 'SAVE':
            filename = command[1]
            filesize = int(command[2])
            content = conn.recv(filesize)

            node_id = select_storage_node(filesize)
            if node_id is None:
                conn.send(b'NO SPACE')
                return

            node = STORAGE_NODES[node_id]
            send_to_storage_node(node, f'SAVE {filename} {filesize}', content)
            FILE_INDEX[filename] = node_id
            node['used'] += filesize
            conn.send(b'SAVED')

        elif action == 'RETRIEVE':
            filename = command[1]
            node_id = FILE_INDEX.get(filename)
            if node_id:
                node = STORAGE_NODES[node_id]
                data = send_to_storage_node(node, f'RETRIEVE {filename}')
                conn.send(data)
            else:
                conn.send(b'NOT FOUND')

        elif action == 'DELETE':
            filename = command[1]
            node_id = FILE_INDEX.get(filename)
            if node_id:
                node = STORAGE_NODES[node_id]
                send_to_storage_node(node, f'DELETE {filename}')
                del FILE_INDEX[filename]
                conn.send(b'DELETED')
            else:
                conn.send(b'NOT FOUND')

    finally:
        conn.close()

def send_to_storage_node(node, command, filedata=None):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((node['host'], node['port']))
        s.send(command.encode())
        if filedata:
            s.send(filedata)
        response = s.recv(1024 * 1024)
        return response

def select_storage_node(filesize):
    global rr_index
    nodes = list(STORAGE_NODES.keys())
    
    if STORAGE_ALGO == 'round_robin':
        for _ in nodes:
            node_id = nodes[rr_index % len(nodes)]
            rr_index += 1
            node = STORAGE_NODES[node_id]
            if node['used'] + filesize <= node['capacity']:
                return node_id
        return None

    elif STORAGE_ALGO == 'threshold':
        for node_id, node in STORAGE_NODES.items():
            if node['used'] + filesize <= node['capacity'] * 0.8:
                return node_id
        return None

    elif STORAGE_ALGO == 'balance':
        candidates = [(nid, node) for nid, node in STORAGE_NODES.items() if node['used'] + filesize <= node['capacity']]
        if not candidates:
            return None
        return min(candidates, key=lambda x: x[1]['used'])[0]

    else:
        print(f"[ERROR] Unknown algorithm: {STORAGE_ALGO}")
        return None

def start_head_server():
    print(f"[HEAD SERVER] Running on {HEAD_SERVER_HOST}:{HEAD_SERVER_PORT}")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HEAD_SERVER_HOST, HEAD_SERVER_PORT))
        s.listen()
        while True:
            conn, addr = s.accept()
            threading.Thread(target=handle_client, args=(conn, addr)).start()

def start_storage_node(node_id, host, port, storage_dir):
    os.makedirs(storage_dir, exist_ok=True)

    def handle_storage(conn, addr):
        try:
            data = conn.recv(4096).decode()
            command = data.strip().split()
            if not command:
                return
            action = command[0].upper()
            filename = command[1]
            filepath = os.path.join(storage_dir, filename)

            if action == 'SAVE':
                filesize = int(command[2])
                content = conn.recv(filesize)
                with open(filepath, 'wb') as f:
                    f.write(content)
                conn.send(b'SAVED')

            elif action == 'RETRIEVE':
                if os.path.exists(filepath):
                    with open(filepath, 'rb') as f:
                        conn.send(f.read())
                else:
                    conn.send(b'NOT FOUND')

            elif action == 'DELETE':
                if os.path.exists(filepath):
                    os.remove(filepath)
                    conn.send(b'DELETED')
                else:
                    conn.send(b'NOT FOUND')
        finally:
            conn.close()

    print(f"[STORAGE NODE {node_id}] Running on {host}:{port}")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, port))
        s.listen()
        while True:
            conn, addr = s.accept()
            threading.Thread(target=handle_storage, args=(conn, addr)).start()

if __name__ == '__main__':
    if len(sys.argv) == 1:
        start_head_server()
    elif sys.argv[1] == 'node1':
        start_storage_node('node1', 'localhost', 9001, './storage_node1')
    elif sys.argv[1] == 'node2':
        start_storage_node('node2', 'localhost', 9002, './storage_node2')