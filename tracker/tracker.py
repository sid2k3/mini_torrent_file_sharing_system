import socket
from pathlib import Path
import threading
import json

BUFFER_SIZE = 512000
SEPARATOR = "--SEPARATE--"
DISCONNECT_MESSAGE = "DISCONNECT"
PORT = 5050
TRACKER_IP = socket.gethostbyname(socket.gethostname())
TRACKER_ADDR = (TRACKER_IP, PORT)
tracker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
tracker.bind(TRACKER_ADDR)
root_dir = Path(__file__).parent


def pad_string(string: str, size):
    return string.ljust(size, ' ')


def my_send(conn, msg, msg_len):
    total_sent = 0
    while total_sent < msg_len:
        sent = conn.send(msg[total_sent:])
        if sent == 0:
            raise RuntimeError("socket connection broken")
        total_sent = total_sent + sent


def my_recv(conn, msg_len):
    chunks = []
    bytes_recd = 0
    while bytes_recd < msg_len:
        chunk = conn.recv(min(msg_len - bytes_recd, 1024))
        if chunk == b'':
            raise RuntimeError("socket connection broken")
        chunks.append(chunk)
        bytes_recd = bytes_recd + len(chunk)
    return b''.join(chunks)


def start_tracker():
    infomap_path = root_dir / 'infomap'
    infomap_path.mkdir(exist_ok=True)
    tracker.listen()
    print(f"[TRACKER STARTED] LISTENING ON {TRACKER_ADDR}")
    while True:
        client, addr = tracker.accept()
        thread = threading.Thread(target=handle_requests, args=(client, addr))
        thread.start()
        print(f"[ACTIVE CONNECTIONS] {threading.active_count() - 1}")


def handle_requests(seeder: socket.socket, client_addr: (str, int)):
    print(f"[NEW CONNECTION] {client_addr} CONNECTED")
    request_type = my_recv(seeder, 10).decode()
    if request_type.split(" ")[0] == "SHARE":
        thread = threading.Thread(target=handle_share_requests, args=(seeder, client_addr))
        thread.start()
        print(f"[SHARE REQUEST] FROM {client_addr}")
    elif request_type.split(" ")[0] == "GET":
        thread = threading.Thread(target=handle_get_requests, args=(seeder, client_addr))
        thread.start()
        print(f"[GET SEEDER-LIST REQUEST] FROM {client_addr}")
    elif request_type.split(" ")[0] == "REMOVE":
        thread = threading.Thread(target=handle_remove_requests, args=(seeder, client_addr))
        thread.start()
        print(f"[REMOVE SEEDER REQUEST] FROM {client_addr}")
    else:
        print(request_type)


def handle_share_requests(seeder: socket.socket, seeder_addr: (str, int)):
    msg = my_recv(seeder, BUFFER_SIZE).decode()
    msg = msg.rstrip()
    print(msg)
    print(f"METADATA FROM {seeder_addr} RECEIVED")
    msg = msg.split(SEPARATOR)
    file_name = msg[0]
    file_string = msg[1]
    seeder_send_port = msg[2]
    seeder_url = f"{seeder_addr[0]}:{seeder_send_port}"
    try:
        with open(file=root_dir / "infomap/info_map.json", mode="r") as file:
            dictionary = json.load(file)
    except FileNotFoundError:
        dictionary = {}

    try:
        if seeder_url not in dictionary[file_string]["seeders"]:
            dictionary[file_string]["seeders"].append(seeder_url)
    except KeyError:
        dictionary[file_string] = {}
        dictionary[file_string]["seeders"] = [seeder_url]
        dictionary[file_string]["name"] = file_name

    with open(file=root_dir / "infomap/info_map.json", mode="w") as file:
        json.dump(dictionary, file, indent=4)

    print(f"SHARE REQUEST FROM {seeder_addr} HANDLED")
    print(f"SEEDER {seeder_url} ADDED TO INFO_MAP")


def handle_get_requests(client: socket.socket, client_addr: (str, int)):
    file_found = False
    seeder_list = []
    file_string = my_recv(client, BUFFER_SIZE).decode()
    file_string = file_string.rstrip()
    try:
        with open(file=root_dir / "infomap/info_map.json", mode="r") as file:
            dictionary = json.load(file)
    except FileNotFoundError:
        dictionary = {}

    if file_string in dictionary.keys():
        file_found = True

    if file_found:
        seeder_list = dictionary[file_string]["seeders"]
        seeder_list_string = SEPARATOR.join(seeder_list)
        my_send(client, pad_string(seeder_list_string, BUFFER_SIZE).encode(), BUFFER_SIZE)
        print(f"[SEEDER-LIST SENT] TO {client_addr}")
    else:
        nof_msg = pad_string("NO FILE FOUND", BUFFER_SIZE)

        my_send(client, nof_msg.encode(), len(nof_msg))
        print("[FILE STRING NOT FOUND]")
        print(f"[NO FILE FOUND MESSAGE] SENT TO {client_addr}")


def handle_remove_requests(seeder: socket.socket, seeder_addr: (str, int)):
    msg = my_recv(seeder, BUFFER_SIZE).decode()
    msg = msg.rstrip()
    print(msg)
    print(f"METADATA FROM {seeder_addr} RECEIVED")
    msg = msg.split(SEPARATOR)
    file_string = msg[0]
    seeder_send_port = msg[1]
    seeder_url = f"{seeder_addr[0]}:{seeder_send_port}"

    try:
        with open(file=root_dir / "infomap/info_map.json", mode="r") as file:
            dictionary = json.load(file)
        dictionary[file_string]["seeders"].remove(seeder_url)
        if len(dictionary[file_string]["seeders"]) == 0:
            dictionary.pop(file_string)

        print(f"SEEDER {seeder_url} REMOVED FROM INFOMAP")
        with open(file=root_dir / "infomap/info_map.json", mode="w") as file:
            json.dump(dictionary, file, indent=4)
    except FileNotFoundError:
        pass


start_tracker()
