import socket
import os
import hashlib
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


def start_tracker():
    tracker.listen()
    print(f"[TRACKER STARTED] LISTENING ON {TRACKER_ADDR}")
    while True:
        client, addr = tracker.accept()
        thread = threading.Thread(target=handle_requests, args=(client, addr))
        thread.start()
        print(f"[ACTIVE CONNECTIONS] {threading.active_count() - 1}")


def handle_requests(seeder: socket.socket, client_addr: (str, int)):
    print(f"[NEW CONNECTION] {client_addr} CONNECTED")
    request_type = seeder.recv(10).decode()
    if request_type.split(" ")[0] == "SHARE":
        thread = threading.Thread(target=handle_share_requests, args=(seeder, client_addr))
        thread.start()
        print(f"[SHARE REQUEST] FROM {client_addr}")
    elif request_type.split(" ")[0] == "GET":
        thread = threading.Thread(target=handle_get_requests, args=(seeder, client_addr))
        thread.start()
        print(f"[GET SEEDER-LIST REQUEST] FROM {client_addr}")
    else:
        print(request_type)


def handle_share_requests(seeder: socket.socket, seeder_addr: (str, int)):
    msg = seeder.recv(BUFFER_SIZE).decode()
    print(msg)
    print(f"METADATA FROM {seeder_addr} RECEIVED")
    msg = msg.split(SEPARATOR)
    file_name = msg[0]
    file_string = msg[1]
    seeder_send_port = msg[2]
    seeder_url = f"{seeder_addr[0]}:{seeder_send_port}"
    try:
        with open(file="infomap/info_map.json", mode="r") as file:
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

    with open(file="infomap/info_map.json", mode="w") as file:
        json.dump(dictionary, file, indent=4)

    print(f"SHARE REQUEST FROM {seeder_addr} HANDLED")
    print(f"SEEDER {seeder_url} ADDED TO INFO_MAP")


def handle_get_requests(client: socket.socket, client_addr: (str, int)):
    file_found = False
    seeder_list = []
    file_string = client.recv(BUFFER_SIZE).decode()
    try:
        with open(file="infomap/info_map.json", mode="r") as file:
            dictionary = json.load(file)
    except FileNotFoundError:
        dictionary = {}

    if file_string in dictionary.keys():
        file_found = True

    if file_found:
        seeder_list = dictionary[file_string]["seeders"]
        seeder_list_string = SEPARATOR.join(seeder_list)
        client.sendall(seeder_list_string.encode())
        print(f"[SEEDER-LIST SENT] TO {client_addr}")
    else:
        client.sendall("NO FILE FOUND".encode())
        print("[FILE STRING NOT FOUND]")
        print(f"[NO FILE FOUND MESSAGE] SENT TO {client_addr}")


start_tracker()
