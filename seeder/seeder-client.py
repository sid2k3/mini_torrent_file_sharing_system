import random
import socket
import os
import hashlib
import json
import threading
from pathlib import Path
from torrent_file_processor import SidTorrentFile
import time

BUFFER_SIZE = 512000
HEADER_SIZE = 50
SEPARATOR = "--SEPARATE--"
DISCONNECT_MESSAGE = "DISCONNECT"
PORT = 5050
TRACKER_IP = "192.168.1.43"
TRACKER_ADDR = (TRACKER_IP, PORT)
file_path = "C:\\Users\\Jetblack\\PycharmProjects\\sidtorrent\\seeder\\test.pdf"
SEND_PORT = 5051
THIS_ADDRESS = (socket.gethostbyname(socket.gethostname()), SEND_PORT)

root_dir = Path(__file__).parent
download_dir = root_dir / "downloaded_files"


def pad_string(string: str, size):
    return string.ljust(size, ' ')


def generate_hash_string(filepath: str):
    hash_string = ""
    with open(filepath, mode="rb") as file:
        while True:
            hash1 = hashlib.sha1()
            bytes_read = file.read(BUFFER_SIZE)
            # print("ok")
            if not bytes_read:
                break

            hash1.update(bytes_read)
            hash_string += hash1.hexdigest()
    print(hash_string)
    print(type(hash_string))
    print(f"Pieces: {len(hash_string) / 40} ")

    print("***************************************")
    return hash_string


def generate_torrent_file(filepath: str):
    tracker_url = {
        "tracker_ip": TRACKER_ADDR[0],
        "tracker_port": TRACKER_ADDR[1]
    }
    hash_string = generate_hash_string(filepath)
    dictionary = {
        "tracker_url": tracker_url,
        "hash_string": hash_string,
        "file_name": os.path.basename(filepath),
        "file_size": os.path.getsize(filepath)
    }
    file_name = os.path.basename(filepath)
    print(file_name)
    hash_of_hash_string = update_path_map(hash_string, filepath)
    with open(f"torrent_files/{file_name}.sidtorrent", mode="w") as torrent_file:
        json.dump(dictionary, torrent_file, indent=4)

    return hash_of_hash_string


def update_path_map(hash_string, filepath: str):
    # file string

    # print(hash_string)
    # print(type(hash_string))

    hash_of_hash_string = hashlib.sha1(hash_string.encode()).hexdigest()
    # print(len(hash_of_hash_string))

    dictionary = {}
    try:
        with open("currently_seeding/seeding.json", mode="r") as file:
            dictionary = json.load(file)
    except FileNotFoundError:
        pass

    dictionary[hash_of_hash_string] = {}
    dictionary[hash_of_hash_string]["path"] = filepath
    dictionary[hash_of_hash_string]["pieces"] = [str(i) for i in range(int(len(hash_string) / 40))]
    with open("currently_seeding/seeding.json", mode="w") as file:
        json.dump(dictionary, file, indent=4)

    return hash_of_hash_string


def share_with_tracker(filepath: str):
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(TRACKER_ADDR)
    file_name = os.path.basename(filepath)
    hash_of_hash_string = generate_torrent_file(filepath)
    port = SEND_PORT
    client.sendall(f"{'SHARE':<10}".encode())
    msg = f"{file_name}{SEPARATOR}{hash_of_hash_string}{SEPARATOR}{SEND_PORT}"
    client.sendall(msg.encode())
    print("METADATA SUCCESSFULLY SENT TO TRACKER")
    client.close()


def get_seeder_list_from_tracker(torrent_filepath: str):
    torrent_file = SidTorrentFile(torrent_filepath)

    tracker_url = torrent_file.tracker_url

    file_string = torrent_file.file_string
    conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    conn.connect(tracker_url)
    conn.sendall(f"{'GET':<10}".encode())
    print("ok")
    print(file_string)
    conn.sendall(file_string.encode())
    print("ok")
    seeders = conn.recv(BUFFER_SIZE).decode()
    if seeders == "NO FILE FOUND":
        print("CORRUPT TORRENT FILE")
        seeder_list = []
    else:
        print(f"[SEEDER LIST RECEIVED] FROM {tracker_url}")
        seeder_list = seeders.split(SEPARATOR)
        print(seeder_list)

    conn.close()

    return seeder_list


# pass destination path
def download_file_from_seeders(torrent_file_path: str):
    seeder_list = get_seeder_list_from_tracker(torrent_file_path)
    torrent_file = SidTorrentFile(torrent_file_path)
    num_of_pieces = torrent_file.pieces
    file_received = False

    if not seeder_list:
        print("NO SEEDERS AVAILABLE or CORRUPT TORRENT FILE")
        return

    seeder_list = list(map(lambda addr: (addr.split(":")[0], int(addr.split(":")[1])), seeder_list))
    print("SEEDER LIST:")
    print(seeder_list)

    map_of_connections = connect_to_all_seeders(seeder_list, torrent_file.file_string)

    map_of_pieces_to_seeders = get_pieces_info_from_seeders(seeder_list, torrent_file.file_string, map_of_connections)

    # arrange pieces by rarity
    print("*********************************************************")
    print("ARRANGED PIECES:")
    arranged_pieces = rarest_first(map_of_pieces_to_seeders)
    print(arranged_pieces)
    modified_file_name = f"downloaded-{torrent_file.file_name}"

    destination_path = download_dir / modified_file_name
    with open(destination_path, mode="wb") as file:
        pass

    # listening_sockets = set()
    received_pieces = set()

    # TODO THREAD
    for seeder in map_of_connections:
        conn = map_of_connections[seeder]
        thread1 = threading.Thread(target=write_to_file, args=(
            conn, destination_path, map_of_connections, map_of_pieces_to_seeders, received_pieces,
            file_received))

        thread1.start()
    while len(received_pieces) != num_of_pieces:
        for piece in arranged_pieces:
            if piece in received_pieces:
                continue
            # time.sleep(0.1)
            # TODO: ADD THREAD
            thread1 = threading.Thread(target=get_piece_from_seeder,
                                       args=(piece, map_of_connections, map_of_pieces_to_seeders, destination_path,

                                             received_pieces, file_received))
            thread1.start()
        print("SLEEPING FOR 60s BEFORE NEXT TRY")
        time.sleep(60)
        # get_piece_from_seeder(piece, map_of_connections, map_of_pieces_to_seeders, destination_path,
        #                       listening_sockets,
        #                       received_pieces)

    print("ALL PIECES WRITTEN TO FILE")
    print("SENDING DISCONNECT MESSAGE TO ALL CONNECTIONS")
    file_received = True
    # time.sleep(10)

    for seeder in map_of_connections:
        conn = map_of_connections[seeder]
        conn.sendall(pad_string(DISCONNECT_MESSAGE, 40).encode())

    # seeder_1 = seeder_list[0]
    # conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # print(seeder_1)
    # conn.connect(seeder_1)
    #
    # print(f"[DOWNLOAD CONNECTION ESTABLISHED] WITH {seeder_1}")
    #
    # thread = threading.Thread(target=write_to_file, args=(conn, torrent_file))
    # thread.start()
    # pieces


test_list = []


def get_piece_from_seeder(piece_no: int, map_of_connections: dict, map_of_pieces_to_seeders: dict, destination_path,
                          received_pieces: set, file_received: bool):
    """Gets the specified piece from the seeder and writes to file"""
    random.shuffle(map_of_pieces_to_seeders[piece_no])
    # TODO: ADD LOGIC FOR CONTACTING 2ND SEEDER IF 1ST IS OFFLINE

    seeder_for_current_piece = map_of_pieces_to_seeders[piece_no][0]
    conn = map_of_connections[seeder_for_current_piece]
    print(f"REQUESTING PIECE {piece_no} FROM SEEDER {seeder_for_current_piece}")
    temp = pad_string(f"DOWNLOAD{SEPARATOR}{piece_no}", 40)
    print(temp)
    test_list.append((piece_no, conn.getpeername()[0]))
    conn.sendall(temp.encode())
    # if conn.getpeername()[0] not in listening_sockets:
    #     listening_sockets.add(conn.getpeername()[0])
    #     # TODO THREAD
    #     thread1 = threading.Thread(target=write_to_file, args=(
    #         conn, destination_path, map_of_connections, map_of_pieces_to_seeders, received_pieces,
    #         file_received))
    #
    #     thread1.start()
    # write_to_file(conn, destination_path, map_of_connections, map_of_pieces_to_seeders, received_pieces,
    #               file_received)


def write_to_file(conn: socket.socket, destination_path, map_of_connections: dict,
                  map_of_pieces_to_seeders: dict, received_pieces: set, file_received: bool):
    last_piece_no = len(map_of_pieces_to_seeders)

    with open(destination_path, mode="r+b") as file:
        while not file_received:
            invalid = False
            try:
                bytes_received = conn.recv(BUFFER_SIZE + HEADER_SIZE)
            except ConnectionAbortedError or ConnectionResetError:
                break
            try:
                header_received = bytes_received[0:50].decode()
                print("**************************************RECEIVED HEADER**************************************")
                print(header_received)
                print("**************************************RECEIVED HEADER**************************************")
                if header_received.split(SEPARATOR)[0] == "HEADER" and 0 <= int(
                        header_received.split(SEPARATOR)[1]) <= last_piece_no:
                    piece_no = int(header_received.split(SEPARATOR)[1])
                else:
                    invalid = True
            except UnicodeDecodeError:
                print("UNICODE DECODE ERROR")
                invalid = True

            if invalid:
                # TODO TRY TO EMPTY BUFFER NOW
                print(f"Invalid Packet from {conn.getpeername()}")
                continue
                # print(f"REQUESTING PIECE {piece_no} AGAIN")
                # file.close()
                # get_piece_from_seeder(piece_no, map_of_connections, map_of_pieces_to_seeders, destination_path)

            if piece_no in received_pieces:
                continue
            print("***********HEADER*************")
            print(header_received)
            print("***********HEADER*************")
            data_received = bytes_received[50:]

            print(f"RECEIVED PIECE NO. {piece_no}")
            print(f"PIECE: {piece_no} RECEIVED AND WRITTEN")
            file.seek(piece_no * BUFFER_SIZE, 0)
            print("*************************************")
            print(f"CALCULATION: {piece_no * BUFFER_SIZE}")
            print("*************************************")

            file.write(data_received)
            received_pieces.add(piece_no)
            print(f"WRITTEN PIECE NO. {piece_no} at offset {file.tell()}")


def connect_to_all_seeders(seeder_list: list, file_string: str):
    """Creates a map between seeder address and the corresponding socket"""
    map_of_connections = {}
    for seeder in seeder_list:
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # TODO: HANDLE EXCEPTION HERE
        conn.connect(seeder)
        print(f"CONNECTION WITH SEEDER {seeder} ESTABLISHED")
        map_of_connections[seeder] = conn
        conn.sendall(file_string.encode())

    print(f"SEEDERS CONNECTED: {len(map_of_connections)} out of {len(seeder_list)} seeders")
    return map_of_connections


def get_pieces_info_from_seeders(seeder_list: list, file_string: str, map_of_connections: dict):
    map_of_pieces_to_seeders = {}

    for seeder in seeder_list:
        conn = map_of_connections[seeder]

        conn.sendall(pad_string("PIECES", 10).encode())

        # RECEIVE PIECES INFO
        pieces_available = conn.recv(BUFFER_SIZE).decode().rstrip().split(SEPARATOR)
        pieces_available = map(lambda x: int(x), pieces_available)

        for piece in pieces_available:
            if piece in map_of_pieces_to_seeders:
                map_of_pieces_to_seeders[piece].append(seeder)
            else:
                map_of_pieces_to_seeders[piece] = [seeder]

    print("**************************************")
    print("MAP OF PIECES:")
    print(map_of_pieces_to_seeders)
    print("**************************************")
    return map_of_pieces_to_seeders


def rarest_first(map_of_pieces_to_seeders: dict):
    final_pieces = sorted(map_of_pieces_to_seeders.keys(), key=lambda dic_key: len(map_of_pieces_to_seeders[dic_key]))
    return final_pieces


def send_pieces_info(conn: socket.socket, other_client_addr: (str, int), file_string: str):
    print(f"REQUESTED PIECES INFO FOR FILE: {file_string}")
    with open(root_dir / "currently_seeding/seeding.json", mode="r") as file:
        currently_seeding = json.load(file)

    # TODO: HANDLE EXCEPTION HERE
    if file_string in currently_seeding:
        pieces_string = SEPARATOR.join(currently_seeding[file_string]["pieces"])
        conn.sendall(pad_string(pieces_string, BUFFER_SIZE).encode())
        print("PIECES INFO SENT SUCCESSFULLY")
    else:
        pass


def listen_for_connections():
    """Listens for connections from other clients"""

    this_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    this_client.bind(THIS_ADDRESS)
    this_client.listen()
    print(f"[SEEDER STARTED] LISTENING ON {THIS_ADDRESS}")
    while True:
        conn, other_client_addr = this_client.accept()
        thread = threading.Thread(target=handle_requests, args=(conn, other_client_addr))
        thread.start()


def handle_requests(conn: socket.socket, other_client_addr: (str, int)):
    """DIRECTS REQUEST TO THE CORRESPONDING FUNCTION"""
    print(f"CONNECTION FROM {other_client_addr}")
    file_required = conn.recv(40).decode()
    print(f"REQUESTED FILE {file_required}")
    request_type = conn.recv(10).decode()
    request_type = request_type.rstrip()
    print(f"REQUEST TYPE: {request_type}")
    if request_type == "PIECES":
        send_pieces_info(conn, other_client_addr, file_required)

    # WAIT FOR DOWNLOAD REQUEST
    print(f"WAITING FOR DOWNLOAD REQUEST FROM {other_client_addr}")
    while True:
        download_req = conn.recv(40).decode().rstrip()
        if download_req == DISCONNECT_MESSAGE:
            print(f"DISCONNECT REQUEST FROM {other_client_addr}")
            conn.close()
            break

        print(download_req)
        if download_req.split(SEPARATOR)[0] == "DOWNLOAD":
            try:
                piece_requested = int(download_req.split(SEPARATOR)[1])
            except ValueError:
                continue

            print(f"DOWNLOAD REQUEST FROM {other_client_addr} PIECE REQUESTED: {piece_requested}")
            # thread = threading.Thread(target=handle_download_request,
            #                           args=(conn, piece_requested, file_required))
            # thread.start()
            handle_download_request(conn, piece_requested, file_required)
            print(f"DOWNLOAD REQUEST FOR PIECE {piece_requested} HANDLED")


def handle_download_request(conn: socket.socket, piece_requested, file_string):
    """HANDLES DOWNLOAD REQUEST AND SHARES THE REQUIRED PIECE TO THE CLIENT"""
    required_file_path = ""
    # try except here
    print("ok")
    with open(root_dir / "currently_seeding/seeding.json", mode="r") as file:
        currently_seeding = json.load(file)

    # print(currently_seeding)
    print("ok1")
    print(file_string)
    if file_string in currently_seeding:
        required_file_path = currently_seeding[file_string]["path"]
        print(f"REQUESTED FILE FOUND, PATH: {required_file_path}")
        print("SENDING REQUIRED PIECE TO CLIENT")

    header_string = pad_string(f"HEADER{SEPARATOR}{piece_requested}{SEPARATOR}", HEADER_SIZE).encode()

    with open(required_file_path, mode="rb") as file:
        file.seek(piece_requested * BUFFER_SIZE, 0)
        bytes_read = file.read(BUFFER_SIZE)
        header_with_data = header_string + bytes_read
        conn.sendall(header_with_data)
        print(f"REQUIRED PIECE ({piece_requested}) SENT")


# def handle_download_request(download_conn: socket.socket, other_client_addr: (str, int)):
#     """Handles download request from other clients and shares the file with them without errors."""
#
#     print(f"DOWNLOAD CONNECTION FROM {other_client_addr}")
#     file_string = download_conn.recv(40).decode()
#     print(f"REQUESTED FILE: {file_string}")
#     # Receiving file string of length 40
#     required_file_path = ""
#     # try except here
#     print("ok")
#     with open("currently_seeding/seeding.json", mode="r") as file:
#         currently_seeding = json.load(file)
#
#     print(currently_seeding)
#     print("ok1")
#     print(file_string)
#     if file_string in currently_seeding:
#         required_file_path = currently_seeding[file_string]["path"]
#         print(f"REQUESTED FILE FOUND, PATH: {required_file_path}")
#         print("SENDING REQUIRED FILE TO CLIENT")
#
#     with open(required_file_path, mode="rb") as file:
#         while True:
#             bytes_read = file.read(BUFFER_SIZE)
#             if not bytes_read:
#                 download_conn.shutdown(socket.SHUT_WR)
#
#                 print(f"FILE SENT SUCCESSFULLY TO {other_client_addr}")
#                 download_conn.close()
#                 print(f"CONNECTION WITH {other_client_addr} CLOSED")
#                 print("*****************************************************************")
#                 break
#
#             download_conn.sendall(bytes_read)


# TODO:Add remove functionality for seeders
# TODO:Add try except for every socket connection
# TODO:Add error handling for the case when the file doesn't exist on the seeder
# other_file_path = root_dir / "file1.mp4"
# print(other_file_path.as_posix())
# share_with_tracker(other_file_path.as_posix())
# torrent_file_path = input("Enter torrent file path: ")
# # get_seeder_list_from_tracker(torrent_file_path)
# listen_for_connections()
# #
torrent_file_path = input("Enter torrent file path: ")

download_file_from_seeders(torrent_file_path)
# print(__file__)
# print("poklsadkapsokd")
# print(test_list)
