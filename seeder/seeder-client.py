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
# file_path = "C:\\Users\\Jetblack\\PycharmProjects\\sidtorrent\\seeder\\test.pdf"
SEND_PORT = 10023
THIS_ADDRESS = (socket.gethostbyname(socket.gethostname()), SEND_PORT)

root_dir = Path(__file__).parent
download_dir = root_dir / "downloaded_files"


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


def pad_string(string: str, size):
    return string.ljust(size, ' ')


def generate_hash_string(filepath: Path):
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


def generate_torrent_file(filepath: Path):
    tracker_url = {
        "tracker_ip": TRACKER_ADDR[0],
        "tracker_port": TRACKER_ADDR[1]
    }
    hash_string = generate_hash_string(filepath)
    dictionary = {
        "tracker_url": tracker_url,
        "hash_string": hash_string,
        "file_name": filepath.name,
        "file_size": os.path.getsize(filepath.as_posix())
    }
    file_name = os.path.basename(filepath)
    print(file_name)
    hash_of_hash_string = update_path_map(hash_string, filepath)
    try:
        with open(root_dir / f"torrent_files/{file_name}.sidtorrent", mode="w") as torrent_file:
            json.dump(dictionary, torrent_file, indent=4)
    except FileNotFoundError:
        Path(root_dir / 'torrent_files').mkdir()
        with open(root_dir / f"torrent_files/{file_name}.sidtorrent", mode="w") as torrent_file:
            json.dump(dictionary, torrent_file, indent=4)

    return hash_of_hash_string


def update_path_map(hash_string, filepath: Path):
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
    dictionary[hash_of_hash_string]["path"] = filepath.as_posix()
    dictionary[hash_of_hash_string]["pieces"] = [str(i) for i in range(int(len(hash_string) / 40))]
    try:
        with open("currently_seeding/seeding.json", mode="w") as file:
            json.dump(dictionary, file, indent=4)
    except FileNotFoundError:
        Path(root_dir / 'currently_seeding').mkdir()
        with open("currently_seeding/seeding.json", mode="w") as file:
            json.dump(dictionary, file, indent=4)

    return hash_of_hash_string


def update_path_map_while_downloading(filepath: Path, file_string: str, received_pieces: set):
    dictionary = {}
    try:
        with open("currently_seeding/seeding.json", mode="r") as file:
            dictionary = json.load(file)
    except FileNotFoundError:
        pass

    dictionary[file_string] = {}
    dictionary[file_string]["path"] = filepath.as_posix()
    dictionary[file_string]["pieces"] = [str(i) for i in received_pieces]
    print(root_dir)

    try:
        with open(root_dir / 'currently_seeding/seeding.json', mode="w") as file:
            json.dump(dictionary, file, indent=4)
    except FileNotFoundError:
        Path(root_dir / 'currently_seeding').mkdir()
        with open("currently_seeding/seeding.json", mode="w") as file:
            json.dump(dictionary, file, indent=4)


def share_with_tracker(filepath: Path, hash_of_hashstring: str = "", received_pieces: set = ()):
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(TRACKER_ADDR)
    file_name = filepath.name
    if hash_of_hashstring == "":
        hash_of_hash_string = generate_torrent_file(filepath)
    else:
        hash_of_hash_string = hash_of_hashstring
        update_path_map_while_downloading(filepath, hash_of_hash_string, received_pieces)

    my_send(client, f"{'SHARE':<10}".encode(), 10)
    msg = f"{file_name}{SEPARATOR}{hash_of_hash_string}{SEPARATOR}{SEND_PORT}"
    my_send(client, pad_string(msg, BUFFER_SIZE).encode(), BUFFER_SIZE)
    print("METADATA SUCCESSFULLY SENT TO TRACKER")
    print("CONNECTION WITH TRACKER CLOSED")
    client.close()


def get_seeder_list_from_tracker(torrent_filepath: str):
    torrent_file = SidTorrentFile(torrent_filepath)

    tracker_url = torrent_file.tracker_url

    file_string = torrent_file.file_string
    conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    conn.connect(tracker_url)
    my_send(conn, f"{'GET':<10}".encode(), 10)
    print("ok")
    print(file_string)
    my_send(conn, pad_string(file_string, BUFFER_SIZE).encode(), BUFFER_SIZE)
    print("ok")
    seeders = my_recv(conn, BUFFER_SIZE).decode().rstrip()
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
    try:
        with open(destination_path, mode="wb") as file:
            pass
    except FileNotFoundError:

        download_dir.mkdir()
        with open(destination_path, mode="wb") as file:
            pass

    received_pieces = set()

    # TODO THREAD
    for seeder in map_of_connections:
        conn = map_of_connections[seeder]
        thread1 = threading.Thread(target=write_to_file, args=(
            conn, destination_path, map_of_connections, map_of_pieces_to_seeders, received_pieces,
            file_received, torrent_file_path))

        thread1.start()

    last_received = 0

    while len(received_pieces) != num_of_pieces:

        if len(received_pieces) >= num_of_pieces // 2:
            # HALF PIECES RECEIVED CLIENT CAN START SEEDING NOW
            print("HALF PIECES DOWNLOADED SENDING SEED REQUEST TO TRACKER")
            share_with_tracker(destination_path, torrent_file.file_string, received_pieces)

        if len(received_pieces) == last_received:
            print("SENDING DOWNLOAD REQUEST FOR ALL THE PIECES TO SEEDERS")

            for piece in arranged_pieces:

                if piece in received_pieces:
                    continue

                get_piece_from_seeder(piece, map_of_connections, map_of_pieces_to_seeders, destination_path,

                                      received_pieces, file_received)

                print(f"PIECES RECEIVED TILL NOW : {len(received_pieces)}")

        last_received = len(received_pieces)
        print("SLEEPING FOR 45s BEFORE NEXT TRY")
        time.sleep(45)

    print("ALL PIECES WRITTEN TO FILE")
    print("SENDING DISCONNECT MESSAGE TO ALL CONNECTIONS")
    file_received = True
    # time.sleep(10)

    for seeder in map_of_connections:
        conn = map_of_connections[seeder]
        my_send(conn, pad_string(DISCONNECT_MESSAGE, 40).encode(), 40)

    print("ALL PIECES DOWNLOADED SENDING SEED REQUEST TO TRACKER")
    share_with_tracker(destination_path, torrent_file.file_string, received_pieces)
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
    """Sends the download request for the specified piece to the seeder"""
    random.shuffle(map_of_pieces_to_seeders[piece_no])
    # TODO: ADD LOGIC FOR CONTACTING 2ND SEEDER IF 1ST IS OFFLINE

    seeder_for_current_piece = map_of_pieces_to_seeders[piece_no][0]
    conn = map_of_connections[seeder_for_current_piece]
    print(f"REQUESTING PIECE {piece_no} FROM SEEDER {seeder_for_current_piece}")
    temp = pad_string(f"DOWNLOAD{SEPARATOR}{piece_no}", 40)
    print(temp)
    test_list.append((piece_no, conn.getpeername()[0]))
    my_send(conn, temp.encode(), len(temp))
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
                  map_of_pieces_to_seeders: dict, received_pieces: set, file_received: bool, torrent_file_path: str):
    last_piece_no = len(map_of_pieces_to_seeders) - 1

    with open(destination_path, mode="r+b") as file:
        while not file_received:
            invalid = False
            try:
                bytes_received = my_recv(conn, BUFFER_SIZE + HEADER_SIZE)
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
                elif header_received.rstrip() == f"{DISCONNECT_MESSAGE}":
                    print(f"DISCONNECT MESSAGE FROM SEEDER {conn.getpeername()} RECEIVED")
                    print(f"ALL PIECES FROM SEEDER {conn.getpeername()} RECEIVED")
                    break
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
            if piece_no == last_piece_no:
                torrent_file = SidTorrentFile(torrent_file_path)
                last_piece_size = torrent_file.file_size % 512000
                data_received = bytes_received[50:50 + last_piece_size]
            else:
                data_received = bytes_received[50:]

            print(f"RECEIVED PIECE NO. {piece_no}")
            print(f"PIECE: {piece_no} RECEIVED AND WRITTEN")
            file.seek(piece_no * BUFFER_SIZE, 0)
            print("*************************************")
            print(f"CALCULATION: {piece_no * BUFFER_SIZE}")
            print(f"WRITING {len(data_received)} BYTES TO FILE")
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
        my_send(conn, file_string.encode(), len(file_string))

    print(f"SEEDERS CONNECTED: {len(map_of_connections)} out of {len(seeder_list)} seeders")
    return map_of_connections


def get_pieces_info_from_seeders(seeder_list: list, file_string: str, map_of_connections: dict):
    map_of_pieces_to_seeders = {}

    for seeder in seeder_list:
        conn = map_of_connections[seeder]

        my_send(conn, pad_string("PIECES", 10).encode(), 10)

        # RECEIVE PIECES INFO
        pieces_available = my_recv(conn, BUFFER_SIZE).decode().rstrip().split(SEPARATOR)
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
        my_send(conn, pad_string(pieces_string, BUFFER_SIZE).encode(), BUFFER_SIZE)
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
    file_required = my_recv(conn, 40).decode()
    print(f"REQUESTED FILE {file_required}")
    request_type = my_recv(conn, 10).decode()
    request_type = request_type.rstrip()

    print(f"REQUEST TYPE: {request_type}")
    if request_type == "PIECES":
        send_pieces_info(conn, other_client_addr, file_required)

    # WAIT FOR DOWNLOAD REQUEST
    print(f"WAITING FOR DOWNLOAD REQUEST FROM {other_client_addr}")
    while True:
        download_req = my_recv(conn, 40).decode().rstrip()
        if download_req == DISCONNECT_MESSAGE:
            print(f"DISCONNECT REQUEST FROM {other_client_addr}")
            my_send(conn, pad_string(DISCONNECT_MESSAGE, BUFFER_SIZE + HEADER_SIZE).encode(), BUFFER_SIZE + HEADER_SIZE)
            time.sleep(10)
            print(f"CLOSING CONNECTION WITH CLIENT {conn.getpeername()}")

            conn.close()

            break

        print(download_req)
        if download_req.split(SEPARATOR)[0] == "DOWNLOAD":
            try:
                piece_requested = int(download_req.split(SEPARATOR)[1])
            except ValueError:
                continue

            # print(f"DOWNLOAD REQUEST FROM {other_client_addr} PIECE REQUESTED: {piece_requested}")
            # thread = threading.Thread(target=handle_download_request,
            #                           args=(conn, piece_requested, file_required))
            # thread.start()
            handle_download_request(conn, piece_requested, file_required)
            print(f"DOWNLOAD REQUEST FOR PIECE {piece_requested} HANDLED")
    print(f"THREAD CORRESPONDING TO CONNECTION WITH CLIENT {other_client_addr}  CLOSED")


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
        print(f"READ LENGTH : {len(bytes_read)}")
        # Condition to handle last piece
        header_with_data = header_string + bytes_read

        if len(bytes_read) != BUFFER_SIZE:
            padded_spaces = " " * (BUFFER_SIZE - len(bytes_read))
            padded_spaces = padded_spaces.encode()
            header_with_data += padded_spaces

        my_send(conn, header_with_data, len(header_with_data))
        print(f"REQUIRED PIECE ({piece_requested}) SENT")
        print(f"SENT DATA LENGTH: {len(header_with_data)}")


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
# share_with_tracker(other_file_path)

# torrent_file_path = input("Enter torrent file path: ")
# get_seeder_list_from_tracker(torrent_file_path)
# listen_for_connections()
# # # #
torrent_file_path = input("Enter torrent file path: ")

download_file_from_seeders(torrent_file_path)
# print(__file__)
# print("poklsadkapsokd")
# print(test_list)
# Path(root_dir / 'currently_seeding').mkdir()
