import json
import hashlib


class SidTorrentFile:

    def __init__(self, torrent_file_path):
        self.file_path = torrent_file_path
        try:
            with open(self.file_path, mode='r') as file:
                torrent_file = json.load(file)
        except FileNotFoundError:
            pass

        self.tracker_url = (torrent_file['tracker_url']['tracker_ip'], torrent_file['tracker_url']['tracker_port'])
        self.hash_string = torrent_file['hash_string']
        self.file_name = torrent_file['file_name']
        self.pieces = len(torrent_file["hash_string"]) / 40
        self.file_string = hashlib.sha1(self.hash_string.encode()).hexdigest()
        self.file_size = torrent_file['file_size']
