# sidtorrent

This a peer to peer (p2p) file sharing system based
on the torrent system.


Instructions:-

1) First start the tracker by using command 
    "py tracker.py",
   now the tracker/server is on listen mode on the local ip of the machine.
   
2) Now start the seeder by using the command
"py seeder-client.py TRACKER_IP",
TRACKER_IP is the local IP of the machine on which the tracker is running (For LANs)

To use this system on public networks, TRACKER_IP should be the public IP of the tracker machine, and port forwarding must
be enabled on the tracker side as well as the seeder side.

PORTS used for communication (Use port forwarding on these ports)  
For Tracker 5050
For Seeder 10023  
