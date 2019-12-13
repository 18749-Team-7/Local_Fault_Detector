# socket_echo_server.py
import socket
import sys
import json
import threading
import time
import socket
import argparse

BLACK =     "\u001b[30m"
RED =       "\u001b[31m"
GREEN =     "\u001b[32m"
YELLOW =    "\u001b[33m"
BLUE =      "\u001b[34m"
MAGENTA =   "\u001b[35m"
CYAN =      "\u001b[36m"
WHITE =     "\u001b[37m"
RESET =     "\u001b[0m"

class LocalFaultDetector:

    def __init__(self, gfd_address):
        self.replica_thread = threading.Thread(target=self.replica_thread_func)
        self.gfd_heartbeat_thread = threading.Thread(target=self.gfd_heartbeat_thread_func)
        self.lfd_port = lfd_port
        self.gfd_address = (gfd_address, 12345)
        self.gfd_hb_interval = 1
        self.replica_isAlive = False
        self.replica_isAlive_lock = threading.Lock()

        

        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.connect(("8.8.8.8", 80))
        self.host_ip = s.getsockname()[0]

        self.client_address = self.host_ip

        self.rp_membership = json.dumps({}).encode("UTF-8") # init as empty string
        self.rp_membership_lock = threading.Lock()

        self.establish_gfd_connection()
        
        self.replica_thread.start()
        time.sleep(5)
        print(RED + "Starting GFD heartbeat..." + RESET)
        self.gfd_heartbeat_thread.start()
    
    def establish_gfd_connection(self):
        try:
            # Create a TCP/IP socket
            self.gfd_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.gfd_conn.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            # Bind the socket to the replication port
            server_address = self.gfd_address
            print(RED + 'Connecting to gfd_address {} port {}'.format(*server_address) + RESET)
            self.gfd_conn.connect(server_address)
        except Exception as e:
            print(RED + "Cannot connect to GFD" + RESET)
            print(e)


    def gfd_heartbeat_thread_func(self):
        
        while True:
            try:
                # Waiting for GFD membership data
                while True:
                    ip_addr = self.client_address[0]
                    data = {}
                    data['server_ip'] = ip_addr
                    with self.replica_isAlive_lock:
                        data['status'] = self.replica_isAlive
                    data['time'] = self.gfd_hb_interval
                    
                    LFD_heartbeat_msg = json.dumps(data).encode("UTF-8")
                    self.gfd_conn.sendall(LFD_heartbeat_msg)
                    time.sleep(self.gfd_hb_interval)

            finally:
                # GFD connection errors
                print(RED + "GFD connection is lost" + RESET)
                # Clean up the connection
                self.gfd_conn.close()


    def replica_thread_func(self):
        # Create a TCP/IP socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # Bind the socket to the replication port
        # host_name = socket.gethostname() 
        # host_ip = socket.gethostbyname(host_name)

        server_address = (self.host_ip, self.lfd_port)
        print(RED + 'Starting listening on replica {} port {}'.format(*server_address) + RESET)
        sock.bind(server_address)
        
        # Listen for incoming connections
        sock.listen(1)
        while True:
            print(RED + "Waiting for replica to connect" + RESET)
            connection, self.client_address = sock.accept()
            try:
                print(RED + 'connection from {}'.format(self.client_address) + RESET)
                count = 0

                # Waiting for replica heart beat
                while True:
                    try:
                        # connection.settimeout(5)
                        data = connection.recv(1024)
                        # connection.settimeout(None)

                    except socket.timeout:
                        print(RED + "Received timeout from Replica {}".format(self.client_address))
                        with self.replica_isAlive_lock:
                            self.replica_isAlive = False
                        # connection.close()
                        continue
                    
                    print(BLUE + 'Received heartbeat from Replica at: {} | Heartbeat count: {}'.format(self.client_address, count) + RESET)
                    count = count + 1
                    with self.replica_isAlive_lock:
                        self.replica_isAlive = True

                    if data:
                        data = data.decode("utf-8")
                        data = json.loads(data)
                        self.gfd_hb_interval = data["time"]
                    else:
                        print(RED + 'No data from {}'.format(self.client_address) + RESET)
                        with self.replica_isAlive_lock:
                            self.replica_isAlive = False
                            print(RED + "replica connection lost" + RESET)
                        connection.close()
                        break

            except Exception as e:
                # Anything fails, ie: replica server fails
                print(RED + "Replica connection lost" + RESET)
                # Clean up the connection
                connection.close()

def get_args():
    parser = argparse.ArgumentParser()

    # IP, PORT, Username
    parser.add_argument('-ip', '--ip', help="Global fault detector IP Address", required=True)
    
    # Parse the arguments
    args = parser.parse_args()
    return args

if __name__=="__main__":
    # Extract Arguments from the 
    args = get_args()

    lfd = LocalFaultDetector(gfd_address=args.ip)

