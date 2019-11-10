# socket_echo_server.py
import socket
import sys
import json
import threading
import time
import socket
import argparse

class LocalFaultDetector:

    def __init__(self, gfd_address, gfd_hb_interval=1, lfd_port=10000):
        self.replica_thread = threading.Thread(target=self.replica_thread_func)
        self.gfd_heartbeat_thread = threading.Thread(target=self.gfd_heartbeat_thread_func)
        self.gfd_membership_thread = threading.Thread(target=self.gfd_membership_thread_func)
        self.lfd_port = lfd_port
        self.gfd_address = (gfd_address, 12345)
        self.gfd_hb_interval = gfd_hb_interval
        self.replica_isAlive = False
        self.replica_isAlive_lock = threading.Lock()

        

        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        self.host_ip = s.getsockname()[0]

        self.client_address = self.host_ip

        self.rp_membership = json.dumps({}).encode("UTF-8") # init as empty string
        self.rp_membership_lock = threading.Lock()

        self.establish_gfd_connection()
        
        print("Replica thread start")
        self.replica_thread.start()
        time.sleep(5)
        print("GFD heartbeat thread start")
        self.gfd_heartbeat_thread.start()
        #print("GFD membership thread start")
        #self.gfd_membership_thread.start()
    
    def establish_gfd_connection(self):
        try:
            # Create a TCP/IP socket
            self.gfd_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            # Bind the socket to the replication port
            server_address = self.gfd_address
            print('Connecting to gfd_address {} port {}'.format(*server_address))
            self.gfd_conn.connect(server_address)
        except Exception as e:
            print("Cannot connect to GFD")
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
                    
                    LFD_heartbeat_msg = json.dumps(data).encode("UTF-8")
                    self.gfd_conn.sendall(LFD_heartbeat_msg)
                    time.sleep(self.gfd_hb_interval)

            finally:
                # GFD connection errors
                print("gfd connection lost")
                # Clean up the connection
                self.gfd_conn.close()

    def gfd_membership_thread_func(self):
        
        while True:
            try:
                # Waiting for replica heart beat
                while True:
                    data = self.gfd_conn.recv(1024)
                    with self.rp_membership_lock:
                        #Membership update here
                        self.rp_membership = data
                
                    print('received membership msg:{!r}'.format(data))

            finally:
                # GFD connection errors
                print("gfd connection lost")
                # Clean up the connection
                self.gfd_conn.close()


    def replica_thread_func(self):
        # Create a TCP/IP socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Bind the socket to the replication port
        # host_name = socket.gethostname() 
        # host_ip = socket.gethostbyname(host_name)

        server_address = (self.host_ip, self.lfd_port)
        print('Starting listening on replica {} port {}'.format(*server_address))
        sock.bind(server_address)
        
        # Listen for incoming connections
        sock.listen(1)
        while True:
            print("Accepting replica")
            connection, self.client_address = sock.accept()
            try:
                print('connection from', self.client_address)
                count = 0

                # Waiting for replica heart beat
                while True:
                    try:
                        connection.settimeout(2)
                        data = connection.recv(1024)
                        connection.settimeout(None)
                    except socket.timeout:
                        print("Receive timeout")
                        with self.replica_isAlive_lock:
                            self.replica_isAlive = False
                        connection.close()
                        break
                    
                    print('Received heartbeat from Replica at: {} | Heartbeat count: {}'.format(self.client_address, count))
                    count = count + 1
                    with self.replica_isAlive_lock:
                        self.replica_isAlive = True

                    if data:
                        pass
                        # TODO: Send membership data to replica
                        # Dummy membership json data
                        # print("Sending membership json file to replica server")
                        # with self.rp_membership_lock:
                        #     membership_json = self.rp_membership
                        #     #membership_json = b"This is a member ship json file"
                        # connection.sendall(membership_json)
                    else:
                        print('no data from', self.client_address)
                        with self.replica_isAlive_lock:
                            self.replica_isAlive = False
                            print("replica connection lost")
                        connection.close()
                        break

            except Exception as e:
                # Anything fails, ie: replica server fails
                print("Replica connection lost")
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

