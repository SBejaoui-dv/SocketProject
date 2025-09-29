# user.py
# Rishith Cheduluri (1225443687) and Sebastian Bejaoui (122)
import socket
import sys

class DSSUser:
    def __init__(self, username, manager_ip, manager_port, m_port, c_port):
        self.username = username
        self.manager_ip = manager_ip
        self.manager_port = manager_port
        self.m_port = m_port
        self.c_port = c_port
        
        # Creating sockets
        self.m_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.m_socket.bind(('', m_port))
        
        self.c_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.c_socket.bind(('', c_port))
        
        print(f"User {username} started on ports {m_port}, {c_port}")
        
        # Registering with hte manager
        self.register()
    
    def register(self):
        """Register with the manager"""
        message = f"register-user|{self.username}|127.0.0.1|{self.m_port}|{self.c_port}"
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(message.encode('utf-8'), (self.manager_ip, self.manager_port))
        
        response, _ = sock.recvfrom(1024)
        print(f"Registration response: {response.decode('utf-8')}")
        sock.close()
    
    def run(self):
        """Interactive command loop"""
        print("Available commands: configure-dss, deregister-user, quit")
        
        while True:
            try:
                cmd = input(f"{self.username}> ").strip()
                print("Command processing not implemented yet")
                    
            except KeyboardInterrupt:
                break

if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Usage: python user.py <username> <manager_ip> <manager_port> <m_port> <c_port>")
        sys.exit(1)
    
    username = sys.argv[1]
    manager_ip = sys.argv[2]
    manager_port = int(sys.argv[3])
    m_port = int(sys.argv[4])
    c_port = int(sys.argv[5])
    
    user = DSSUser(username, manager_ip, manager_port, m_port, c_port)
    user.run()