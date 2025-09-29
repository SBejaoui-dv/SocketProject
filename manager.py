# manager.py
# Rishith Cheduluri (1225443687) and Sebastian Bejaoui (122)
import socket
import sys

class DSSManager:
    def __init__(self, port):
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(('', port))
        
        # Adding state storage
        self.users = {}  # of the format {username: {ip, m_port, c_port}}
        self.disks = {}  # of the format {diskname: {ip, m_port, c_port, status}}
        self.dsss = {}   # of the format {dss_name: {disks, striping_unit, files}}
        
        print(f"Manager started on port {port}")
    
    def run(self):
        """Main server loop"""
        while True:
            try:
                data, addr = self.socket.recvfrom(1024)
                message = data.decode('utf-8')
                print(f"Received: {message} from {addr}")
                
                response = self.process_message(message)
                self.socket.sendto(response.encode('utf-8'), addr)
                
            except Exception as e:
                print(f"Error: {e}")
    
    def process_message(self, message):
        """Process incoming messages"""
        parts = message.split('|')
        command = parts[0]
        
        if command == "register-user":
            return self.register_user(parts[1:])
        elif command == "deregister-user":
            return self.deregister_user(parts[1:])
        else:
            return "FAILURE|Unknown command"
    
    def register_user(self, params):
        """Handle register-user command"""
        if len(params) != 4:
            return "FAILURE|Invalid parameters"
        
        username, ip, m_port, c_port = params
        
        # Checking if teh user already exists
        if username in self.users:
            return "FAILURE|User already registered"


        # Storing the user info
        self.users[username] = {
            'ip': ip,
            'm_port': int(m_port),
            'c_port': int(c_port)
        }
        
        print(f"User {username} registered")
        return "SUCCESS"
    
    def deregister_user(self, params):
        """Handle deregister-user command"""
        if len(params) != 1:
            return "FAILURE|Invalid parameters"
        
        username = params[0]
        if username not in self.users:
            return "FAILURE|User not found"
        
        del self.users[username]
        print(f"User {username} deregistered")
        return "SUCCESS"

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python manager.py <port>")
        sys.exit(1)
    
    port = int(sys.argv[1])
    manager = DSSManager(port)
    manager.run()