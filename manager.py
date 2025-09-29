# manager.py
# Rishith Cheduluri (1225443687) and Sebastian Bejaoui (122)
import socket
import sys
# For DSS
import threading
import json

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
        elif command == "register-disk":
            return self.register_disk(parts[1:])
        elif command == "deregister-user":
            return self.deregister_user(parts[1:])
        elif command == "deregister-disk":
            return self.deregister_disk(parts[1:])
        elif command == "configure-dss":
            return self.configure_dss(parts[1:])
        else:
            return "FAILURE|Unknown command"
    
    def register_user(self, params):
        """Handle register-user command"""
        if len(params) != 4:
            return "FAILURE|Invalid parameters"
        
        username, ip, m_port, c_port = params
        
        # Checking if the user already exists
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
    
    def register_disk(self, params):
        """Handle register-disk command"""
        if len(params) != 4:
            return "FAILURE|Invalid parameters"
        
        diskname, ip, m_port, c_port = params
        
        # Checking ifthe disk already exists
        if diskname in self.disks:
            return "FAILURE|Disk already registered"
        
        # Storing the disk info
        self.disks[diskname] = {
            'ip': ip,
            'm_port': int(m_port),
            'c_port': int(c_port),
            'status': 'Free'
        }
        
        print(f"Disk {diskname} registered")
        return "SUCCESS"
    
    def configure_dss(self, params):
        """Handle configure-dss command"""
        if len(params) != 3:
            return "FAILURE|Invalid parameters"
        
        dss_name, n, striping_unit = params
        n = int(n)
        striping_unit = int(striping_unit)
        
        # Validating
        if n < 3:
            return "FAILURE|n must be >= 3"
        
        if dss_name in self.dsss:
            return "FAILURE|DSS already exists"
        
        # Checking if theres enough free disks
        free_disks = [name for name, info in self.disks.items() if info['status'] == 'Free']
        if len(free_disks) < n:
            return "FAILURE|Not enough free disks"
        
        # Checking if the striping unit is power of 2 and in range
        if not (128 <= striping_unit <= 1048576 and (striping_unit & (striping_unit - 1)) == 0):
            return "FAILURE|Invalid striping unit"
        
        # Selecting 'n' disks randomly
        import random
        selected_disks = random.sample(free_disks, n)
        
        # Updating the disk status
        for disk_name in selected_disks:
            self.disks[disk_name]['status'] = 'InDSS'
        
        # Creating the DSS
        self.dsss[dss_name] = {
            'disks': selected_disks,
            'n': n,
            'striping_unit': striping_unit,
            'files': {}
        }
        
        print(f"DSS {dss_name} configured with disks: {selected_disks}")
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
    
    def deregister_disk(self, params):
        """Handle deregister-disk command"""
        if len(params) != 1:
            return "FAILURE|Invalid parameters"
        
        diskname = params[0]
        if diskname not in self.disks:
            return "FAILURE|Disk not found"
        
        if self.disks[diskname]['status'] == 'InDSS':
            return "FAILURE|Disk is in use"
        
        del self.disks[diskname]
        print(f"Disk {diskname} deregistered")
        return "SUCCESS"

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python manager.py <port>")
        sys.exit(1)
    
    port = int(sys.argv[1])
    manager = DSSManager(port)
    manager.run()