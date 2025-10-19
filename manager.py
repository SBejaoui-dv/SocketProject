# manager.py
# Rishith Cheduluri (1225443687) and Sebastian Bejaoui (122)
import socket
import sys
# For DSS
import threading
import json
import random
from collections import defaultdict

class DSSManager:
    def __init__(self, port):
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(('', port))
        
        # Adding state storage
        self.users = {}  # of the format {username: {ip, m_port, c_port}}
        self.disks = {}  # of the format {diskname: {ip, m_port, c_port, status}}
        self.dsss = {}   # of the format {dss_name: {disks, n, striping_unit, files}}
        
        self.lock = threading.Lock()
        self.critical_section = None  # Tracking which DSS is in critical operation
        self.read_operations = defaultdict(set)  # of the format {dss_name: {users reading}}
        self.pending_copy = {}  # of the format {user_name: {dss_name, file_name, file_size, owner}}
        self.pending_failure = {}  # of the format {user_name: dss_name}

        print(f"Manager started on port {port}")

        listener = threading.Thread(target=self.run)
        listener.daemon = True
        listener.start()
    
    def run(self):
        """Main server loop"""
        while True:
            try:
                data, addr = self.socket.recvfrom(2048)
                message = data.decode('utf-8')
                print(f"[MANAGER] Received: {message} from {addr}")
                
                response = self.process_message(message, addr)
                self.socket.sendto(response.encode('utf-8'), addr)
                
            except Exception as e:
                print(f"[MANAGER ERROR] {e}")
    
    def process_message(self, message, addr):
        """Process incoming messages and enforce critical sections"""
        try:
            parts = message.split('|')
            command = parts[0]
            
            critical_commands = ['copy', 'disk-failure', 'decommission-dss']
            
            # Checking if we're in criticla section for wrong DSS
            if command in critical_commands and len(parts) > 1:
                dss_name = parts[1]
                with self.lock:
                    if self.critical_section and self.critical_section != dss_name:
                        return "FAILURE|DSS in critical operation"
            
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
            elif command == "ls":
                return self.handle_ls()
            elif command == "copy":
                return self.handle_copy_phase1(parts[1:])
            elif command == "copy-complete":
                return self.handle_copy_phase2(parts[1:])
            elif command == "read":
                return self.handle_read_phase1(parts[1:])
            elif command == "read-complete":
                return self.handle_read_complete(parts[1:])
            elif command == "disk-failure":
                return self.handle_disk_failure_phase1(parts[1:])
            elif command == "recovery-complete":
                return self.handle_recovery_complete(parts[1:])
            elif command == "decommission-dss":
                return self.handle_decommission_phase1(parts[1:])
            elif command == "decommission-complete":
                return self.handle_decommission_phase2(parts[1:])
            else:
                return "FAILURE|Unknown command"
        except Exception as e:
            return f"FAILURE|Error processing message: {str(e)}"
    
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
        
        print(f"[MANAGER] User {username} registered")
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
        
        print(f"[MANAGER] Disk {diskname} registered")
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
        selected_disks = random.sample(free_disks, n)
        
        # Updating disk status
        with self.lock:
            for disk_name in selected_disks:
                self.disks[disk_name]['status'] = 'InDSS'
            
            self.dsss[dss_name] = {
                'disks': selected_disks,
                'n': n,
                'striping_unit': striping_unit,
                'files': {}
            }
        
        print(f"[MANAGER] DSS {dss_name} configured with disks: {selected_disks}")
        return "SUCCESS"
    
    def handle_ls(self):
        """Handle ls command - list all files"""
        if not self.dsss:
            return "FAILURE|No DSSs configured"
        
        with self.lock:
            response = "SUCCESS"
            for dss_name, dss_info in self.dsss.items():
                response += f"|DSS:{dss_name}|n={dss_info['n']}"
                response += f"|striping_unit={dss_info['striping_unit']}"
                response += f"|disks={','.join(dss_info['disks'])}"
                
                if dss_info['files']:
                    for file_name, file_info in dss_info['files'].items():
                        response += f"|FILE:{file_name}|size={file_info['size']}"
                        response += f"|owner={file_info['owner']}"
                else:
                    response += "|FILES:none"
        
        return response
    
    def handle_copy_phase1(self, params):
        """Phase 1: User requests to copy file - return DSS parameters"""
        if len(params) != 3:
            return "FAILURE|Invalid parameters"
        
        file_name, file_size, owner = params
        file_size = int(file_size)
        
        if not self.dsss:
            return "FAILURE|No DSSs configured"
        
        with self.lock:
            # Preventing operatinos during critical section
            if self.critical_section:
                return "FAILURE|Critical operation in progress"
            
            # Selecting a random DSS
            dss_name = random.choice(list(self.dsss.keys()))
            dss = self.dsss[dss_name]
            
            # Entering hte critical section for this DSS
            self.critical_section = dss_name
            
            # Tracking pending copy
            self.pending_copy[owner] = {
                'dss_name': dss_name,
                'file_name': file_name,
                'file_size': file_size,
                'owner': owner
            }
        
        # Building a response
        response = f"SUCCESS|{dss_name}|{dss['n']}|{dss['striping_unit']}"
        for disk_name in dss['disks']:
            disk = self.disks[disk_name]
            response += f"|{disk_name}|{disk['ip']}|{disk['c_port']}"
        
        print(f"[MANAGER] Copy phase 1: {owner} -> {file_name} on {dss_name}")
        return response
    
    def handle_copy_phase2(self, params):
        """Phase 2: User confirms copy complete - update state"""
        if len(params) != 1:
            return "FAILURE|Invalid parameters"
        
        user_name = params[0]
        
        if user_name not in self.pending_copy:
            return "FAILURE|No pending copy for user"
        
        with self.lock:
            copy_info = self.pending_copy[user_name]
            dss_name = copy_info['dss_name']
            
            if dss_name not in self.dsss:
                return "FAILURE|DSS not found"
            
            # Updating the DSS file list
            self.dsss[dss_name]['files'][copy_info['file_name']] = {
                'size': copy_info['file_size'],
                'owner': copy_info['owner']
            }
            
            # Cleaning up and exiting the critical section
            del self.pending_copy[user_name]
            self.critical_section = None
        
        print(f"[MANAGER] Copy phase 2 complete: {copy_info['file_name']} stored")
        return "SUCCESS"
    
    def handle_read_phase1(self, params):
        """Phase 1: User requests to read file - validate and return DSS params"""
        if len(params) != 3:
            return "FAILURE|Invalid parameters"
        
        dss_name, file_name, user_name = params
        
        if dss_name not in self.dsss:
            return "FAILURE|DSS not found"
        
        if file_name not in self.dsss[dss_name]['files']:
            return "FAILURE|File not found"
        
        file_info = self.dsss[dss_name]['files'][file_name]
        if file_info['owner'] != user_name:
            return "FAILURE|Not file owner"
        
        with self.lock:
            # Checking for critical section
            if self.critical_section and self.critical_section != dss_name:
                return "FAILURE|DSS in critical operation"
            
            # Tracking the read operation
            self.read_operations[dss_name].add(user_name)
        
        # Building response with the DSS parameters
        dss = self.dsss[dss_name]
        response = f"SUCCESS|{dss['n']}|{dss['striping_unit']}|{file_info['size']}"
        for disk_name in dss['disks']:
            disk = self.disks[disk_name]
            response += f"|{disk_name}|{disk['ip']}|{disk['c_port']}"
        
        print(f"[MANAGER] Read phase 1: {user_name} reading {file_name} from {dss_name}")
        return response
    
    def handle_read_complete(self, params):
        """Phase 2: User completes read - clean up tracking"""
        if len(params) != 2:
            return "FAILURE|Invalid parameters"
        
        user_name, dss_name = params
        
        with self.lock:
            if dss_name in self.read_operations:
                self.read_operations[dss_name].discard(user_name)
        
        print(f"[MANAGER] Read complete: {user_name} on {dss_name}")
        return "SUCCESS"
    
    def handle_disk_failure_phase1(self, params):
        """Phase 1: User triggers disk failure - return DSS params"""
        if len(params) != 1:
            return "FAILURE|Invalid parameters"
        
        dss_name = params[0]
        
        if dss_name not in self.dsss:
            return "FAILURE|DSS not found"
        
        with self.lock:
            # Checking for the read operations in progress
            if dss_name in self.read_operations and len(self.read_operations[dss_name]) > 0:
                return "FAILURE|Read operations in progress"
            
            # Entering hte critical section
            self.critical_section = dss_name
            self.pending_failure[dss_name] = True
        
        # Returning the DSS parameters
        dss = self.dsss[dss_name]
        response = f"SUCCESS|{dss['n']}|{dss['striping_unit']}"
        for disk_name in dss['disks']:
            disk = self.disks[disk_name]
            response += f"|{disk_name}|{disk['ip']}|{disk['c_port']}"
        
        print(f"[MANAGER] Disk failure phase 1: {dss_name}")
        return response
    
    def handle_recovery_complete(self, params):
        """Phase 2: User completes recovery - update state"""
        if len(params) != 1:
            return "FAILURE|Invalid parameters"
        
        dss_name = params[0]
        
        if dss_name not in self.pending_failure:
            return "FAILURE|No pending failure for DSS"
        
        with self.lock:
            del self.pending_failure[dss_name]
            self.critical_section = None
        
        print(f"[MANAGER] Recovery complete: {dss_name}")
        return "SUCCESS"
    
    def handle_decommission_phase1(self, params):
        """Phase 1: User initiates decommission - enter critical section"""
        if len(params) != 1:
            return "FAILURE|Invalid parameters"
        
        dss_name = params[0]
        
        if dss_name not in self.dsss:
            return "FAILURE|DSS not found"
        
        with self.lock:
            # Entering hte critical section
            self.critical_section = dss_name
        
        # Returning the DSS parameters
        dss = self.dsss[dss_name]
        response = f"SUCCESS|{dss['n']}|{dss['striping_unit']}"
        for disk_name in dss['disks']:
            disk = self.disks[disk_name]
            response += f"|{disk_name}|{disk['ip']}|{disk['c_port']}"
        
        print(f"[MANAGER] Decommission phase 1: {dss_name}")
        return response
    
    def handle_decommission_phase2(self, params):
        """Phase 2: User completes decommission - cleanup and release disks"""
        if len(params) != 1:
            return "FAILURE|Invalid parameters"
        
        dss_name = params[0]
        
        if dss_name not in self.dsss:
            return "FAILURE|DSS not found"
        
        with self.lock:
            dss = self.dsss[dss_name]
            
            # Releasing all the disks back to Free status
            for disk_name in dss['disks']:
                self.disks[disk_name]['status'] = 'Free'
            
            # Removing the DSS
            del self.dsss[dss_name]
            
            # Exiting the critical section
            self.critical_section = None
        
        print(f"[MANAGER] Decommission complete: {dss_name}")
        return "SUCCESS"
    
    def deregister_user(self, params):
        """Handle deregister-user command"""
        if len(params) != 1:
            return "FAILURE|Invalid parameters"
        
        username = params[0]
        if username not in self.users:
            return "FAILURE|User not found"
        
        del self.users[username]
        print(f"[MANAGER] User {username} deregistered")
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
        print(f"[MANAGER] Disk {diskname} deregistered")
        return "SUCCESS"

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python manager.py <port>")
        sys.exit(1)
    
    port = int(sys.argv[1])
    manager = DSSManager(port)
    
    # Keep the manager running
    try:
        while True:
            pass
    except KeyboardInterrupt:
        print("\n[MANAGER] Shutting down...")