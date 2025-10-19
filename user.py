# user.py
# Rishith Cheduluri (1225443687) and Sebastian Bejaoui (122)
import socket
import sys
import threading
import os
import subprocess
import random

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
        
        # Make c_socket non-blocking for receiving blocks
        self.c_socket.setblocking(False)
        
        print(f"[USER {username}] Started on ports {m_port}, {c_port}")
        
        # Start listener thread for peer-to-peer messages
        self.start_listeners()
        
       # Registering with the manager
        self.register()
    
    def start_listeners(self):
        """Start listener thread for P2P messages on c_port"""
        c_thread = threading.Thread(target=self.listen_c_port, daemon=True)
        c_thread.start()
    
    def listen_c_port(self):
        """Listen for peer messages (blocks during read/copy)"""
        while True:
            try:
                data, addr = self.c_socket.recvfrom(65536)
                message = data.decode('utf-8', errors='ignore')
                print(f"[USER {self.username}] C-port received: {message[:50]}...")
            except:
                # Non-blocking socket will raise exception when no data
                pass
    
     def register(self):
        """Register with the manager"""
        message = f"register-user|{self.username}|127.0.0.1|{self.m_port}|{self.c_port}"
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(message.encode('utf-8'), (self.manager_ip, self.manager_port))
        response, _ = sock.recvfrom(1024)
        print(f"[USER {self.username}] Registration: {response.decode('utf-8')}")
        sock.close()
    
    def send_to_manager(self, command):
        """Send command to manager and receive response"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(command.encode('utf-8'), (self.manager_ip, self.manager_port))
        
        response, _ = sock.recvfrom(4096)
        sock.close()
        return response.decode('utf-8')
    
    def send_to_peer(self, peer_ip, peer_port, message):
        """Send message to a peer (disk process)"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(message.encode('utf-8'), (peer_ip, peer_port))
        sock.close()
    
    def handle_ls(self):
        """Handle ls command"""
        response = self.send_to_manager("ls")
        print(f"\n[USER {self.username}] File Listing:")
        if response.startswith("SUCCESS"):
            parts = response.split('|')
            i = 1
            while i < len(parts):
                if parts[i].startswith("DSS:"):
                    dss_name = parts[i].replace("DSS:", "")
                    print(f"\n{dss_name}:")
                    i += 1
                    while i < len(parts) and not parts[i].startswith("DSS:"):
                        if parts[i].startswith("FILE:"):
                            file_name = parts[i].replace("FILE:", "")
                            size = parts[i+1].split('=')[1]
                            owner = parts[i+2].split('=')[1]
                            print(f"  {file_name} ({size} bytes) - Owner: {owner}")
                            i += 3
                        else:
                            print(f"  {parts[i]}")
                            i += 1
                else:
                    i += 1
        else:
            print(f"Error: {response}")
    
    def handle_configure_dss(self, dss_name, n, striping_unit):
        """Handle configure-dss command"""
        command = f"configure-dss|{dss_name}|{n}|{striping_unit}"
        response = self.send_to_manager(command)
        print(f"[USER {self.username}] Configure DSS: {response}")
    
    def handle_copy(self, file_path):
        """Handle copy command - two phase operation"""
        if not os.path.exists(file_path):
            print(f"[USER {self.username}] File not found: {file_path}")
            return
        
        file_name = os.path.basename(file_path)
        file_size = os.path.getsize(file_path)
        
        # Phase 1: Get DSS parameters
        command = f"copy|{file_name}|{file_size}|{self.username}"
        response = self.send_to_manager(command)
        
        if response.startswith("FAILURE"):
            print(f"[USER {self.username}] Copy failed: {response}")
            return
        
        # Parse DSS parameters
        parts = response.split('|')
        dss_name = parts[1]
        n = int(parts[2])
        striping_unit = int(parts[3])
        
        # Extract disk triples
        disk_triples = []
        for i in range(n):
            idx = 4 + i * 3
            disk_name = parts[idx]
            disk_ip = parts[idx + 1]
            disk_port = int(parts[idx + 2])
            disk_triples.append((disk_name, disk_ip, disk_port))
        
        print(f"[USER {self.username}] Copy phase 1: {file_name} -> {dss_name}")
        
        # Phase 2: Actually copy file
        self.copy_file_to_dss(file_path, dss_name, n, striping_unit, disk_triples)
        
        # Phase 3: Notify manager copy is complete
        complete_cmd = f"copy-complete|{self.username}"
        response = self.send_to_manager(complete_cmd)
        print(f"[USER {self.username}] Copy phase 2: {response}")
    
    def copy_file_to_dss(self, file_path, dss_name, n, striping_unit, disk_triples):
        """Read file and stripe it across disks with parity"""
        print(f"[USER {self.username}] Striping {file_path} across {n} disks...")
        
        with open(file_path, 'rb') as f:
            stripe_num = 0
            while True:
                # Read n-1 data blocks
                data_blocks = []
                for i in range(n - 1):
                    block = f.read(striping_unit)
                    if not block and i == 0:
                        return  # EOF
                    if not block or len(block) < striping_unit:
                        # Pad with zeros
                        block = block.ljust(striping_unit, b'\x00')
                    data_blocks.append(block)
                
                # Compute parity block
                parity = self.compute_parity(data_blocks)
                
                # Determine which disk gets parity for this stripe
                parity_disk_idx = n - ((stripe_num % n) + 1)
                
                print(f"[USER {self.username}] Stripe {stripe_num}: parity on disk {parity_disk_idx}")
                
                # Write blocks in parallel
                threads = []
                for i in range(n):
                    if i == parity_disk_idx:
                        block_data = parity
                        block_type = 'parity'
                    else:
                        # Map data block index (skip parity disk)
                        data_idx = i if i < parity_disk_idx else i - 1
                        block_data = data_blocks[data_idx]
                        block_type = 'data'
                    
                    disk_name, disk_ip, disk_port = disk_triples[i]
                    t = threading.Thread(
                        target=self.write_block_to_disk,
                        args=(disk_name, disk_ip, disk_port, dss_name, file_path,
                              stripe_num, i, block_data, block_type)
                    )
                    threads.append(t)
                    t.start()
                
                # Wait for all writes
                for t in threads:
                    t.join()
                
                stripe_num += 1
    
    def write_block_to_disk(self, disk_name, disk_ip, disk_port, dss_name, 
                           file_name, stripe, block_idx, block_data, block_type):
        """Send a block to disk"""
        try:
            file_base = os.path.basename(file_name)
            # Format: WRITE_BLOCK|dss_name|file_name|stripe|block_idx|block_type
            # Block data follows as binary after delimiter
            header = f"WRITE_BLOCK|{dss_name}|{file_base}|{stripe}|{block_idx}|{block_type}|"
            
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            # Send header + binary data
            message = header.encode('utf-8') + block_data
            sock.sendto(message, (disk_ip, disk_port))
            
            # Wait for ACK
            sock.settimeout(2)
            response, _ = sock.recvfrom(1024)
            print(f"[USER {self.username}] Block {stripe}:{block_idx} written to {disk_name}")
            sock.close()
        except Exception as e:
            print(f"[USER {self.username}] Error writing block to {disk_name}: {e}")
    
    def compute_parity(self, data_blocks):
        """XOR all data blocks to compute parity"""
        parity = bytearray(len(data_blocks[0]))
        for block in data_blocks:
            for i in range(len(block)):
                parity[i] ^= block[i]
        return bytes(parity)
    
    def handle_read(self, dss_name, file_name):
        """Handle read command - two phase operation"""
        # Phase 1: Request file from manager
        command = f"read|{dss_name}|{file_name}|{self.username}"
        response = self.send_to_manager(command)
        
        if response.startswith("FAILURE"):
            print(f"[USER {self.username}] Read failed: {response}")
            return
        
        # Parse response
        parts = response.split('|')
        n = int(parts[1])
        striping_unit = int(parts[2])
        file_size = int(parts[3])
        
        # Extract disk triples
        disk_triples = []
        for i in range(n):
            idx = 4 + i * 3
            disk_name = parts[idx]
            disk_ip = parts[idx + 1]
            disk_port = int(parts[idx + 2])
            disk_triples.append((disk_name, disk_ip, disk_port))
        
        print(f"[USER {self.username}] Read phase 1: {file_name} from {dss_name}")
        
        # Phase 2: Read file from DSS
        self.read_file_from_dss(dss_name, file_name, file_size, n, striping_unit, disk_triples)
        
        # Phase 3: Notify manager read is complete
        complete_cmd = f"read-complete|{self.username}|{dss_name}"
        response = self.send_to_manager(complete_cmd)
        print(f"[USER {self.username}] Read complete: {response}")
        
        # Verify with diff
        if os.path.exists(file_name):
            recovered_file = f"{file_name}.recovered"
            try:
                result = subprocess.run(['diff', file_name, recovered_file], 
                                      capture_output=True, text=True)
                if result.returncode == 0:
                    print(f"[USER {self.username}] ✓ File verification PASSED")
                else:
                    print(f"[USER {self.username}] ✗ File verification FAILED")
            except Exception as e:
                print(f"[USER {self.username}] Could not verify: {e}")
    
    def read_file_from_dss(self, dss_name, file_name, file_size, n, striping_unit, disk_triples):
        """Read file from DSS with parity verification"""
        num_stripes = (file_size + (n-1)*striping_unit - 1) // ((n-1)*striping_unit)
        print(f"[USER {self.username}] Reading {num_stripes} stripes from DSS...")
        
        with open(f"{file_name}.recovered", 'wb') as out:
            for stripe in range(num_stripes):
                # Read all blocks of this stripe in parallel
                blocks = [None] * n
                threads = []
                
                for i in range(n):
                    disk_name, disk_ip, disk_port = disk_triples[i]
                    t = threading.Thread(
                        target=self.read_block_from_disk,
                        args=(disk_name, disk_ip, disk_port, dss_name, file_name,
                              stripe, i, blocks)
                    )
                    threads.append(t)
                    t.start()
                
                # Wait for all reads
                for t in threads:
                    t.join()
                
                # Introduce bit error with probability p
                p = 10  # 10% error rate for testing
                for i in range(n):
                    if blocks[i] and random.randint(0, 100) < p:
                        print(f"[USER {self.username}] Introducing error in stripe {stripe} block {i}")
                        block_arr = bytearray(blocks[i])
                        bit_pos = random.randint(0, len(block_arr) * 8 - 1)
                        byte_idx = bit_pos // 8
                        bit_idx = bit_pos % 8
                        block_arr[byte_idx] ^= (1 << bit_idx)
                        blocks[i] = bytes(block_arr)
                
                # Verify parity
                parity_disk_idx = n - ((stripe % n) + 1)
                data_blocks = [blocks[i] for i in range(n) if i != parity_disk_idx]
                
                computed_parity = self.compute_parity(data_blocks)
                if computed_parity == blocks[parity_disk_idx]:
                    print(f"[USER {self.username}] Stripe {stripe}: parity verified ✓")
                    # Write data blocks to output
                    for i in range(n):
                        if i != parity_disk_idx:
                            out.write(blocks[i])
                else:
                    print(f"[USER {self.username}] Stripe {stripe}: parity mismatch! Retrying...")
                    # Retry: read again
                    stripe -= 1  # Retry this stripe
    
    def read_block_from_disk(self, disk_name, disk_ip, disk_port, dss_name, 
                            file_name, stripe, block_idx, blocks_array):
        """Read a block from disk"""
        try:
            file_base = os.path.basename(file_name)
            msg = f"READ_BLOCK|{dss_name}|{file_base}|{stripe}|{block_idx}"
            
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.sendto(msg.encode('utf-8'), (disk_ip, disk_port))
            
            sock.settimeout(2)
            data, _ = sock.recvfrom(65536)
            blocks_array[block_idx] = data
            sock.close()
        except Exception as e:
            print(f"[USER {self.username}] Error reading from {disk_name}: {e}")
    
    def handle_disk_failure(self, dss_name):
        """Handle disk-failure command - two phase operation"""
        # Phase 1: Get DSS parameters
        command = f"disk-failure|{dss_name}"
        response = self.send_to_manager(command)
        
        if response.startswith("FAILURE"):
            print(f"[USER {self.username}] Disk failure failed: {response}")
            return
        
        # Parse response
        parts = response.split('|')
        n = int(parts[1])
        striping_unit = int(parts[2])
        
        # Extract disk triples
        disk_triples = []
        for i in range(n):
            idx = 3 + i * 3
            disk_name = parts[idx]
            disk_ip = parts[idx + 1]
            disk_port = int(parts[idx + 2])
            disk_triples.append((disk_name, disk_ip, disk_port))
        
        print(f"[USER {self.username}] Disk failure phase 1: {dss_name}")
        
        # Phase 2: Simulate failure and recover
        self.simulate_failure_and_recover(dss_name, n, striping_unit, disk_triples)
        
        # Phase 3: Notify manager recovery complete
        complete_cmd = f"recovery-complete|{dss_name}"
        response = self.send_to_manager(complete_cmd)
        print(f"[USER {self.username}] Recovery complete: {response}")
    
    def simulate_failure_and_recover(self, dss_name, n, striping_unit, disk_triples):
        """Simulate disk failure and recovery"""
        # Randomly select failed disk
        failed_disk_idx = random.randint(0, n - 1)
        failed_disk = disk_triples[failed_disk_idx]
        
        print(f"[USER {self.username}] Failing disk {failed_disk_idx}: {failed_disk[0]}")
        
        # Send fail message to failed disk
        fail_msg = f"FAIL|{dss_name}"
        self.send_to_peer(failed_disk[1], failed_disk[2], fail_msg)
        
        print(f"[USER {self.username}] Recovering data to disk {failed_disk_idx}...")
        
        # For each file, recover each stripe
        # This is simplified - in real impl would need to know which files exist
        # For now, just recover the structure
        for i in range(n):
            if i != failed_disk_idx:
                # Send recover command
                recover_msg = f"RECOVER|{dss_name}|{i}"
                disk = disk_triples[i]
                self.send_to_peer(disk[1], disk[2], recover_msg)
    
    def handle_decommission_dss(self, dss_name):
        """Handle decommission-dss command - two phase operation"""
        # Phase 1: Get DSS parameters
        command = f"decommission-dss|{dss_name}"
        response = self.send_to_manager(command)
        
        if response.startswith("FAILURE"):
            print(f"[USER {self.username}] Decommission failed: {response}")
            return
        
        # Parse response
        parts = response.split('|')
        n = int(parts[1])
        
        # Extract disk triples
        disk_triples = []
        for i in range(n):
            idx = 2 + i * 3
            disk_name = parts[idx]
            disk_ip = parts[idx + 1]
            disk_port = int(parts[idx + 2])
            disk_triples.append((disk_name, disk_ip, disk_port))
        
        print(f"[USER {self.username}] Decommission phase 1: {dss_name}")
        
        # Phase 2: Send fail to all disks to clear data
        for disk_name, disk_ip, disk_port in disk_triples:
            fail_msg = f"FAIL|{dss_name}"
            self.send_to_peer(disk_ip, disk_port, fail_msg)
        
        # Phase 3: Notify manager decommission is complete
        complete_cmd = f"decommission-complete|{dss_name}"
        response = self.send_to_manager(complete_cmd)
        print(f"[USER {self.username}] Decommission complete: {response}")
    
   def run(self):
        """Interactive command loop"""
        print(f"\n[USER {self.username}] Available commands:")
        print("  configure-dss <name> <n> <striping_unit>")
        print("  copy <file_path>")
        print("  read <dss_name> <file_name>")
        print("  ls")
        print("  disk-failure <dss_name>")
        print("  decommission-dss <dss_name>")
        print("  deregister-user")
        print("  quit\n")
        
       while True:
            try:
                cmd = input(f"{self.username}> ").strip()
                
                if not cmd:
                    continue
                
                if cmd == "quit":
                    self.send_to_manager(f"deregister-user|{self.username}")
                    break
                elif cmd == "ls":
                    self.handle_ls()
                elif cmd.startswith("configure-dss "):
                    parts = cmd.split()
                    if len(parts) == 4:
                        self.handle_configure_dss(parts[1], int(parts[2]), int(parts[3]))
                    else:
                        print("Usage: configure-dss <name> <n> <striping_unit>")
                elif cmd.startswith("copy "):
                    file_path = cmd[5:].strip()
                    self.handle_copy(file_path)
                elif cmd.startswith("read "):
                    parts = cmd.split()
                    if len(parts) == 3:
                        self.handle_read(parts[1], parts[2])
                    else:
                        print("Usage: read <dss_name> <file_name>")
                elif cmd.startswith("disk-failure "):
                    dss_name = cmd[13:].strip()
                    self.handle_disk_failure(dss_name)
                elif cmd.startswith("decommission-dss "):
                    dss_name = cmd[17:].strip()
                    self.handle_decommission_dss(dss_name)
                elif cmd == "deregister-user":
                    self.send_to_manager(f"deregister-user|{self.username}")
                    break
                else:
                    print("Unknown command")
                    
            except KeyboardInterrupt:
                break
        
        print(f"[USER {self.username}] Exiting...")

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