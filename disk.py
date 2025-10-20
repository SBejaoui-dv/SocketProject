# disk.py
# Rishith Cheduluri (1225443687) and Sebastian Bejaoui (122)
import socket
import sys
import threading
import struct

class DSSDisk:
    def __init__(self, diskname, manager_ip, manager_port, m_port, c_port):
        self.diskname = diskname
        self.manager_ip = manager_ip
        self.manager_port = manager_port
        self.m_port = m_port
        self.c_port = c_port

        # Storage: {dss_name: {file_name: {stripe: {block_idx: block_data}}}}
        self.storage = {}
        self.lock = threading.Lock()

        # Create sockets
        self.m_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.m_socket.bind(('', m_port))

        self.c_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.c_socket.bind(('', c_port))

        print(f"[DISK {diskname}] Started on ports {m_port}, {c_port}")

        # Register with manager
        self.register()

        # Start listener threads
        self.start_listeners()

    def close(self):
        """Close sockets gracefully."""
        try:
            self.m_socket.close()
        except Exception:
            pass
        try:
            self.c_socket.close()
        except Exception:
            pass

    def send_command(self, command: str) -> str:
        """Send a command to the manager and return the response text."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(command.encode('utf-8'), (self.manager_ip, self.manager_port))
        response, _ = sock.recvfrom(1024)
        sock.close()
        resp_text = response.decode('utf-8')
        return resp_text

    def register(self):
        """Register with the manager."""
        message = f"register-disk|{self.diskname}|127.0.0.1|{self.m_port}|{self.c_port}"
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(message.encode('utf-8'), (self.manager_ip, self.manager_port))
        response, _ = sock.recvfrom(1024)
        print(f"[DISK {self.diskname}] Registration: {response.decode('utf-8')}")
        sock.close()

    def start_listeners(self):
        """Start listener threads for both ports."""
        m_thread = threading.Thread(target=self.listen_m_port, daemon=True)
        c_thread = threading.Thread(target=self.listen_c_port, daemon=True)
        m_thread.start()
        c_thread.start()

    def listen_m_port(self):
        """Listen for management messages."""
        while True:
            try:
                data, addr = self.m_socket.recvfrom(1024)
                message = data.decode('utf-8')
                print(f"[DISK {self.diskname}] M-port: {message}")
            except Exception as e:
                print(f"[DISK {self.diskname}] M-port error: {e}")
                break

    def listen_c_port(self):
        """Listen for command messages (block transfers, etc.)"""
        while True:
            try:
                data, addr = self.c_socket.recvfrom(65536)
                
                # Parse message header to determine type
                try:
                    header_end = data.index(b'|', data.index(b'|', data.index(b'|') + 1) + 1)
                    header_end = data.index(b'|', header_end + 1)
                    header_end = data.index(b'|', header_end + 1)
                    header_end = data.index(b'|', header_end + 1)
                    header_end = data.index(b'|', header_end + 1)
                except:
                    header_end = len(data)
                
                header = data[:header_end].decode('utf-8', errors='ignore')
                body = data[header_end + 1:]
                
                parts = header.split('|')
                msg_type = parts[0]
                
                if msg_type == "WRITE_BLOCK":
                    # Format: WRITE_BLOCK|dss_name|file_name|stripe|block_idx|block_type|block_size
                    dss_name, file_name, stripe, block_idx, block_type, block_size = parts[1:7]
                    self.handle_write_block(dss_name, file_name, stripe, block_idx, 
                                           block_type, int(block_size), body, addr)
                
                elif msg_type == "READ_BLOCK":
                    # Format: READ_BLOCK|dss_name|file_name|stripe|block_idx
                    dss_name, file_name, stripe, block_idx = parts[1:5]
                    self.handle_read_block(dss_name, file_name, stripe, block_idx, addr)
                
                elif msg_type == "FAIL":
                    # Format: FAIL|dss_name
                    dss_name = parts[1]
                    self.handle_fail(dss_name, addr)
                
                elif msg_type == "RECOVER":
                    # Format: RECOVER|dss_name|source_disk_idx
                    dss_name, source_idx = parts[1:3]
                    self.handle_recover(dss_name, source_idx, addr)
                
            except Exception as e:
                print(f"[DISK {self.diskname}] C-port error: {e}")
                break

    def handle_write_block(self, dss_name, file_name, stripe, block_idx, 
                          block_type, block_size, block_data, addr):
        """Store a block from user."""
        stripe = int(stripe)
        block_idx = int(block_idx)
        
        # Extract exact block size from body
        actual_block = block_data[:block_size]
        
        with self.lock:
            # Initialize storage structure if needed
            if dss_name not in self.storage:
                self.storage[dss_name] = {}
            if file_name not in self.storage[dss_name]:
                self.storage[dss_name][file_name] = {}
            if stripe not in self.storage[dss_name][file_name]:
                self.storage[dss_name][file_name][stripe] = {}
            
            # Store the block
            self.storage[dss_name][file_name][stripe][block_idx] = actual_block
        
        print(f"[DISK {self.diskname}] Stored {dss_name}/{file_name}/stripe{stripe}/block{block_idx} ({len(actual_block)} bytes)")
       
        # Send ACK back to user
        ack = f"WRITE_ACK|{dss_name}|{file_name}|{stripe}|{block_idx}"
        self.c_socket.sendto(ack.encode('utf-8'), addr)

    def handle_read_block(self, dss_name, file_name, stripe, block_idx, addr):
        """Retrieve a block for user."""
        stripe = int(stripe)
        block_idx = int(block_idx)
        
        block_data = b""
        with self.lock:
            if (dss_name in self.storage and 
                file_name in self.storage[dss_name] and
                stripe in self.storage[dss_name][file_name] and
                block_idx in self.storage[dss_name][file_name][stripe]):
                block_data = self.storage[dss_name][file_name][stripe][block_idx]
        
        print(f"[DISK {self.diskname}] Read {dss_name}/{file_name}/stripe{stripe}/block{block_idx} ({len(block_data)} bytes)")
        
        # Send block back with size prefix (4-byte big-endian)
        size_bytes = struct.pack('>I', len(block_data))
        self.c_socket.sendto(size_bytes + block_data, addr)

    def handle_fail(self, dss_name, addr):
        """Simulate disk failure by clearing data for this DSS."""
        with self.lock:
            if dss_name in self.storage:
                del self.storage[dss_name]
        
        print(f"[DISK {self.diskname}] Failed DSS {dss_name} - data cleared")
        
        # Send complete message back
        fail_complete = f"FAIL_COMPLETE|{dss_name}"
        self.c_socket.sendto(fail_complete.encode('utf-8'), addr)

    def handle_recover(self, dss_name, source_idx, addr):
        """Handle recovery request (simplified)."""
        print(f"[DISK {self.diskname}] Recovery message received (stub)")
        # Full implementation would read from source disk, XOR, and store

    def run(self):
        """Interactive command loop for the disk process."""
        try:
            while True:
                cmd = input(f"\n[{self.diskname}]> ").strip()

                if cmd == "quit":
                    resp = self.send_command(f"deregister-disk|{self.diskname}")
                    if "SUCCESS" in resp:
                        self.close()
                        sys.exit(0)

                # Deregister disk (with or without explicit name)
                elif cmd.startswith("deregister-disk"):
                    parts = cmd.split()
                    target = parts[1] if len(parts) > 1 else self.diskname
                    resp = self.send_command(f"deregister-disk|{target}")
                    if target == self.diskname and "SUCCESS" in resp:
                        self.close()
                        sys.exit(0)

                else:
                    print("Commands: deregister-disk, quit")

        except KeyboardInterrupt:
            # Best-effort deregister on Ctrl+C
            try:
                self.send_command(f"deregister-disk|{self.diskname}")
            except Exception:
                pass
            self.close()


if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Usage: python disk.py <diskname> <manager_ip> <manager_port> <m_port> <c_port>")
        sys.exit(1)

    diskname = sys.argv[1]
    manager_ip = sys.argv[2]
    manager_port = int(sys.argv[3])
    m_port = int(sys.argv[4])
    c_port = int(sys.argv[5])

    disk = DSSDisk(diskname, manager_ip, manager_port, m_port, c_port)
    disk.run()