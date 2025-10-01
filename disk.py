# disk.py
# Rishith Cheduluri (1225443687) and Sebastian Bejaoui (122)
import socket
import sys
import threading


class DSSDisk:
    def __init__(self, diskname, manager_ip, manager_port, m_port, c_port):
        self.diskname = diskname
        self.manager_ip = manager_ip
        self.manager_port = manager_port
        self.m_port = m_port
        self.c_port = c_port

        # Creating sockets
        self.m_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.m_socket.bind(('', m_port))

        self.c_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.c_socket.bind(('', c_port))

        print(f"Disk {diskname} started on ports {m_port}, {c_port}")

        # Registering with the manager
        self.register()

        # Starting the listening threads
        self.start_listeners()

    def register(self):
        """Register with the manager"""
        message = f"register-disk|{self.diskname}|127.0.0.1|{self.m_port}|{self.c_port}"

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(message.encode('utf-8'), (self.manager_ip, self.manager_port))

        response, _ = sock.recvfrom(1024)
        print(f"Registration response: {response.decode('utf-8')}")
        sock.close()

    def start_listeners(self):
        """Start listener threads for both ports"""
        m_thread = threading.Thread(target=self.listen_m_port)
        c_thread = threading.Thread(target=self.listen_c_port)

        m_thread.daemon = True
        c_thread.daemon = True

        m_thread.start()
        c_thread.start()

    def close(self):
        """Close sockets gracefully"""
        try:
            self.m_socket.close()
        except Exception:
            pass
        try:
            self.c_socket.close()
        except Exception:
            pass

    def send_command(self, command):
        """Send a command to the manager and return its response text"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(command.encode('utf-8'), (self.manager_ip, self.manager_port))
        response, _ = sock.recvfrom(1024)
        sock.close()
        resp_text = response.decode('utf-8')
        print(f"Response: {resp_text}")
        return resp_text

    def listen_m_port(self):
        """Listen for management messages"""
        while True:
            try:
                data, addr = self.m_socket.recvfrom(1024)
                message = data.decode('utf-8')
                print(f"M-port received: {message}")

            except Exception as e:
                print(f"M-port error: {e}")
                break  # exit loop if socket closed

    def listen_c_port(self):
        """Listen for command messages"""
        while True:
            try:
                data, addr = self.c_socket.recvfrom(1024)
                message = data.decode('utf-8')
                print(f"C-port received: {message}")

            except Exception as e:
                print(f"C-port error: {e}")
                break  # exit loop if socket closed

    def run(self):
        """Keep the disk process running"""
        try:
            while True:
                cmd = input("Type 'quit' or 'deregister-disk [name]': ").strip()

                if cmd == "quit":
                    # Deregister this disk before quitting
                    resp = self.send_command(f"deregister-disk|{self.diskname}")
                    if "SUCCESS" in resp:
                        self.close()
                        sys.exit(0)
                    break

                elif cmd.startswith("deregister-disk"):
                    # Accept both "deregister-disk" and "deregister-disk <disk-name>"
                    parts = cmd.split()
                    target = parts[1] if len(parts) > 1 else self.diskname
                    resp = self.send_command(f"deregister-disk|{target}")
                    # If we successfully deregistered ourselves, terminate
                    if target == self.diskname and "SUCCESS" in resp:
                        self.close()
                        sys.exit(0)

                else:
                    print("Unknown command")

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
