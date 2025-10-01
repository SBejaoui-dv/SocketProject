# user.py
# Rishith Cheduluri (1225443687) and Sebastian Bejaoui (122)
import socket
import sys
import threading


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

        # Registering with the manager
        self.register()

    # ---------- helpers ----------
    @staticmethod
    def _is_power_of_two(x: int) -> bool:
        return x > 0 and (x & (x - 1)) == 0

    @staticmethod
    def _valid_name(s: str) -> bool:
        return s.isalpha() and len(s) <= 15

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

    # ---------- network ops ----------
    def register(self):
        """Register with the manager"""
        message = f"register-user|{self.username}|127.0.0.1|{self.m_port}|{self.c_port}"
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(message.encode('utf-8'), (self.manager_ip, self.manager_port))
        response, _ = sock.recvfrom(1024)
        print(f"Registration response: {response.decode('utf-8')}")
        sock.close()

    def send_command(self, command):
        """Send command to manager, print and return the response text"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(command.encode('utf-8'), (self.manager_ip, self.manager_port))
        response, _ = sock.recvfrom(1024)
        resp_text = response.decode('utf-8')
        print(f"Response: {resp_text}")
        sock.close()
        return resp_text

    # ---------- REPL ----------
    def run(self):
        """Interactive command loop"""
        print("Available commands: configure-dss <name> <n> <striping-unit>, deregister-user [name], quit")

        while True:
            try:
                cmd = input(f"{self.username}> ").strip()
                # Normalize any pasted Unicode dashes into ASCII '-'
                cmd = (cmd.replace('\u2010', '-')
                       .replace('\u2011', '-')
                       .replace('\u2012', '-')
                       .replace('\u2013', '-')
                       .replace('\u2014', '-')
                       .replace('\u2212', '-'))

                if cmd == "quit":
                    # Best-effort deregister self before quitting
                    resp = self.send_command(f"deregister-user|{self.username}")
                    if "SUCCESS" in resp:
                        self.close()
                        sys.exit(0)
                    break  # even if failure, leave loop

                elif cmd.startswith("configure-dss"):
                    parts = cmd.split()
                    if len(parts) != 4:
                        print("Usage: configure-dss <dss-name> <n>=#disks(>=3) <striping-unit>=power-of-two bytes")
                        continue

                    _, dss_name, n_str, su_str = parts

                    # client-side validation to avoid 'Invalid parameters'
                    try:
                        n = int(n_str)
                        su = int(su_str)
                    except ValueError:
                        print("Error: <n> and <striping-unit> must be integers.")
                        continue

                    if not self._valid_name(dss_name):
                        print("Error: <dss-name> must be alphabetic and ≤ 15 chars.")
                        continue
                    if n < 3:
                        print("Error: <n> must be ≥ 3.")
                        continue
                    if not self._is_power_of_two(su):
                        print("Error: <striping-unit> must be a power of two (e.g., 512, 1024, 2048, 4096...).")
                        continue

                    # Send exactly what the manager expects
                    self.send_command(f"configure-dss|{dss_name}|{n}|{su}")
                    continue  # <- prevent falling through to "Unknown command"

                elif cmd.startswith("deregister-user"):
                    # Accept both "deregister-user" and "deregister-user <name>"
                    parts = cmd.split()
                    target = parts[1] if len(parts) > 1 else self.username
                    resp = self.send_command(f"deregister-user|{target}")
                    if target == self.username and "SUCCESS" in resp:
                        self.close()
                        sys.exit(0)
                    continue  # keep REPL alive if deregistering someone else

                else:
                    print("Unknown command")
                    continue

            except KeyboardInterrupt:
                # Try to deregister on Ctrl+C for the current user, then exit
                try:
                    self.send_command(f"deregister-user|{self.username}")
                except Exception:
                    pass
                self.close()
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
