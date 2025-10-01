# manager.py
# Minimal UDP manager for DSS milestone demo
import socket
import sys
import threading

class DSSManager:
    def __init__(self, listen_port):
        self.listen_port = listen_port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('', listen_port))

        # Registries
        # users[name] = {"ip": str, "m_port": int, "c_port": int}
        self.users = {}
        # disks[name] = {"ip": str, "m_port": int, "c_port": int, "state": "Available" or "InDSS:<dssname>"}
        self.disks = {}
        # dss[dssname] = {"n": int, "striping_unit": int, "disks": [names]}
        self.dss = {}

        self._running = True
        print(f"[manager] listening on UDP :{listen_port}")

    # ---------- helpers ----------
    @staticmethod
    def _is_pow2(x: int) -> bool:
        return x > 0 and (x & (x - 1)) == 0

    @staticmethod
    def _valid_name(s: str) -> bool:
        return s.isalpha() and len(s) <= 15

    def _send(self, text: str, addr):
        # Always send bytes back to the sender
        self.sock.sendto(text.encode('utf-8'), addr)

    def _log(self, *args):
        print(*args, flush=True)

    # ---------- command handlers ----------
    def handle_register_user(self, parts, addr):
        # register-user|name|ip|m_port|c_port
        if len(parts) != 5:
            self._send("FAILURE|invalid-arity", addr); return
        name, ip, m_str, c_str = parts[1], parts[2], parts[3], parts[4]
        try:
            m_port, c_port = int(m_str), int(c_str)
        except ValueError:
            self._send("FAILURE|non-integer-port", addr); return
        if name in self.users:
            self._send("FAILURE|user-exists", addr); return
        self.users[name] = {"ip": ip, "m_port": m_port, "c_port": c_port}
        self._log(f"[manager] user registered: {name} @ {ip} m:{m_port} c:{c_port}")
        self._send("SUCCESS", addr)

    def handle_register_disk(self, parts, addr):
        # register-disk|name|ip|m_port|c_port
        if len(parts) != 5:
            self._send("FAILURE|invalid-arity", addr); return
        name, ip, m_str, c_str = parts[1], parts[2], parts[3], parts[4]
        try:
            m_port, c_port = int(m_str), int(c_str)
        except ValueError:
            self._send("FAILURE|non-integer-port", addr); return
        if name in self.disks:
            self._send("FAILURE|disk-exists", addr); return
        self.disks[name] = {"ip": ip, "m_port": m_port, "c_port": c_port, "state": "Available"}
        self._log(f"[manager] disk registered: {name} @ {ip} m:{m_port} c:{c_port} state=Available")
        self._send("SUCCESS", addr)

    def handle_deregister_user(self, parts, addr):
        # deregister-user|name
        if len(parts) != 2:
            self._send("FAILURE|invalid-arity", addr); return
        name = parts[1]
        if name not in self.users:
            self._send("FAILURE|no-such-user", addr); return
        del self.users[name]
        self._log(f"[manager] user deregistered: {name}")
        self._send("SUCCESS", addr)

    def handle_deregister_disk(self, parts, addr):
        # deregister-disk|name   (FAILURE if not exist OR state InDSS)
        if len(parts) != 2:
            self._send("FAILURE|invalid-arity", addr); return
        name = parts[1]
        if name not in self.disks:
            self._send("FAILURE|no-such-disk", addr); return
        state = self.disks[name]["state"]
        if isinstance(state, str) and state.startswith("InDSS"):
            self._send("FAILURE|disk-in-dss", addr); return
        del self.disks[name]
        self._log(f"[manager] disk deregistered: {name}")
        self._send("SUCCESS", addr)

    def handle_configure_dss(self, parts, addr):
        # configure-dss|<dss-name>|<n>|<striping-unit>  (sent by a USER)
        if len(parts) != 4:
            self._send("FAILURE|invalid-arity", addr); return
        dss_name, n_str, su_str = parts[1], parts[2], parts[3]
        # Validate parameters
        if not self._valid_name(dss_name):
            self._send("FAILURE|bad-dss-name", addr); return
        try:
            n = int(n_str); su = int(su_str)
        except ValueError:
            self._send("FAILURE|non-integer", addr); return
        if n < 3:
            self._send("FAILURE|n-must-be->=3", addr); return
        if not self._is_pow2(su):
            self._send("FAILURE|striping-unit-not-power-of-two", addr); return
        if dss_name in self.dss:
            self._send("FAILURE|dss-exists", addr); return

        # Allocate disks
        avail = [name for name, meta in self.disks.items() if meta["state"] == "Available"]
        if len(avail) < n:
            self._send("FAILURE|insufficient-disks", addr); return

        chosen = avail[:n]
        for d in chosen:
            self.disks[d]["state"] = f"InDSS:{dss_name}"
        self.dss[dss_name] = {"n": n, "striping_unit": su, "disks": chosen}

        self._log(f"[manager] DSS configured: {dss_name} n={n} su={su} using disks {chosen}")
        # (Optional) you could notify disks on their m_port here.
        self._send(f"SUCCESS|configured {dss_name} with n={n}, su={su}", addr)

    # ---------- server loops ----------
    def udp_loop(self):
        while self._running:
            try:
                data, addr = self.sock.recvfrom(2048)
            except OSError:
                break  # socket closed
            msg = data.decode('utf-8').strip()
            self._log(f"[manager] from {addr}: {msg}")
            parts = msg.split('|')
            if not parts:
                self._send("FAILURE|empty", addr); continue

            cmd = parts[0]
            try:
                if cmd == "register-user":
                    self.handle_register_user(parts, addr)
                elif cmd == "register-disk":
                    self.handle_register_disk(parts, addr)
                elif cmd == "deregister-user":
                    self.handle_deregister_user(parts, addr)
                elif cmd == "deregister-disk":
                    self.handle_deregister_disk(parts, addr)
                elif cmd == "configure-dss":
                    self.handle_configure_dss(parts, addr)
                else:
                    self._send("FAILURE|unknown-command", addr)
            except Exception as e:
                # Defensive: never crash the manager on bad input
                self._log(f"[manager] error handling '{msg}': {e}")
                self._send("FAILURE|internal-error", addr)

    def console_loop(self):
        # Local manager console for terminate-manager and simple introspection
        while self._running:
            try:
                line = input().strip()
            except EOFError:
                break
            if line == "terminate-manager":
                self._log("[manager] terminating...")
                self._running = False
                break
            elif line == "show-state":
                self._log(f"[state] users={list(self.users.keys())}")
                self._log(f"[state] disks={{name:meta['state'] for name,meta in self.disks.items()}}")
                self._log(f"[state] dss={self.dss}")
            else:
                self._log("[manager] console commands: terminate-manager | show-state")

    def run(self):
        t = threading.Thread(target=self.udp_loop, daemon=True)
        t.start()
        self.console_loop()
        # Shutdown
        try:
            self.sock.close()
        except Exception:
            pass
        t.join(timeout=0.5)
        self._log("[manager] stopped.")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 manager.py <listen_port>")
        sys.exit(1)
    port = int(sys.argv[1])
    mgr = DSSManager(port)
    mgr.run()
