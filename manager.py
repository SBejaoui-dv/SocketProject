# manager.py
# Rishith Cheduluri (1225443687) and Sebastian Bejaoui (122)
import socket
import sys
import threading
import json

class DSSManager:
    def __init__(self, port):
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(('', port))
        
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
        return "FAILURE|Not implemented"


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python manager.py <port>")
        sys.exit(1)
    
    port = int(sys.argv[1])
    manager = DSSManager(port)
    manager.run()