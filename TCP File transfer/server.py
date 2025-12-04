#!/usr/bin/env python3
import socket
import os
import sys
from datetime import datetime

class FileTransferServer:
    def __init__(self, host='0.0.0.0', port=5555):
        self.host = host
        self.port = port
        self.server_socket = None
        self.buffer_size = 4096
        
    def start(self):
        """Initialize and start the server"""
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            self.server_socket.bind((self.host, self.port))

            self.server_socket.listen(5)
            
            print("=" * 50)
            print(f"[SERVER] TCP File Transfer Server Started")
            print(f"[SERVER] Listening on {self.host}:{self.port}")
            print(f"[SERVER] Waiting for connections...")
            print("=" * 50)
            
            self.accept_connections()
            
        except Exception as e:
            print(f"[ERROR] Failed to start server: {e}")
            sys.exit(1)
    
    def accept_connections(self):
        """Accept and handle incoming connections"""
        while True:
            try:
                conn, addr = self.server_socket.accept()
                print(f"\n[CONNECTION] New connection from {addr[0]}:{addr[1]}")
                
                self.receive_file(conn, addr)
                
            except KeyboardInterrupt:
                print("\n[SERVER] Shutting down gracefully...")
                break
            except Exception as e:
                print(f"[ERROR] Connection error: {e}")
                continue
    
    def receive_file(self, conn, addr):
        """Receive file from client"""
        try:
            filename_len_data = self.recv_exact(conn, 4)
            if not filename_len_data:
                print("[ERROR] Failed to receive filename length")
                conn.close()
                return
            
            filename_len = int.from_bytes(filename_len_data, byteorder='big')
            print(f"[INFO] Filename length: {filename_len} bytes")
            
            filename_data = self.recv_exact(conn, filename_len)
            if not filename_data:
                print("[ERROR] Failed to receive filename")
                conn.close()
                return
            
            filename = filename_data.decode('utf-8')
            print(f"[INFO] Receiving file: {filename}")
            
            filesize_data = self.recv_exact(conn, 8)
            if not filesize_data:
                print("[ERROR] Failed to receive file size")
                conn.close()
                return
            
            filesize = int.from_bytes(filesize_data, byteorder='big')
            print(f"[INFO] File size: {filesize} bytes ({filesize / 1024:.2f} KB)")
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            save_filename = f"received_{timestamp}_{filename}"
            
            received_bytes = 0
            with open(save_filename, 'wb') as f:
                while received_bytes < filesize:
                    remaining = filesize - received_bytes
                    chunk_size = min(self.buffer_size, remaining)
                    
                    chunk = conn.recv(chunk_size)
                    if not chunk:
                        break
                    
                    f.write(chunk)
                    received_bytes += len(chunk)
                    
                    progress = (received_bytes / filesize) * 100
                    print(f"[PROGRESS] {progress:.1f}% ({received_bytes}/{filesize} bytes)", end='\r')
            
            print(f"\n[SUCCESS] File saved as: {save_filename}")
            
            conn.sendall(b"ACK")
            print("[INFO] Acknowledgment sent to client")
            
        except Exception as e:
            print(f"[ERROR] File transfer failed: {e}")
        finally:
            conn.close()
            print(f"[CONNECTION] Closed connection from {addr[0]}:{addr[1]}")
    
    def recv_exact(self, conn, num_bytes):
        """Receive exact number of bytes"""
        data = b''
        while len(data) < num_bytes:
            chunk = conn.recv(num_bytes - len(data))
            if not chunk:
                return None
            data += chunk
        return data
    
    def stop(self):
        """Stop the server"""
        if self.server_socket:
            self.server_socket.close()
            print("[SERVER] Server stopped")

def main():
    if len(sys.argv) > 2:
        host = sys.argv[1]
        port = int(sys.argv[2])
    elif len(sys.argv) > 1:
        host = '0.0.0.0'
        port = int(sys.argv[1])
    else:
        host = '0.0.0.0'
        port = 5555
    
    server = FileTransferServer(host, port)
    try:
        server.start()
    except KeyboardInterrupt:
        server.stop()

if __name__ == "__main__":
    main()
