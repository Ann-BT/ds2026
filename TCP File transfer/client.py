import socket
import os
import sys
import time

class FileTransferClient:
    def __init__(self, host='127.0.0.1', port=5555):
        self.host = host
        self.port = port
        self.client_socket = None
        self.buffer_size = 4096
    
    def connect(self):
        """Establish connection to server"""
        try:
            self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            
            print(f"[CLIENT] Connecting to {self.host}:{self.port}...")

            self.client_socket.connect((self.host, self.port))
            
            print(f"[CLIENT] Connected successfully!")
            return True
            
        except ConnectionRefusedError:
            print(f"[ERROR] Connection refused. Is the server running?")
            return False
        except Exception as e:
            print(f"[ERROR] Connection failed: {e}")
            return False
    
    def send_file(self, filepath):
        """Send file to server"""
        try:
            if not os.path.exists(filepath):
                print(f"[ERROR] File not found: {filepath}")
                return False
            
            if not os.path.isfile(filepath):
                print(f"[ERROR] Path is not a file: {filepath}")
                return False

            filename = os.path.basename(filepath)
            filesize = os.path.getsize(filepath)
            
            print("=" * 50)
            print(f"[INFO] File: {filename}")
            print(f"[INFO] Size: {filesize} bytes ({filesize / 1024:.2f} KB)")
            print("=" * 50)
            
            filename_bytes = filename.encode('utf-8')
            filename_len = len(filename_bytes)
            self.client_socket.sendall(filename_len.to_bytes(4, byteorder='big'))
            print(f"[SENT] Filename length: {filename_len} bytes")
            
            self.client_socket.sendall(filename_bytes)
            print(f"[SENT] Filename: {filename}")

            self.client_socket.sendall(filesize.to_bytes(8, byteorder='big'))
            print(f"[SENT] File size: {filesize} bytes")

            print(f"[INFO] Sending file data...")
            sent_bytes = 0
            start_time = time.time()
            
            with open(filepath, 'rb') as f:
                while sent_bytes < filesize:
                    chunk = f.read(self.buffer_size)
                    if not chunk:
                        break

                    self.client_socket.sendall(chunk)
                    sent_bytes += len(chunk)

                    progress = (sent_bytes / filesize) * 100
                    print(f"[PROGRESS] {progress:.1f}% ({sent_bytes}/{filesize} bytes)", end='\r')
            
            elapsed_time = time.time() - start_time
            speed = (sent_bytes / 1024) / elapsed_time if elapsed_time > 0 else 0
            
            print(f"\n[SUCCESS] File sent completely!")
            print(f"[INFO] Time: {elapsed_time:.2f} seconds")
            print(f"[INFO] Speed: {speed:.2f} KB/s")

            print(f"[INFO] Waiting for server acknowledgment...")
            ack = self.client_socket.recv(3)
            
            if ack == b"ACK":
                print("[SUCCESS] Transfer confirmed by server!")
                return True
            else:
                print("[WARNING] Did not receive proper acknowledgment")
                return False
                
        except Exception as e:
            print(f"[ERROR] File transfer failed: {e}")
            return False
        finally:
            self.disconnect()
    
    def disconnect(self):
        """Close connection to server"""
        if self.client_socket:
            self.client_socket.close()
            print("[CLIENT] Disconnected from server")

def main():
    if len(sys.argv) < 2:
        print("Usage: python client.py <filepath> [host] [port]")
        print("Example: python client.py myfile.txt")
        print("Example: python client.py myfile.txt 192.168.1.100 5555")
        sys.exit(1)
    
    filepath = sys.argv[1]
    host = sys.argv[2] if len(sys.argv) > 2 else '127.0.0.1'
    port = int(sys.argv[3]) if len(sys.argv) > 3 else 5555
    
    client = FileTransferClient(host, port)

    if client.connect():
        success = client.send_file(filepath)
        if success:
            print("\n" + "=" * 50)
            print("[COMPLETE] File transfer completed successfully!")
            print("=" * 50)
        else:
            print("\n[FAILED] File transfer failed")
            sys.exit(1)
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()