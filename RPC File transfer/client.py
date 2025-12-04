import xmlrpc.client
import base64
import os
import sys
import time

class FileTransferRPCClient:
    def __init__(self, host='127.0.0.1', port=8000):
        self.server_url = f"http://{host}:{port}"
        self.proxy = None
        self.chunk_size = 65536
    
    def connect(self):
        try:
            print(f"[CLIENT] Connecting to RPC server at {self.server_url}...")
            self.proxy = xmlrpc.client.ServerProxy(self.server_url, allow_none=True)
            
            response = self.proxy.ping()
            if response['status'] == 'success':
                print(f"[CLIENT] Connected successfully!")
                return True
            else:
                print(f"[ERROR] Server returned error: {response.get('message', 'Unknown error')}")
                return False
                
        except Exception as e:
            print(f"[ERROR] Connection failed: {e}")
            return False
    
    def send_file(self, filepath):
        try:
            if not os.path.exists(filepath):
                print(f"[ERROR] File not found: {filepath}")
                return False
            
            if not os.path.isfile(filepath):
                print(f"[ERROR] Path is not a file: {filepath}")
                return False
            
            filename = os.path.basename(filepath)
            filesize = os.path.getsize(filepath)
            
            print("\n" + "=" * 60)
            print(f"[INFO] File: {filename}")
            print(f"[INFO] Size: {filesize} bytes ({filesize / 1024:.2f} KB)")
            print("=" * 60)
            
            print("\n[STEP 1] Initiating transfer...")
            response = self.proxy.start_transfer(filename, filesize)
            
            if response['status'] != 'success':
                print(f"[ERROR] Failed to start transfer: {response.get('message')}")
                return False
            
            transfer_id = response['transfer_id']
            print(f"[SUCCESS] Transfer initiated with ID: {transfer_id}")
            
            print(f"\n[STEP 2] Uploading file in {self.chunk_size} byte chunks...")
            
            chunk_number = 0
            sent_bytes = 0
            start_time = time.time()
            
            with open(filepath, 'rb') as f:
                while True:
                    chunk = f.read(self.chunk_size)
                    if not chunk:
                        break
                    
                    chunk_number += 1
                    encoded_chunk = base64.b64encode(chunk).decode('utf-8')
                    response = self.proxy.upload_chunk(transfer_id, encoded_chunk, chunk_number)
                    
                    if response['status'] != 'success':
                        print(f"\n[ERROR] Chunk {chunk_number} upload failed: {response.get('message')}")
                        self.proxy.cancel_transfer(transfer_id)
                        return False
                    
                    sent_bytes += len(chunk)
                    progress = (sent_bytes / filesize) * 100
                    
                    print(f"[CHUNK {chunk_number}] Sent {len(chunk)} bytes - Progress: {progress:.1f}%", end='\r')
            
            elapsed_time = time.time() - start_time
            speed = (sent_bytes / 1024) / elapsed_time if elapsed_time > 0 else 0
            
            print(f"\n[SUCCESS] All chunks uploaded!")
            print(f"[INFO] Total chunks: {chunk_number}")
            print(f"[INFO] Time: {elapsed_time:.2f} seconds")
            print(f"[INFO] Speed: {speed:.2f} KB/s")
            
            print(f"\n[STEP 3] Finalizing transfer...")
            response = self.proxy.finish_transfer(transfer_id)
            
            if response['status'] == 'success':
                print(f"[SUCCESS] Transfer completed successfully!")
                print(f"[INFO] Server saved file as: {response.get('filepath', 'N/A')}")
                return True
            else:
                print(f"[ERROR] Failed to finalize transfer: {response.get('message')}")
                return False
                
        except Exception as e:
            print(f"\n[ERROR] File transfer failed: {e}")
            return False
    
    def list_server_files(self):
        try:
            print("\n[INFO] Requesting file list from server...")
            response = self.proxy.list_files()
            
            if response['status'] == 'success':
                files = response['files']
                
                if not files:
                    print("[INFO] No files on server")
                    return True
                
                print("\n" + "=" * 60)
                print(f"Files on server ({len(files)} files):")
                print("=" * 60)
                
                for file_info in files:
                    print(f"\nFilename: {file_info['filename']}")
                    print(f"Size: {file_info['size']} bytes ({file_info['size'] / 1024:.2f} KB)")
                    print(f"Modified: {file_info['modified']}")
                
                print("=" * 60)
                return True
            else:
                print(f"[ERROR] Failed to get file list: {response.get('message')}")
                return False
                
        except Exception as e:
            print(f"[ERROR] Failed to list files: {e}")
            return False

def print_usage():
    print("Usage:")
    print("  python rpc_client.py <filepath> [host] [port]")
    print("  python rpc_client.py --list [host] [port]")
    print("\nExamples:")
    print("  python rpc_client.py myfile.txt")
    print("  python rpc_client.py myfile.txt 192.168.1.100 8000")
    print("  python rpc_client.py --list")
    print("  python rpc_client.py --list 192.168.1.100 8000")

def main():
    if len(sys.argv) < 2:
        print_usage()
        sys.exit(1)
    
    if sys.argv[1] == '--list':
        host = sys.argv[2] if len(sys.argv) > 2 else '127.0.0.1'
        port = int(sys.argv[3]) if len(sys.argv) > 3 else 8000
        
        client = FileTransferRPCClient(host, port)
        if client.connect():
            client.list_server_files()
        sys.exit(0)
    
    filepath = sys.argv[1]
    host = sys.argv[2] if len(sys.argv) > 2 else '127.0.0.1'
    port = int(sys.argv[3]) if len(sys.argv) > 3 else 8000
    
    client = FileTransferRPCClient(host, port)
    
    if client.connect():
        success = client.send_file(filepath)
        
        if success:
            print("\n" + "=" * 60)
            print("[COMPLETE] File transfer completed successfully!")
            print("=" * 60)
        else:
            print("\n[FAILED] File transfer failed")
            sys.exit(1)
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()