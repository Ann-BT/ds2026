#!/usr/bin/env python3

from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
import base64
import os
import sys
from datetime import datetime

class FileTransferRPCServer:
    def __init__(self):
        self.upload_dir = "uploads"
        self.chunk_size = 65536
        self.active_transfers = {}
        
        if not os.path.exists(self.upload_dir):
            os.makedirs(self.upload_dir)
    
    def start_transfer(self, filename, filesize):
        try:
            transfer_id = f"{filename}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            save_filename = f"received_{timestamp}_{filename}"
            filepath = os.path.join(self.upload_dir, save_filename)
            
            self.active_transfers[transfer_id] = {
                'filename': filename,
                'filepath': filepath,
                'filesize': filesize,
                'received': 0,
                'file_handle': open(filepath, 'wb')
            }
            
            print(f"[START] Transfer initiated: {filename} ({filesize} bytes)")
            print(f"[INFO] Transfer ID: {transfer_id}")
            
            return {
                'status': 'success',
                'transfer_id': transfer_id,
                'message': f'Transfer started for {filename}'
            }
        except Exception as e:
            print(f"[ERROR] Failed to start transfer: {e}")
            return {
                'status': 'error',
                'message': str(e)
            }
    
    def upload_chunk(self, transfer_id, chunk_data, chunk_number):
        try:
            if transfer_id not in self.active_transfers:
                return {
                    'status': 'error',
                    'message': 'Invalid transfer ID'
                }
            
            transfer = self.active_transfers[transfer_id]
            binary_data = base64.b64decode(chunk_data)
            
            transfer['file_handle'].write(binary_data)
            transfer['received'] += len(binary_data)
            
            progress = (transfer['received'] / transfer['filesize']) * 100
            
            print(f"[CHUNK {chunk_number}] Received {len(binary_data)} bytes ({progress:.1f}% complete)")
            
            return {
                'status': 'success',
                'received': transfer['received'],
                'progress': progress
            }
        except Exception as e:
            print(f"[ERROR] Chunk upload failed: {e}")
            return {
                'status': 'error',
                'message': str(e)
            }
    
    def finish_transfer(self, transfer_id):
        try:
            if transfer_id not in self.active_transfers:
                return {
                    'status': 'error',
                    'message': 'Invalid transfer ID'
                }
            
            transfer = self.active_transfers[transfer_id]
            transfer['file_handle'].close()
            
            actual_size = os.path.getsize(transfer['filepath'])
            expected_size = transfer['filesize']
            
            if actual_size == expected_size:
                print(f"[SUCCESS] Transfer complete: {transfer['filename']}")
                print(f"[INFO] Saved as: {transfer['filepath']}")
                print(f"[INFO] Size: {actual_size} bytes")
                
                del self.active_transfers[transfer_id]
                
                return {
                    'status': 'success',
                    'message': 'Transfer completed successfully',
                    'filepath': transfer['filepath'],
                    'size': actual_size
                }
            else:
                print(f"[WARNING] Size mismatch: expected {expected_size}, got {actual_size}")
                return {
                    'status': 'warning',
                    'message': f'Size mismatch: expected {expected_size}, got {actual_size}'
                }
        except Exception as e:
            print(f"[ERROR] Failed to finish transfer: {e}")
            return {
                'status': 'error',
                'message': str(e)
            }
    
    def cancel_transfer(self, transfer_id):
        try:
            if transfer_id not in self.active_transfers:
                return {
                    'status': 'error',
                    'message': 'Invalid transfer ID'
                }
            
            transfer = self.active_transfers[transfer_id]
            transfer['file_handle'].close()
            
            if os.path.exists(transfer['filepath']):
                os.remove(transfer['filepath'])
            
            del self.active_transfers[transfer_id]
            print(f"[CANCEL] Transfer cancelled: {transfer['filename']}")
            
            return {
                'status': 'success',
                'message': 'Transfer cancelled'
            }
        except Exception as e:
            print(f"[ERROR] Failed to cancel transfer: {e}")
            return {
                'status': 'error',
                'message': str(e)
            }
    
    def list_files(self):
        try:
            files = []
            for filename in os.listdir(self.upload_dir):
                filepath = os.path.join(self.upload_dir, filename)
                if os.path.isfile(filepath):
                    files.append({
                        'filename': filename,
                        'size': os.path.getsize(filepath),
                        'modified': datetime.fromtimestamp(
                            os.path.getmtime(filepath)
                        ).strftime('%Y-%m-%d %H:%M:%S')
                    })
            return {
                'status': 'success',
                'files': files
            }
        except Exception as e:
            return {
                'status': 'error',
                'message': str(e)
            }
    
    def ping(self):
        return {
            'status': 'success',
            'message': 'pong'
        }

class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

def main():
    host = sys.argv[1] if len(sys.argv) > 1 else '0.0.0.0'
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 8000
    
    print("=" * 60)
    print("RPC File Transfer Server")
    print("=" * 60)
    
    server = SimpleXMLRPCServer((host, port),
                                requestHandler=RequestHandler,
                                allow_none=True)
    server.register_introspection_functions()
    
    file_service = FileTransferRPCServer()
    server.register_instance(file_service)
    
    print(f"[SERVER] RPC Server listening on {host}:{port}")
    print(f"[SERVER] Upload directory: {file_service.upload_dir}")
    print(f"[SERVER] Ready to accept RPC calls...")
    print("=" * 60)
    print("\nAvailable RPC methods:")
    print("  - start_transfer(filename, filesize)")
    print("  - upload_chunk(transfer_id, chunk_data, chunk_number)")
    print("  - finish_transfer(transfer_id)")
    print("  - cancel_transfer(transfer_id)")
    print("  - list_files()")
    print("  - ping()")
    print("\nPress Ctrl+C to stop server\n")
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[SERVER] Shutting down...")

if __name__ == "__main__":
    main()
