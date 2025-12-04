#!/usr/bin/env python3

from mpi4py import MPI
import os
import sys
import time

class MPIFileTransferClient:
    def __init__(self):
        self.comm = MPI.COMM_WORLD
        self.rank = self.comm.Get_rank()
        self.size = self.comm.Get_size()
        self.chunk_size = 65536
    
    def send_file(self, filepath):
        if self.rank != 1:
            if self.rank == 0:
                print("[ERROR] Client must run from rank 1 or higher")
            return False
        
        if not os.path.exists(filepath):
            print(f"[ERROR] File not found: {filepath}")
            return False
        
        filename = os.path.basename(filepath)
        filesize = os.path.getsize(filepath)
        
        chunks = []
        with open(filepath, 'rb') as f:
            while True:
                chunk = f.read(self.chunk_size)
                if not chunk:
                    break
                chunks.append(chunk)
        
        num_chunks = len(chunks)
        
        print("=" * 60)
        print(f"MPI File Transfer Client (Rank {self.rank})")
        print("=" * 60)
        print(f"[INFO] File: {filename}")
        print(f"[INFO] Size: {filesize} bytes ({filesize / 1024:.2f} KB)")
        print(f"[INFO] Chunks: {num_chunks}")
        print("=" * 60)
        
        request = {
            'type': 'transfer',
            'source_rank': self.rank,
            'filename': filename,
            'filesize': filesize,
            'num_chunks': num_chunks
        }
        
        print(f"\n[CLIENT] Sending transfer request to master (rank 0)...")
        self.comm.send(request, dest=0, tag=1)
        
        response = self.comm.recv(source=0, tag=2)
        
        if response['status'] == 'ready':
            print(f"[CLIENT] Master ready to receive")
            print(f"[INFO] File will be saved as: {os.path.basename(response['filepath'])}")
            
            print(f"\n[CLIENT] Sending {num_chunks} chunks...")
            start_time = time.time()
            
            for chunk_num, chunk_data in enumerate(chunks):
                self.comm.send({'data': chunk_data}, dest=MPI.ANY_SOURCE, tag=100 + chunk_num)
                progress = ((chunk_num + 1) / num_chunks) * 100
                print(f"[PROGRESS] Sent chunk {chunk_num + 1}/{num_chunks} ({progress:.1f}%)", end='\r')
            
            print()
            
            completion = self.comm.recv(source=0, tag=3)
            elapsed_time = time.time() - start_time
            speed = (filesize / 1024) / elapsed_time if elapsed_time > 0 else 0
            
            if completion['status'] == 'complete':
                print(f"[SUCCESS] Transfer completed!")
                print(f"[INFO] Time: {elapsed_time:.2f} seconds")
                print(f"[INFO] Speed: {speed:.2f} KB/s")
                return True
        
        return False
    
    def list_files(self):
        if self.rank != 1:
            if self.rank == 0:
                print("[ERROR] Client operations must run from rank 1 or higher")
            return False
        
        print(f"\n[CLIENT] Requesting file list from master...")
        
        request = {
            'type': 'list',
            'source_rank': self.rank
        }
        
        self.comm.send(request, dest=0, tag=1)
        response = self.comm.recv(source=0, tag=4)
        
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
        
        return False
    
    def shutdown_server(self):
        if self.rank != 1:
            return
        
        print("[CLIENT] Sending shutdown request...")
        request = {
            'type': 'shutdown',
            'source_rank': self.rank
        }
        self.comm.send(request, dest=0, tag=1)

def print_usage():
    print("Usage:")
    print("  mpiexec -n <num_processes> python mpi_client.py <filepath>")
    print("  mpiexec -n <num_processes> python mpi_client.py --list")
    print("  mpiexec -n <num_processes> python mpi_client.py --shutdown")
    print("\nExamples:")
    print("  mpiexec -n 4 python mpi_client.py myfile.txt")
    print("  mpiexec -n 2 python mpi_client.py --list")
    print("  mpiexec -n 2 python mpi_client.py --shutdown")

def main():
    client = MPIFileTransferClient()
    
    if client.rank == 0:
        print("[INFO] Rank 0 is reserved for master process")
        print("[INFO] Client operations run from rank 1+")
        return
    
    if client.rank == 1:
        if len(sys.argv) < 2:
            print_usage()
            return
        
        if sys.argv[1] == '--list':
            client.list_files()
        elif sys.argv[1] == '--shutdown':
            client.shutdown_server()
        else:
            filepath = sys.argv[1]
            success = client.send_file(filepath)
            
            if success:
                print("\n" + "=" * 60)
                print("[COMPLETE] File transfer completed successfully!")
                print("=" * 60)
            else:
                print("\n[FAILED] File transfer failed")

if __name__ == "__main__":
    main()
