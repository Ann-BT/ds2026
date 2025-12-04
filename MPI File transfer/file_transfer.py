from mpi4py import MPI
import os
import sys
from datetime import datetime
import time

class MPIFileTransfer:
    def __init__(self):
        self.comm = MPI.COMM_WORLD
        self.rank = self.comm.Get_rank()
        self.size = self.comm.Get_size()
        self.chunk_size = 65536
        self.upload_dir = "mpi_uploads"
        
        if self.rank == 0 and not os.path.exists(self.upload_dir):
            os.makedirs(self.upload_dir)
    
    def run_server(self):
        if self.rank == 0:
            self.master_process()
        else:
            self.worker_process()
    
    def master_process(self):
        print("=" * 60)
        print("MPI File Transfer Server (Master - Rank 0)")
        print(f"Total Processes: {self.size}")
        print(f"Available Workers: {self.size - 1}")
        print("=" * 60)
        
        active = True
        while active:
            print("\n[MASTER] Waiting for requests...")
            
            status = MPI.Status()
            request = self.comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
            source = status.Get_source()
            
            if request['type'] == 'send_file':
                self.handle_file_transfer(request, source)
            elif request['type'] == 'list':
                self.handle_list_request(source)
            elif request['type'] == 'shutdown':
                active = False
                self.broadcast_shutdown()
    
    def handle_file_transfer(self, request, source):
        filename = request['filename']
        filesize = request['filesize']
        num_chunks = request['num_chunks']
        
        print(f"\n[MASTER] File transfer from rank {source}")
        print(f"[INFO] File: {filename}, Size: {filesize} bytes, Chunks: {num_chunks}")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        save_filename = f"received_{timestamp}_{filename}"
        filepath = os.path.join(self.upload_dir, save_filename)
        
        workers = [i for i in range(1, self.size) if i != source]
        
        if not workers or num_chunks < 2:
            print(f"[MASTER] Receiving directly (no parallel processing)")
            self.comm.send({'status': 'ready', 'method': 'direct'}, dest=source, tag=20)
            self.receive_file_direct(source, filepath, num_chunks)
        else:
            num_workers = min(len(workers), num_chunks)
            selected_workers = workers[:num_workers]
            
            print(f"[MASTER] Using {num_workers} workers: {selected_workers}")
            
            self.comm.send({
                'status': 'ready',
                'method': 'parallel',
                'workers': selected_workers,
                'num_chunks': num_chunks
            }, dest=source, tag=20)
            
            self.coordinate_parallel_transfer(source, selected_workers, filepath, num_chunks)
        
        print(f"[SUCCESS] File saved: {save_filename}")
        self.comm.send({'status': 'complete'}, dest=source, tag=21)
    
    def receive_file_direct(self, source, filepath, num_chunks):
        with open(filepath, 'wb') as f:
            for i in range(num_chunks):
                chunk = self.comm.recv(source=source, tag=100 + i)
                f.write(chunk)
                progress = ((i + 1) / num_chunks) * 100
                print(f"[MASTER] Progress: {progress:.1f}%", end='\r')
        print()
    
    def coordinate_parallel_transfer(self, source, workers, filepath, num_chunks):
        chunks_per_worker = num_chunks // len(workers)
        remainder = num_chunks % len(workers)
        
        assignments = []
        start = 0
        
        for i, worker in enumerate(workers):
            count = chunks_per_worker + (1 if i < remainder else 0)
            end = start + count
            
            self.comm.send({
                'task': 'receive',
                'source': source,
                'start_chunk': start,
                'end_chunk': end,
                'filepath': f"{filepath}.part{worker}"
            }, dest=worker, tag=30)
            
            assignments.append((worker, start, end, f"{filepath}.part{worker}"))
            start = end
        
        for worker, s, e, part_file in assignments:
            self.comm.recv(source=worker, tag=31)
            print(f"[MASTER] Worker {worker} completed chunks {s}-{e-1}")
        
        with open(filepath, 'wb') as outfile:
            for worker, s, e, part_file in sorted(assignments, key=lambda x: x[1]):
                with open(part_file, 'rb') as infile:
                    outfile.write(infile.read())
                os.remove(part_file)
    
    def handle_list_request(self, source):
        files = []
        if os.path.exists(self.upload_dir):
            for fname in os.listdir(self.upload_dir):
                fpath = os.path.join(self.upload_dir, fname)
                if os.path.isfile(fpath):
                    files.append({
                        'filename': fname,
                        'size': os.path.getsize(fpath),
                        'modified': datetime.fromtimestamp(
                            os.path.getmtime(fpath)
                        ).strftime('%Y-%m-%d %H:%M:%S')
                    })
        
        self.comm.send({'status': 'success', 'files': files}, dest=source, tag=40)
    
    def broadcast_shutdown(self):
        print("\n[MASTER] Broadcasting shutdown...")
        for i in range(1, self.size):
            self.comm.send({'task': 'shutdown'}, dest=i, tag=30)
    
    def worker_process(self):
        print(f"[WORKER {self.rank}] Ready")
        
        while True:
            msg = self.comm.recv(source=0, tag=30)
            
            if msg.get('task') == 'shutdown':
                print(f"[WORKER {self.rank}] Shutting down")
                break
            
            if msg.get('task') == 'receive':
                source = msg['source']
                start = msg['start_chunk']
                end = msg['end_chunk']
                filepath = msg['filepath']
                
                print(f"[WORKER {self.rank}] Receiving chunks {start}-{end-1}")
                
                with open(filepath, 'wb') as f:
                    for i in range(start, end):
                        chunk = self.comm.recv(source=source, tag=100 + i)
                        f.write(chunk)
                
                self.comm.send({'status': 'done'}, dest=0, tag=31)
    
    def send_file(self, filepath):
        if not os.path.exists(filepath):
            print(f"[ERROR] File not found: {filepath}")
            return False
        
        filename = os.path.basename(filepath)
        filesize = os.path.getsize(filepath)
        
        with open(filepath, 'rb') as f:
            chunks = []
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
            'type': 'send_file',
            'filename': filename,
            'filesize': filesize,
            'num_chunks': num_chunks
        }
        
        print("\n[CLIENT] Sending request to master...")
        self.comm.send(request, dest=0, tag=MPI.ANY_TAG)
        
        response = self.comm.recv(source=0, tag=20)
        
        if response['status'] == 'ready':
            print(f"[CLIENT] Server ready - Method: {response['method']}")
            
            start_time = time.time()
            
            if response['method'] == 'parallel':
                workers = response['workers']
                print(f"[CLIENT] Parallel transfer using workers: {workers}")
            
            for i, chunk in enumerate(chunks):
                self.comm.send(chunk, dest=MPI.ANY_SOURCE, tag=100 + i)
                progress = ((i + 1) / num_chunks) * 100
                print(f"[PROGRESS] {progress:.1f}%", end='\r')
            
            print()
            
            completion = self.comm.recv(source=0, tag=21)
            elapsed = time.time() - start_time
            speed = (filesize / 1024) / elapsed if elapsed > 0 else 0
            
            if completion['status'] == 'complete':
                print(f"[SUCCESS] Transfer completed in {elapsed:.2f}s ({speed:.2f} KB/s)")
                return True
        
        return False
    
    def list_files(self):
        print("[CLIENT] Requesting file list...")
        
        self.comm.send({'type': 'list'}, dest=0, tag=MPI.ANY_TAG)
        response = self.comm.recv(source=0, tag=40)
        
        if response['status'] == 'success':
            files = response['files']
            
            if not files:
                print("[INFO] No files on server")
                return
            
            print("\n" + "=" * 60)
            print(f"Files on server ({len(files)} files):")
            print("=" * 60)
            
            for f in files:
                print(f"\nFilename: {f['filename']}")
                print(f"Size: {f['size']} bytes ({f['size'] / 1024:.2f} KB)")
                print(f"Modified: {f['modified']}")
            
            print("=" * 60)
    
    def shutdown_server(self):
        print("[CLIENT] Sending shutdown...")
        self.comm.send({'type': 'shutdown'}, dest=0, tag=MPI.ANY_TAG)

def print_usage():
    print("Usage:")
    print("  Server: mpiexec -n <N> python mpi_file_transfer.py --server")
    print("  Client: mpiexec -n <N> python mpi_file_transfer.py <file>")
    print("  List:   mpiexec -n <N> python mpi_file_transfer.py --list")
    print("\nExamples:")
    print("  mpiexec -n 4 python mpi_file_transfer.py --server")
    print("  mpiexec -n 4 python mpi_file_transfer.py myfile.txt")
    print("  mpiexec -n 2 python mpi_file_transfer.py --list")

def main():
    app = MPIFileTransfer()
    
    if len(sys.argv) < 2:
        if app.rank == 0:
            print_usage()
        return
    
    mode = sys.argv[1]
    
    if mode == '--server':
        app.run_server()
    elif mode == '--list':
        if app.rank == 0:
            app.run_server()
        elif app.rank == 1:
            app.list_files()
    elif mode == '--shutdown':
        if app.rank == 0:
            app.run_server()
        elif app.rank == 1:
            app.shutdown_server()
    else:
        if app.rank == 0:
            app.run_server()
        elif app.rank == 1:
            filepath = sys.argv[1]
            app.send_file(filepath)

if __name__ == "__main__":
    main()
