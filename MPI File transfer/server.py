from mpi4py import MPI
import os
import sys
from datetime import datetime
import pickle

class MPIFileTransferServer:
    def __init__(self):
        self.comm = MPI.COMM_WORLD
        self.rank = self.comm.Get_rank()
        self.size = self.comm.Get_size()
        self.upload_dir = "mpi_uploads"
        
        if self.rank == 0:
            if not os.path.exists(self.upload_dir):
                os.makedirs(self.upload_dir)
    
    def run(self):
        if self.rank == 0:
            self.master_process()
        else:
            self.worker_process()
    
    def master_process(self):
        print("=" * 60)
        print(f"MPI File Transfer Server (Master Process)")
        print(f"Total Processes: {self.size}")
        print(f"Workers: {self.size - 1}")
        print("=" * 60)
        
        while True:
            print("\n[MASTER] Waiting for file transfer request...")
            
            request = self.comm.recv(source=MPI.ANY_SOURCE, tag=1)
            
            if request['type'] == 'transfer':
                source_rank = request['source_rank']
                filename = request['filename']
                filesize = request['filesize']
                num_chunks = request['num_chunks']
                
                print(f"\n[MASTER] Received transfer request from rank {source_rank}")
                print(f"[INFO] File: {filename}")
                print(f"[INFO] Size: {filesize} bytes ({filesize / 1024:.2f} KB)")
                print(f"[INFO] Chunks: {num_chunks}")
                
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                save_filename = f"received_{timestamp}_{filename}"
                filepath = os.path.join(self.upload_dir, save_filename)
                
                self.comm.send({'status': 'ready', 'filepath': filepath}, dest=source_rank, tag=2)
                
                available_workers = list(range(1, self.size))
                if source_rank in available_workers:
                    available_workers.remove(source_rank)
                
                if not available_workers:
                    print("[WARNING] No workers available, using master for reception")
                    self.receive_all_chunks(source_rank, filepath, num_chunks)
                else:
                    num_workers = min(len(available_workers), num_chunks)
                    workers = available_workers[:num_workers]
                    
                    print(f"[MASTER] Distributing work to {num_workers} workers: {workers}")
                    
                    chunks_per_worker = num_chunks // num_workers
                    remainder = num_chunks % num_workers
                    
                    chunk_assignments = []
                    start_chunk = 0
                    
                    for i, worker in enumerate(workers):
                        end_chunk = start_chunk + chunks_per_worker + (1 if i < remainder else 0)
                        chunk_assignments.append((worker, start_chunk, end_chunk))
                        
                        work_data = {
                            'source_rank': source_rank,
                            'start_chunk': start_chunk,
                            'end_chunk': end_chunk,
                            'filepath': filepath
                        }
                        self.comm.send(work_data, dest=worker, tag=10)
                        print(f"[MASTER] Assigned chunks {start_chunk}-{end_chunk-1} to worker {worker}")
                        
                        start_chunk = end_chunk
                    
                    for worker, start, end in chunk_assignments:
                        result = self.comm.recv(source=worker, tag=11)
                        print(f"[MASTER] Worker {worker} completed chunks {start}-{end-1}")
                    
                    print(f"[SUCCESS] File saved as: {save_filename}")
                    self.comm.send({'status': 'complete'}, dest=source_rank, tag=3)
            
            elif request['type'] == 'list':
                source_rank = request['source_rank']
                files = self.list_files()
                self.comm.send({'status': 'success', 'files': files}, dest=source_rank, tag=4)
            
            elif request['type'] == 'shutdown':
                print("\n[MASTER] Shutdown request received")
                for i in range(1, self.size):
                    self.comm.send({'type': 'shutdown'}, dest=i, tag=10)
                break
    
    def worker_process(self):
        print(f"[WORKER {self.rank}] Ready and waiting for tasks...")
        
        while True:
            work_data = self.comm.recv(source=0, tag=10)
            
            if work_data.get('type') == 'shutdown':
                print(f"[WORKER {self.rank}] Shutting down...")
                break
            
            source_rank = work_data['source_rank']
            start_chunk = work_data['start_chunk']
            end_chunk = work_data['end_chunk']
            filepath = work_data['filepath']
            
            print(f"[WORKER {self.rank}] Receiving chunks {start_chunk}-{end_chunk-1} from rank {source_rank}")
            
            temp_file = f"{filepath}.part{self.rank}"
            
            with open(temp_file, 'wb') as f:
                for chunk_num in range(start_chunk, end_chunk):
                    chunk_data = self.comm.recv(source=source_rank, tag=100 + chunk_num)
                    f.write(chunk_data['data'])
                    print(f"[WORKER {self.rank}] Received chunk {chunk_num}", end='\r')
            
            print(f"\n[WORKER {self.rank}] Completed receiving chunks")
            
            self.comm.send({'status': 'done'}, dest=0, tag=11)
    
    def receive_all_chunks(self, source_rank, filepath, num_chunks):
        with open(filepath, 'wb') as f:
            for chunk_num in range(num_chunks):
                chunk_data = self.comm.recv(source=source_rank, tag=100 + chunk_num)
                f.write(chunk_data['data'])
                progress = ((chunk_num + 1) / num_chunks) * 100
                print(f"[MASTER] Received chunk {chunk_num + 1}/{num_chunks} ({progress:.1f}%)", end='\r')
        print()
    
    def list_files(self):
        files = []
        if os.path.exists(self.upload_dir):
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
        return files

def main():
    server = MPIFileTransferServer()
    server.run()

if __name__ == "__main__":
    main()
