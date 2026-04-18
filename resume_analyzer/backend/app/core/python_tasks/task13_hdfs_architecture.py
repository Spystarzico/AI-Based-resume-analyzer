#!/usr/bin/env python3
"""
Task 13: HDFS Architecture
Explain Namenode, Datanode and simulate data storage.
"""

import json
import logging
from datetime import datetime
from typing import Dict, List, Any
from pathlib import Path
import hashlib
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class HDFSNamenode:
    """Simulate HDFS Namenode - manages filesystem metadata."""
    
    def __init__(self):
        self.namespace = {}  # File system namespace
        self.blocks_map = {}  # Block to datanode mapping
        self.datanodes = {}  # Registered datanodes
        self.edit_log = []  # Edit log for durability
        self.fsimage = {}  # Checkpoint of namespace
        
    def format(self):
        """Format the namenode."""
        logger.info("Formatting namenode...")
        self.namespace = {'/': {'type': 'directory', 'children': {}}}
        self.blocks_map = {}
        self.datanodes = {}
        self.edit_log = []
        self.fsimage = {
            'format_time': datetime.now().isoformat(),
            'namespace': self.namespace,
            'blocks_map': self.blocks_map
        }
        logger.info("Namenode formatted successfully")
    
    def register_datanode(self, datanode_id: str, capacity: int, ip: str, port: int):
        """Register a new datanode."""
        self.datanodes[datanode_id] = {
            'id': datanode_id,
            'ip': ip,
            'port': port,
            'capacity': capacity,
            'used': 0,
            'blocks': [],
            'last_heartbeat': datetime.now().isoformat(),
            'status': 'active'
        }
        logger.info(f"Registered datanode: {datanode_id} ({ip}:{port})")
    
    def create_file(self, path: str, replication: int = 3) -> Dict:
        """Create a new file entry in namespace."""
        dirs = path.strip('/').split('/')
        current = self.namespace['/']
        
        for dir_name in dirs[:-1]:
            if dir_name not in current['children']:
                current['children'][dir_name] = {
                    'type': 'directory',
                    'children': {}
                }
            current = current['children'][dir_name]
        
        filename = dirs[-1]
        file_entry = {
            'type': 'file',
            'size': 0,
            'blocks': [],
            'replication': replication,
            'created': datetime.now().isoformat(),
            'modified': datetime.now().isoformat(),
            'permissions': '644'
        }
        
        current['children'][filename] = file_entry
        
        self.edit_log.append({
            'operation': 'CREATE',
            'path': path,
            'timestamp': datetime.now().isoformat()
        })
        
        logger.info(f"Created file: {path}")
        return file_entry
    
    def allocate_blocks(self, file_path: str, num_blocks: int, block_size: int) -> List[Dict]:
        """Allocate blocks for a file and assign to datanodes."""
        blocks = []
        
        for i in range(num_blocks):
            block_id = f"blk_{hashlib.md5(f'{file_path}_{i}_{datetime.now()}'.encode()).hexdigest()[:16]}"
            
            # Select datanodes for replication (rack-aware in real HDFS)
            available_datanodes = list(self.datanodes.keys())
            replication = min(3, len(available_datanodes))
            selected_datanodes = random.sample(available_datanodes, replication)
            
            block_info = {
                'block_id': block_id,
                'block_num': i,
                'size': block_size,
                'replicas': selected_datanodes,
                'file_path': file_path
            }
            
            self.blocks_map[block_id] = block_info
            
            # Update datanode block lists
            for dn_id in selected_datanodes:
                self.datanodes[dn_id]['blocks'].append(block_id)
            
            blocks.append(block_info)
        
        # Update file entry
        self._update_file_blocks(file_path, blocks)
        
        return blocks
    
    def _update_file_blocks(self, file_path: str, blocks: List[Dict]):
        """Update file entry with block information."""
        dirs = file_path.strip('/').split('/')
        current = self.namespace['/']
        
        for dir_name in dirs[:-1]:
            current = current['children'][dir_name]
        
        filename = dirs[-1]
        current['children'][filename]['blocks'] = [b['block_id'] for b in blocks]
    
    def get_block_locations(self, block_id: str) -> List[str]:
        """Get datanode locations for a block."""
        if block_id in self.blocks_map:
            return self.blocks_map[block_id]['replicas']
        return []
    
    def heartbeat(self, datanode_id: str):
        """Process datanode heartbeat."""
        if datanode_id in self.datanodes:
            self.datanodes[datanode_id]['last_heartbeat'] = datetime.now().isoformat()
    
    def get_namespace_tree(self) -> Dict:
        """Get the filesystem namespace tree."""
        return self.namespace
    
    def get_metadata(self) -> Dict:
        """Get namenode metadata."""
        return {
            'namespace_size': len(self.namespace),
            'total_blocks': len(self.blocks_map),
            'registered_datanodes': len(self.datanodes),
            'datanodes': self.datanodes,
            'edit_log_size': len(self.edit_log)
        }


class HDFSDatanode:
    """Simulate HDFS Datanode - stores actual data blocks."""
    
    def __init__(self, datanode_id: str, storage_path: str, capacity: int):
        self.datanode_id = datanode_id
        self.storage_path = Path(storage_path)
        self.capacity = capacity
        self.used = 0
        self.blocks = {}
        self.storage_path.mkdir(parents=True, exist_ok=True)
        
    def write_block(self, block_id: str, data: bytes) -> bool:
        """Write a block to storage."""
        block_path = self.storage_path / block_id
        
        with open(block_path, 'wb') as f:
            f.write(data)
        
        self.blocks[block_id] = {
            'size': len(data),
            'written': datetime.now().isoformat()
        }
        self.used += len(data)
        
        logger.info(f"Datanode {self.datanode_id}: Wrote block {block_id} ({len(data)} bytes)")
        return True
    
    def read_block(self, block_id: str) -> bytes:
        """Read a block from storage."""
        block_path = self.storage_path / block_id
        
        if not block_path.exists():
            raise FileNotFoundError(f"Block not found: {block_id}")
        
        with open(block_path, 'rb') as f:
            return f.read()
    
    def delete_block(self, block_id: str) -> bool:
        """Delete a block from storage."""
        block_path = self.storage_path / block_id
        
        if block_path.exists():
            size = block_path.stat().st_size
            block_path.unlink()
            self.used -= size
            del self.blocks[block_id]
            logger.info(f"Datanode {self.datanode_id}: Deleted block {block_id}")
            return True
        return False
    
    def get_block_report(self) -> Dict:
        """Get block report for namenode."""
        return {
            'datanode_id': self.datanode_id,
            'capacity': self.capacity,
            'used': self.used,
            'available': self.capacity - self.used,
            'num_blocks': len(self.blocks),
            'blocks': list(self.blocks.keys())
        }
    
    def send_heartbeat(self) -> Dict:
        """Send heartbeat to namenode."""
        return {
            'datanode_id': self.datanode_id,
            'status': 'active',
            'timestamp': datetime.now().isoformat(),
            'capacity': self.capacity,
            'used': self.used
        }


class HDFSFederation:
    """Simulate HDFS with multiple namenodes (federation)."""
    
    def __init__(self):
        self.namenodes = {}
        self.datanodes = {}
        
    def add_namenode(self, namenode_id: str, namespace: str):
        """Add a namenode for a specific namespace."""
        namenode = HDFSNamenode()
        namenode.format()
        
        self.namenodes[namenode_id] = {
            'instance': namenode,
            'namespace': namespace,
            'manages': f'/{namespace}'
        }
        
        logger.info(f"Added namenode {namenode_id} for namespace: {namespace}")
    
    def add_datanode(self, datanode_id: str, storage_path: str, capacity: int):
        """Add a datanode to the cluster."""
        datanode = HDFSDatanode(datanode_id, storage_path, capacity)
        self.datanodes[datanode_id] = datanode
        
        # Register with all namenodes
        for nn_id, nn_info in self.namenodes.items():
            nn_info['instance'].register_datanode(
                datanode_id, capacity, 
                f'10.0.0.{len(self.datanodes)}', 50010 + len(self.datanodes)
            )
        
        logger.info(f"Added datanode: {datanode_id}")
    
    def write_file(self, namenode_id: str, file_path: str, data: bytes, block_size: int = 64*1024*1024):
        """Write a file to HDFS."""
        namenode = self.namenodes[namenode_id]['instance']
        
        # Create file entry
        file_entry = namenode.create_file(file_path)
        
        # Calculate blocks
        num_blocks = (len(data) + block_size - 1) // block_size
        
        # Allocate blocks
        blocks = namenode.allocate_blocks(file_path, num_blocks, block_size)
        
        # Write data to datanodes
        for i, block in enumerate(blocks):
            block_data = data[i*block_size:(i+1)*block_size]
            
            for datanode_id in block['replicas']:
                if datanode_id in self.datanodes:
                    self.datanodes[datanode_id].write_block(block['block_id'], block_data)
        
        return blocks
    
    def get_architecture_info(self) -> Dict:
        """Get HDFS architecture information."""
        return {
            'namenode_role': {
                'description': 'Manages filesystem namespace and regulates access to files',
                'responsibilities': [
                    'Maintains filesystem tree and metadata',
                    'Manages file-to-block mapping',
                    'Tracks block locations on datanodes',
                    'Handles client requests for file operations',
                    'Manages replication factor',
                    'Maintains edit log for durability'
                ],
                'components': {
                    'fsimage': 'Persistent checkpoint of namespace',
                    'edit_log': 'Log of all namespace modifications',
                    'in_memory_metadata': 'Runtime namespace in RAM'
                }
            },
            'datanode_role': {
                'description': 'Stores actual data blocks and serves read/write requests',
                'responsibilities': [
                    'Stores data blocks on local filesystem',
                    'Serves read/write requests from clients',
                    'Sends periodic heartbeats to namenode',
                    'Sends block reports to namenode',
                    'Handles block replication as instructed',
                    'Performs checksum verification'
                ],
                'processes': {
                    'heartbeat': 'Every 3 seconds to report health',
                    'block_report': 'Every 6 hours to report all blocks',
                    'block_scanner': 'Periodic integrity verification'
                }
            },
            'write_pipeline': {
                'steps': [
                    '1. Client contacts namenode for block allocation',
                    '2. Namenode returns list of datanodes for replication',
                    '3. Client writes to first datanode',
                    '4. First datanode forwards to second, second to third',
                    '5. Acknowledgments flow back to client',
                    '6. Client confirms write completion to namenode'
                ]
            },
            'read_pipeline': {
                'steps': [
                    '1. Client contacts namenode for block locations',
                    '2. Namenode returns sorted list by proximity',
                    '3. Client reads from closest datanode',
                    '4. Checksum verified during read',
                    '5. On failure, client tries next replica'
                ]
            },
            'high_availability': {
                'active_namenode': 'Handles all client operations',
                'standby_namenode': 'Maintains synchronized state',
                'journal_nodes': 'Share edit logs between namenodes',
                'zookeeper': 'Manages failover coordination',
                'fencing': 'Prevents split-brain scenario'
            }
        }


def main():
    """Main execution for Task 13."""
    # Create HDFS federation
    hdfs = HDFSFederation()
    
    # Add namenodes (federation)
    hdfs.add_namenode('nn1', 'data')
    hdfs.add_namenode('nn2', 'users')
    
    # Add datanodes
    hdfs.add_datanode('dn1', 'data/hdfs/datanode1', 100*1024*1024*1024)  # 100GB
    hdfs.add_datanode('dn2', 'data/hdfs/datanode2', 100*1024*1024*1024)
    hdfs.add_datanode('dn3', 'data/hdfs/datanode3', 100*1024*1024*1024)
    
    # Write a file
    test_data = b"Hello, HDFS! This is test data for demonstration. " * 1000
    blocks = hdfs.write_file('nn1', '/data/test_file.txt', test_data, block_size=1024)
    
    # Get namenode metadata
    nn_metadata = hdfs.namenodes['nn1']['instance'].get_metadata()
    
    # Get datanode reports
    dn_reports = [dn.get_block_report() for dn in hdfs.datanodes.values()]
    
    # Get architecture info
    architecture = hdfs.get_architecture_info()
    
    logger.info("Task 13 completed successfully!")
    
    print("\n=== HDFS Architecture ===")
    print("\nNamenode Responsibilities:")
    for resp in architecture['namenode_role']['responsibilities']:
        print(f"  - {resp}")
    
    print("\nDatanode Responsibilities:")
    for resp in architecture['datanode_role']['responsibilities']:
        print(f"  - {resp}")
    
    print("\n=== Cluster Status ===")
    print(f"Namenodes: {len(hdfs.namenodes)}")
    print(f"Datanodes: {len(hdfs.datanodes)}")
    print(f"Total Blocks: {nn_metadata['total_blocks']}")
    
    print("\n=== Write Pipeline ===")
    for step in architecture['write_pipeline']['steps']:
        print(f"  {step}")
    
    return {
        'architecture': architecture,
        'namenode_metadata': nn_metadata,
        'datanode_reports': dn_reports,
        'file_blocks': blocks
    }


if __name__ == "__main__":
    main()
