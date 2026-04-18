#!/usr/bin/env python3
"""
Task 1: Linux + File System
Set up a Linux environment and create a structured directory for a data pipeline project.
Implement file permissions, automate file movement using shell scripts, and log operations.
"""

import os
import shutil
import stat
import logging
from datetime import datetime
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/filesystem_operations.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class LinuxFileSystemManager:
    """Manages Linux-style file system operations for data pipeline projects."""
    
    def __init__(self, base_path="/mnt/okcomputer/output/app/data_pipeline"):
        self.base_path = Path(base_path)
        self.directories = {
            'raw': self.base_path / 'data' / 'raw',
            'processed': self.base_path / 'data' / 'processed',
            'staging': self.base_path / 'data' / 'staging',
            'archive': self.base_path / 'data' / 'archive',
            'logs': self.base_path / 'logs',
            'scripts': self.base_path / 'scripts',
            'config': self.base_path / 'config',
            'temp': self.base_path / 'temp'
        }
        
    def create_directory_structure(self):
        """Create structured directory for data pipeline project."""
        logger.info("Creating directory structure...")
        
        for name, path in self.directories.items():
            path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created directory: {path}")
            
        # Create subdirectories for different data types
        (self.directories['raw'] / 'csv').mkdir(exist_ok=True)
        (self.directories['raw'] / 'json').mkdir(exist_ok=True)
        (self.directories['raw'] / 'xml').mkdir(exist_ok=True)
        (self.directories['processed'] / 'parquet').mkdir(exist_ok=True)
        (self.directories['processed'] / 'reports').mkdir(exist_ok=True)
        
        logger.info("Directory structure created successfully!")
        return self.directories
    
    def set_permissions(self, path, owner_read=True, owner_write=True, owner_execute=True,
                       group_read=True, group_write=False, group_execute=True,
                       other_read=True, other_write=False, other_execute=False):
        """Set file permissions (Linux-style chmod simulation)."""
        mode = 0
        if owner_read: mode |= stat.S_IRUSR
        if owner_write: mode |= stat.S_IWUSR
        if owner_execute: mode |= stat.S_IXUSR
        if group_read: mode |= stat.S_IRGRP
        if group_write: mode |= stat.S_IWGRP
        if group_execute: mode |= stat.S_IXGRP
        if other_read: mode |= stat.S_IROTH
        if other_write: mode |= stat.S_IWOTH
        if other_execute: mode |= stat.S_IXOTH
        
        os.chmod(path, mode)
        logger.info(f"Set permissions {oct(mode)} on {path}")
        return mode
    
    def secure_directory(self, directory_path):
        """Apply secure permissions to sensitive directories."""
        # Logs directory - only owner can read/write
        self.set_permissions(directory_path, 
                           owner_read=True, owner_write=True, owner_execute=True,
                           group_read=False, group_write=False, group_execute=False,
                           other_read=False, other_write=False, other_execute=False)
        logger.info(f"Secured directory: {directory_path}")
    
    def move_files(self, source_pattern, destination, archive_source=False):
        """Automate file movement with optional archiving."""
        source_path = Path(source_pattern)
        dest_path = Path(destination)
        
        moved_files = []
        if source_path.is_file():
            files = [source_path]
        else:
            files = list(source_path.parent.glob(source_path.name))
        
        for file in files:
            if file.is_file():
                dest_file = dest_path / file.name
                shutil.move(str(file), str(dest_file))
                moved_files.append(dest_file)
                logger.info(f"Moved: {file} -> {dest_file}")
                
                if archive_source:
                    archive_path = self.directories['archive'] / file.name
                    shutil.copy2(str(dest_file), str(archive_path))
                    logger.info(f"Archived: {archive_path}")
        
        return moved_files
    
    def generate_shell_script(self):
        """Generate a shell script for automated file operations."""
        script_content = """#!/bin/bash
# Data Pipeline File Management Script
# Generated: {timestamp}

BASE_DIR="{base_dir}"
LOG_FILE="$BASE_DIR/logs/file_operations.log"

# Function to log messages
log_message() {{
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" >> "$LOG_FILE"
}}

# Create directories if they don't exist
mkdir -p "$BASE_DIR"/data/{{raw,processed,staging,archive}}
mkdir -p "$BASE_DIR"/logs
mkdir -p "$BASE_DIR"/scripts
mkdir -p "$BASE_DIR"/config

# Set permissions
chmod 755 "$BASE_DIR"/data/raw
chmod 750 "$BASE_DIR"/data/processed
chmod 700 "$BASE_DIR"/logs
chmod 755 "$BASE_DIR"/scripts

# Move processed files to archive
find "$BASE_DIR"/data/processed -name "*.csv" -mtime +7 -exec mv {{}} "$BASE_DIR"/data/archive/ \\;

log_message "File operations completed successfully"
""".format(timestamp=datetime.now().isoformat(), base_dir=self.base_path)
        
        script_path = self.directories['scripts'] / 'file_manager.sh'
        with open(script_path, 'w') as f:
            f.write(script_content)
        
        self.set_permissions(script_path, owner_read=True, owner_write=True, owner_execute=True,
                           group_read=True, group_execute=True, other_read=True, other_execute=True)
        
        logger.info(f"Generated shell script: {script_path}")
        return script_path
    
    def get_directory_tree(self):
        """Get the directory structure as a tree."""
        tree = []
        for root, dirs, files in os.walk(self.base_path):
            level = root.replace(str(self.base_path), '').count(os.sep)
            indent = ' ' * 2 * level
            tree.append(f'{indent}{os.path.basename(root)}/')
            subindent = ' ' * 2 * (level + 1)
            for file in files[:5]:  # Limit to 5 files per directory
                tree.append(f'{subindent}{file}')
            if len(files) > 5:
                tree.append(f'{subindent}... and {len(files) - 5} more files')
        return '\n'.join(tree)


def main():
    """Main execution for Task 1."""
    # Create logs directory
    Path('logs').mkdir(exist_ok=True)
    
    # Initialize file system manager
    fs_manager = LinuxFileSystemManager()
    
    # Create directory structure
    directories = fs_manager.create_directory_structure()
    
    # Set secure permissions on logs
    fs_manager.secure_directory(directories['logs'])
    
    # Generate shell script
    script_path = fs_manager.generate_shell_script()
    
    # Create sample files for demonstration
    sample_file = directories['raw'] / 'csv' / 'sample_data.csv'
    with open(sample_file, 'w') as f:
        f.write("id,name,value\n1,Test,100\n2,Sample,200\n")
    
    logger.info("Task 1 completed successfully!")
    
    return {
        'directories': directories,
        'script_path': script_path,
        'directory_tree': fs_manager.get_directory_tree()
    }


if __name__ == "__main__":
    result = main()
    print("\n=== Directory Structure ===")
    print(result['directory_tree'])
