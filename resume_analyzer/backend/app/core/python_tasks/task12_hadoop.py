#!/usr/bin/env python3
"""
Task 12: Hadoop
Set up HDFS locally and upload structured/unstructured data.

Real implementation:
  - Connects to live HDFS via 'hdfs' Python client (pip install hdfs)
  - Targets the Hadoop NameNode running in Docker (http://localhost:9870)
  - Falls back to local filesystem simulation if HDFS is unavailable

Docker setup (required for real mode):
  docker-compose --profile hadoop up
  # NameNode Web UI: http://localhost:9870
  # HDFS WebHDFS API: http://localhost:9870/webhdfs/v1/
"""

import os
import json
import logging
import shutil
import hashlib
import random
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── Try real HDFS client ──────────────────────────────────────────────────────
try:
    from hdfs import InsecureClient
    from hdfs.util import HdfsError
    HDFS_AVAILABLE = True
    logger.info("hdfs library found — will attempt real HDFS connection")
except ImportError:
    HDFS_AVAILABLE = False
    logger.warning("hdfs library not installed. Run: pip install hdfs")
    logger.warning("Falling back to local filesystem simulation.")

HDFS_URL = os.getenv("HDFS_URL", "http://localhost:9870")
HDFS_USER = os.getenv("HDFS_USER", "root")


# ═══════════════════════════════════════════════════════════════════════════════
# REAL HDFS CLIENT (uses hdfs Python library → WebHDFS REST API)
# ═══════════════════════════════════════════════════════════════════════════════

class RealHDFSClient:
    """
    Connects to Apache Hadoop HDFS via the WebHDFS REST API.
    Requires: pip install hdfs
    Requires: docker-compose --profile hadoop up
    """

    def __init__(self, url: str = HDFS_URL, user: str = HDFS_USER):
        self.url = url
        self.user = user
        self.client = InsecureClient(url, user=user)
        logger.info(f"Connected to HDFS at {url} as user '{user}'")

    def _ensure_dir(self, hdfs_path: str):
        """Create directory on HDFS if it doesn't exist."""
        try:
            self.client.makedirs(hdfs_path)
        except HdfsError:
            pass  # Already exists

    def mkdir(self, path: str) -> bool:
        """Create a directory in HDFS."""
        self.client.makedirs(path)
        logger.info(f"HDFS mkdir: {path}")
        return True

    def ls(self, path: str = "/") -> List[Dict]:
        """List directory contents on HDFS."""
        try:
            items = self.client.list(path, status=True)
            result = []
            for name, status in items:
                result.append({
                    "name": name,
                    "type": status["type"],
                    "size": status.get("length", 0),
                    "permissions": status.get("permission", "---"),
                    "replication": status.get("replication", 0),
                    "modified": datetime.fromtimestamp(
                        status.get("modificationTime", 0) / 1000
                    ).isoformat(),
                })
            return result
        except HdfsError as e:
            logger.error(f"HDFS ls failed: {e}")
            return []

    def put(self, local_path: str, hdfs_path: str, overwrite: bool = True) -> Dict:
        """Upload a local file to HDFS."""
        local_file = Path(local_path)
        if not local_file.exists():
            raise FileNotFoundError(f"Local file not found: {local_path}")

        self._ensure_dir(hdfs_path if hdfs_path.endswith("/") else str(Path(hdfs_path).parent))

        dest = hdfs_path if not hdfs_path.endswith("/") else f"{hdfs_path.rstrip('/')}/{local_file.name}"

        with open(local_path, "rb") as f:
            self.client.write(dest, f, overwrite=overwrite)

        file_size = local_file.stat().st_size
        block_size = 128 * 1024 * 1024  # 128 MB default
        num_blocks = max(1, (file_size + block_size - 1) // block_size)

        logger.info(f"Uploaded {local_file.name} → HDFS:{dest} ({file_size} bytes, {num_blocks} block(s))")
        return {
            "source": local_path,
            "destination": dest,
            "size": file_size,
            "blocks": num_blocks,
            "replication_factor": 3,
            "hdfs_url": f"{self.url}/webhdfs/v1{dest}?op=OPEN",
        }

    def get(self, hdfs_path: str, local_path: str) -> Dict:
        """Download a file from HDFS."""
        Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        with self.client.read(hdfs_path) as reader, open(local_path, "wb") as f:
            data = reader.read()
            f.write(data)
        logger.info(f"Downloaded HDFS:{hdfs_path} → {local_path} ({len(data)} bytes)")
        return {"source": hdfs_path, "destination": local_path, "size": len(data)}

    def cat(self, hdfs_path: str) -> str:
        """Read file contents from HDFS."""
        with self.client.read(hdfs_path, encoding="utf-8") as reader:
            return reader.read()

    def rm(self, hdfs_path: str, recursive: bool = False) -> bool:
        """Remove a file or directory from HDFS."""
        self.client.delete(hdfs_path, recursive=recursive)
        logger.info(f"Deleted HDFS:{hdfs_path}")
        return True

    def df(self) -> Dict:
        """Get HDFS filesystem summary (calls NameNode REST API)."""
        import urllib.request
        url = f"{self.url}/webhdfs/v1/?op=GETCONTENTSUMMARY"
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                data = json.loads(resp.read())
            summary = data.get("ContentSummary", {})
            return {
                "filesystem": self.url,
                "space_consumed": summary.get("spaceConsumed", 0),
                "quota": summary.get("quota", -1),
                "space_quota": summary.get("spaceQuota", -1),
                "file_count": summary.get("fileCount", 0),
                "directory_count": summary.get("directoryCount", 0),
            }
        except Exception as e:
            return {"filesystem": self.url, "error": str(e)}

    def namenode_status(self) -> Dict:
        """Get NameNode status via JMX REST endpoint."""
        import urllib.request
        url = f"{self.url}/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus"
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                data = json.loads(resp.read())
            beans = data.get("beans", [{}])
            return {
                "state": beans[0].get("State", "unknown"),
                "namenode_id": beans[0].get("NamenodeId", "nn1"),
                "hdfs_url": self.url,
            }
        except Exception as e:
            return {"state": "unreachable", "error": str(e)}


# ═══════════════════════════════════════════════════════════════════════════════
# LOCAL SIMULATION FALLBACK (no Docker needed)
# Mirrors real HDFS semantics: NameNode metadata, DataNode block replication
# ═══════════════════════════════════════════════════════════════════════════════

class SimulatedHDFS:
    """
    Local filesystem simulation of HDFS.
    Mimics: NameNode fsimage metadata, 3x block replication across DataNodes,
    block splitting at 128 MB, WebHDFS-compatible response shapes.
    """

    BLOCK_SIZE = 128 * 1024 * 1024  # 128 MB
    REPLICATION = 3

    def __init__(self, root: str = "data/hdfs_simulation"):
        self.root = Path(root)
        self._fsimage: Dict = {}  # NameNode metadata
        self._init_cluster()

    def _init_cluster(self):
        """Initialise NameNode + 3 DataNode directories."""
        for d in ["namenode", "datanode/dn1", "datanode/dn2", "datanode/dn3",
                  "tmp", "user/root"]:
            (self.root / d).mkdir(parents=True, exist_ok=True)

        fsimage_path = self.root / "namenode" / "fsimage.json"
        if fsimage_path.exists():
            with open(fsimage_path) as f:
                self._fsimage = json.load(f)
        else:
            self._fsimage = {
                "cluster_id": f"CID-{hashlib.md5(str(self.root).encode()).hexdigest()[:8]}",
                "format_time": datetime.now().isoformat(),
                "block_size": self.BLOCK_SIZE,
                "replication": self.REPLICATION,
                "files": {},
                "datanodes": {
                    "dn1": {"host": "datanode1:50010", "capacity_gb": 100, "used_mb": 0},
                    "dn2": {"host": "datanode2:50010", "capacity_gb": 100, "used_mb": 0},
                    "dn3": {"host": "datanode3:50010", "capacity_gb": 100, "used_mb": 0},
                },
            }
            self._save_fsimage()
        logger.info(f"HDFS cluster ready (simulation). ClusterID={self._fsimage['cluster_id']}")

    def _save_fsimage(self):
        with open(self.root / "namenode" / "fsimage.json", "w") as f:
            json.dump(self._fsimage, f, indent=2)

    def mkdir(self, path: str) -> bool:
        (self.root / "user" / path.lstrip("/")).mkdir(parents=True, exist_ok=True)
        logger.info(f"HDFS mkdir: {path}")
        return True

    def ls(self, path: str = "/") -> List[Dict]:
        target = self.root / "user" / path.lstrip("/")
        if not target.exists():
            return []
        result = []
        for item in target.iterdir():
            st = item.stat()
            result.append({
                "name": item.name,
                "type": "DIRECTORY" if item.is_dir() else "FILE",
                "size": st.st_size if item.is_file() else 0,
                "permissions": oct(st.st_mode)[-3:],
                "replication": self.REPLICATION if item.is_file() else 0,
                "modified": datetime.fromtimestamp(st.st_mtime).isoformat(),
            })
        return result

    def put(self, local_path: str, hdfs_path: str) -> Dict:
        src = Path(local_path)
        if not src.exists():
            raise FileNotFoundError(f"Not found: {local_path}")

        dest_dir = self.root / "user" / hdfs_path.lstrip("/")
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_file = dest_dir / src.name
        shutil.copy2(src, dest_file)

        size = src.stat().st_size
        num_blocks = max(1, (size + self.BLOCK_SIZE - 1) // self.BLOCK_SIZE)

        # Simulate block replication across 3 DataNodes
        blocks = []
        for i in range(num_blocks):
            block_id = f"blk_{hashlib.md5(f'{dest_file}_{i}'.encode()).hexdigest()[:16]}"
            replicas = []
            for dn in ["dn1", "dn2", "dn3"]:
                replica_path = self.root / "datanode" / dn / block_id
                replica_path.write_text(f"[block {i} of {src.name}]")
                replicas.append({"datanode": dn, "block_id": block_id})
                self._fsimage["datanodes"][dn]["used_mb"] += size // (1024 * 1024 * num_blocks)
            blocks.append({"block_num": i, "block_id": block_id, "replicas": replicas})

        self._fsimage["files"][str(dest_file)] = {
            "hdfs_path": f"{hdfs_path}/{src.name}",
            "size": size,
            "blocks": num_blocks,
            "block_ids": [b["block_id"] for b in blocks],
            "replication": self.REPLICATION,
            "upload_time": datetime.now().isoformat(),
            "permissions": "rw-r--r--",
        }
        self._save_fsimage()

        logger.info(f"Uploaded {src.name} → HDFS:{hdfs_path} ({size} bytes, {num_blocks} block(s), 3x replication)")
        return {"source": local_path, "destination": f"{hdfs_path}/{src.name}",
                "size": size, "blocks": blocks}

    def get(self, hdfs_path: str, local_path: str) -> Dict:
        src = self.root / "user" / hdfs_path.lstrip("/")
        if not src.exists():
            raise FileNotFoundError(f"HDFS path not found: {hdfs_path}")
        dest = Path(local_path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)
        logger.info(f"Downloaded HDFS:{hdfs_path} → {local_path}")
        return {"source": hdfs_path, "destination": local_path, "size": src.stat().st_size}

    def cat(self, hdfs_path: str) -> str:
        f = self.root / "user" / hdfs_path.lstrip("/")
        if not f.exists():
            raise FileNotFoundError(hdfs_path)
        return f.read_text()

    def rm(self, hdfs_path: str, recursive: bool = False) -> bool:
        target = self.root / "user" / hdfs_path.lstrip("/")
        if not target.exists():
            return False
        if target.is_dir() and recursive:
            shutil.rmtree(target)
        elif target.is_file():
            target.unlink()
        else:
            raise ValueError("Use recursive=True to delete directories")
        self._fsimage["files"] = {
            k: v for k, v in self._fsimage["files"].items()
            if not k.startswith(str(target))
        }
        self._save_fsimage()
        logger.info(f"Deleted HDFS:{hdfs_path}")
        return True

    def df(self) -> Dict:
        total_gb = sum(d["capacity_gb"] for d in self._fsimage["datanodes"].values())
        used_mb = sum(d["used_mb"] for d in self._fsimage["datanodes"].values())
        return {
            "filesystem": str(self.root),
            "total_capacity_gb": total_gb,
            "used_mb": round(used_mb, 2),
            "available_gb": round(total_gb - used_mb / 1024, 2),
            "use_percent": round(used_mb / (total_gb * 1024) * 100, 2),
            "datanodes": self._fsimage["datanodes"],
            "cluster_id": self._fsimage["cluster_id"],
        }

    def namenode_status(self) -> Dict:
        return {
            "state": "active",
            "cluster_id": self._fsimage["cluster_id"],
            "format_time": self._fsimage["format_time"],
            "total_files": len(self._fsimage["files"]),
            "datanodes_live": 3,
            "block_size_mb": self.BLOCK_SIZE // (1024 * 1024),
            "replication_factor": self.REPLICATION,
            "mode": "SIMULATION (start Docker for real HDFS)",
        }


# ═══════════════════════════════════════════════════════════════════════════════
# FACTORY — picks real or simulated client automatically
# ═══════════════════════════════════════════════════════════════════════════════

def get_hdfs_client(url: str = HDFS_URL, user: str = HDFS_USER):
    """
    Return a real HDFS client if Docker is running and hdfs library is installed,
    otherwise return the local simulation.
    """
    if HDFS_AVAILABLE:
        try:
            client = RealHDFSClient(url, user)
            # Quick connectivity test
            client.ls("/")
            logger.info("✅ Connected to REAL HDFS at %s", url)
            return client, "real"
        except Exception as e:
            logger.warning("Real HDFS unavailable (%s). Using simulation.", e)

    logger.info("🔵 Using HDFS simulation (local filesystem)")
    return SimulatedHDFS(), "simulation"


# ═══════════════════════════════════════════════════════════════════════════════
# SAMPLE DATA GENERATOR
# ═══════════════════════════════════════════════════════════════════════════════

def generate_sample_data(data_dir: str = "data/hdfs_input") -> Path:
    """Generate structured, semi-structured, unstructured, and log data."""
    out = Path(data_dir)
    out.mkdir(parents=True, exist_ok=True)

    # Structured — CSV
    csv_lines = ["id,name,age,city,skill\n"]
    cities = ["Mumbai", "Delhi", "Bangalore", "Hyderabad", "Pune"]
    skills = ["Python", "Java", "SQL", "AWS", "Spark"]
    for i in range(1000):
        csv_lines.append(f"{i},Candidate_{i},{random.randint(22,45)},"
                         f"{random.choice(cities)},{random.choice(skills)}\n")
    (out / "structured_resumes.csv").write_text("".join(csv_lines))

    # Semi-structured — JSON
    records = [{"id": i, "name": f"Candidate_{i}",
                "skills": random.sample(skills, random.randint(1, 4)),
                "experience_years": random.randint(0, 15)} for i in range(200)]
    (out / "semi_structured_profiles.json").write_text(json.dumps(records, indent=2))

    # Unstructured — text
    (out / "unstructured_resumes.txt").write_text(
        "\n\n".join(
            f"CANDIDATE {i}\nExperience: {random.randint(1,15)} years\n"
            f"Skills: {', '.join(random.sample(skills, 3))}"
            for i in range(100)
        )
    )

    # Log data
    levels = ["INFO", "WARN", "ERROR"]
    (out / "pipeline_logs.log").write_text(
        "\n".join(
            f"{datetime.now().isoformat()} [{random.choice(levels)}] Pipeline event {i}"
            for i in range(500)
        )
    )

    logger.info("Generated sample data in %s", out)
    return out


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN DEMO
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    """Run complete Task 12 — HDFS setup and data upload demonstration."""
    print("\n" + "=" * 60)
    print("TASK 12: Hadoop / HDFS")
    print("=" * 60)

    hdfs, mode = get_hdfs_client()
    print(f"\n▶  Mode: {mode.upper()}")
    if mode == "real":
        print(f"   NameNode: {HDFS_URL}")
        print(f"   WebHDFS UI: {HDFS_URL}/dfshealth.html")

    # NameNode status
    status = hdfs.namenode_status()
    print(f"\n📊 NameNode Status:")
    for k, v in status.items():
        print(f"   {k}: {v}")

    # Create directory structure
    print("\n📁 Creating HDFS directory structure...")
    for path in ["/data", "/data/structured", "/data/unstructured",
                 "/data/logs", "/data/processed"]:
        hdfs.mkdir(path)
        print(f"   Created: {path}")

    # Generate and upload data
    data_dir = generate_sample_data()

    uploads = {}
    file_map = {
        "structured_resumes.csv": "/data/structured",
        "semi_structured_profiles.json": "/data/structured",
        "unstructured_resumes.txt": "/data/unstructured",
        "pipeline_logs.log": "/data/logs",
    }

    print("\n⬆  Uploading files to HDFS...")
    for filename, hdfs_dir in file_map.items():
        result = hdfs.put(str(data_dir / filename), hdfs_dir)
        uploads[filename] = result
        print(f"   ✅ {filename}: {result['size']:,} bytes")

    # List directories
    print("\n📋 HDFS /data/structured contents:")
    for item in hdfs.ls("/data/structured"):
        print(f"   {item['type']:10s}  {item['name']:40s}  {item.get('size', 0):>10,} bytes")

    # Filesystem stats
    stats = hdfs.df()
    print(f"\n💾 Filesystem Stats:")
    for k, v in stats.items():
        if k != "datanodes":
            print(f"   {k}: {v}")

    # Read back a file
    print("\n📖 Reading back structured_resumes.csv (first 3 lines):")
    content = hdfs.cat("/data/structured/structured_resumes.csv")
    for line in content.splitlines()[:3]:
        print(f"   {line}")

    print(f"\n✅ Task 12 complete — {len(uploads)} files uploaded to HDFS")
    return {"mode": mode, "uploads": uploads, "namenode_status": status, "fs_stats": stats}


if __name__ == "__main__":
    main()
