#!/usr/bin/env python3
"""
Task 25: Cloud Storage
Upload and retrieve files from S3/GCS.

Real implementation:
  - AWS S3: uses boto3 (pip install boto3)
  - MinIO (local S3-compatible): uses boto3 pointed at localhost:9000
  - GCS: uses google-cloud-storage (pip install google-cloud-storage)
  - Falls back to in-memory simulation if libraries/credentials unavailable

Docker setup for local S3 (MinIO):
  docker-compose --profile cloud up
  # MinIO Console: http://localhost:9001  (minio_admin / minio_password)
  # MinIO S3 API:  http://localhost:9000
"""

import io
import json
import logging
import hashlib
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── Try real cloud libraries ──────────────────────────────────────────────────
try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError, EndpointResolutionError
    from botocore.config import Config
    BOTO3_AVAILABLE = True
    logger.info("boto3 found — S3/MinIO operations available")
except ImportError:
    BOTO3_AVAILABLE = False
    logger.warning("boto3 not installed. Run: pip install boto3")

try:
    from google.cloud import storage as gcs_storage
    from google.api_core.exceptions import GoogleAPIError
    GCS_AVAILABLE = True
    logger.info("google-cloud-storage found — GCS operations available")
except ImportError:
    GCS_AVAILABLE = False
    logger.warning("google-cloud-storage not installed. Run: pip install google-cloud-storage")

# MinIO local config (matches docker-compose.yml)
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_password")
MINIO_REGION     = "us-east-1"

# AWS config (real credentials from environment)
AWS_REGION       = os.getenv("AWS_DEFAULT_REGION", "us-east-1")


# ═══════════════════════════════════════════════════════════════════════════════
# REAL S3 CLIENT (boto3 → AWS S3 or MinIO)
# ═══════════════════════════════════════════════════════════════════════════════

class S3Client:
    """
    Real S3 client using boto3.
    Supports both AWS S3 (via env credentials) and
    MinIO (local Docker, S3-compatible).
    """

    def __init__(self, use_minio: bool = True):
        """
        use_minio=True  → connect to local MinIO (Docker, no AWS account needed)
        use_minio=False → connect to real AWS S3 (needs AWS credentials)
        """
        self.use_minio = use_minio
        if use_minio:
            self.s3 = boto3.client(
                "s3",
                endpoint_url=MINIO_ENDPOINT,
                aws_access_key_id=MINIO_ACCESS_KEY,
                aws_secret_access_key=MINIO_SECRET_KEY,
                region_name=MINIO_REGION,
                config=Config(signature_version="s3v4"),
            )
            self.provider = "MinIO (local Docker)"
        else:
            self.s3 = boto3.client("s3", region_name=AWS_REGION)
            self.provider = "AWS S3"
        logger.info(f"S3Client initialised — provider: {self.provider}")

    def create_bucket(self, bucket_name: str, region: str = None) -> Dict:
        """Create an S3 bucket."""
        try:
            if self.use_minio or region in (None, "us-east-1"):
                self.s3.create_bucket(Bucket=bucket_name)
            else:
                self.s3.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={"LocationConstraint": region or AWS_REGION},
                )
            logger.info(f"Created bucket: {bucket_name}")
            return {"bucket": bucket_name, "status": "created", "provider": self.provider}
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                logger.info(f"Bucket already exists: {bucket_name}")
                return {"bucket": bucket_name, "status": "already_exists"}
            raise

    def enable_versioning(self, bucket_name: str) -> Dict:
        """Enable S3 versioning on a bucket."""
        self.s3.put_bucket_versioning(
            Bucket=bucket_name,
            VersioningConfiguration={"Status": "Enabled"},
        )
        logger.info(f"Versioning enabled on {bucket_name}")
        return {"bucket": bucket_name, "versioning": "Enabled"}

    def upload_file(self, bucket_name: str, key: str, data: bytes,
                    metadata: Dict = None, content_type: str = "application/octet-stream") -> Dict:
        """Upload bytes to S3."""
        extra = {"ContentType": content_type}
        if metadata:
            extra["Metadata"] = {k: str(v) for k, v in metadata.items()}

        self.s3.put_object(Bucket=bucket_name, Key=key, Body=data, **extra)
        etag = hashlib.md5(data).hexdigest()
        logger.info(f"Uploaded s3://{bucket_name}/{key} ({len(data):,} bytes)")
        return {"bucket": bucket_name, "key": key, "size": len(data), "etag": etag}

    def upload_file_from_path(self, bucket_name: str, key: str, local_path: str) -> Dict:
        """Upload a local file to S3."""
        path = Path(local_path)
        self.s3.upload_file(str(path), bucket_name, key)
        size = path.stat().st_size
        logger.info(f"Uploaded {local_path} → s3://{bucket_name}/{key} ({size:,} bytes)")
        return {"bucket": bucket_name, "key": key, "size": size}

    def download_file(self, bucket_name: str, key: str) -> bytes:
        """Download an S3 object as bytes."""
        response = self.s3.get_object(Bucket=bucket_name, Key=key)
        data = response["Body"].read()
        logger.info(f"Downloaded s3://{bucket_name}/{key} ({len(data):,} bytes)")
        return data

    def download_to_path(self, bucket_name: str, key: str, local_path: str) -> Dict:
        """Download an S3 object to a local file."""
        Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        self.s3.download_file(bucket_name, key, local_path)
        size = Path(local_path).stat().st_size
        logger.info(f"Downloaded s3://{bucket_name}/{key} → {local_path}")
        return {"source": f"s3://{bucket_name}/{key}", "destination": local_path, "size": size}

    def list_objects(self, bucket_name: str, prefix: str = "") -> List[Dict]:
        """List objects in a bucket."""
        paginator = self.s3.get_paginator("list_objects_v2")
        objects = []
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            for obj in page.get("Contents", []):
                objects.append({
                    "key": obj["Key"],
                    "size": obj["Size"],
                    "last_modified": obj["LastModified"].isoformat(),
                    "etag": obj["ETag"].strip('"'),
                })
        return objects

    def delete_object(self, bucket_name: str, key: str) -> Dict:
        """Delete an S3 object."""
        self.s3.delete_object(Bucket=bucket_name, Key=key)
        logger.info(f"Deleted s3://{bucket_name}/{key}")
        return {"deleted": True, "key": key}

    def generate_presigned_url(self, bucket_name: str, key: str,
                                expiration: int = 3600) -> str:
        """Generate a presigned URL for temporary object access."""
        url = self.s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket_name, "Key": key},
            ExpiresIn=expiration,
        )
        logger.info(f"Generated presigned URL for s3://{bucket_name}/{key} (expires in {expiration}s)")
        return url

    def set_lifecycle_policy(self, bucket_name: str, rules: List[Dict]) -> Dict:
        """Set lifecycle management rules on a bucket."""
        self.s3.put_bucket_lifecycle_configuration(
            Bucket=bucket_name,
            LifecycleConfiguration={"Rules": rules},
        )
        logger.info(f"Set lifecycle policy on {bucket_name} ({len(rules)} rule(s))")
        return {"bucket": bucket_name, "rules_applied": len(rules)}

    def get_bucket_size(self, bucket_name: str) -> Dict:
        """Calculate total size and object count for a bucket."""
        objects = self.list_objects(bucket_name)
        total_size = sum(o["size"] for o in objects)
        return {
            "bucket": bucket_name,
            "object_count": len(objects),
            "total_size_bytes": total_size,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
        }


# ═══════════════════════════════════════════════════════════════════════════════
# REAL GCS CLIENT
# ═══════════════════════════════════════════════════════════════════════════════

class GCSClient:
    """
    Real Google Cloud Storage client using google-cloud-storage.
    Requires: pip install google-cloud-storage
    Requires: GOOGLE_APPLICATION_CREDENTIALS env var pointing to service account JSON
    """

    def __init__(self, project_id: str = None):
        self.project_id = project_id or os.getenv("GCP_PROJECT_ID", "resume-analyzer-project")
        self.client = gcs_storage.Client(project=self.project_id)
        logger.info(f"GCS client initialised — project: {self.project_id}")

    def create_bucket(self, bucket_name: str, location: str = "US",
                      storage_class: str = "STANDARD") -> Dict:
        bucket = self.client.bucket(bucket_name)
        bucket.storage_class = storage_class
        new_bucket = self.client.create_bucket(bucket, location=location)
        logger.info(f"Created GCS bucket: gs://{bucket_name} ({location})")
        return {"bucket": bucket_name, "location": new_bucket.location,
                "storage_class": storage_class}

    def upload_file(self, bucket_name: str, blob_name: str, data: bytes,
                    content_type: str = "application/octet-stream",
                    metadata: Dict = None) -> Dict:
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        if metadata:
            blob.metadata = metadata
        blob.upload_from_string(data, content_type=content_type)
        logger.info(f"Uploaded gs://{bucket_name}/{blob_name} ({len(data):,} bytes)")
        return {"bucket": bucket_name, "blob": blob_name, "size": len(data),
                "md5": blob.md5_hash}

    def download_file(self, bucket_name: str, blob_name: str) -> bytes:
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        data = blob.download_as_bytes()
        logger.info(f"Downloaded gs://{bucket_name}/{blob_name} ({len(data):,} bytes)")
        return data

    def list_blobs(self, bucket_name: str, prefix: str = "") -> List[Dict]:
        return [
            {"name": b.name, "size": b.size, "updated": b.updated.isoformat(),
             "storage_class": b.storage_class, "md5": b.md5_hash}
            for b in self.client.list_blobs(bucket_name, prefix=prefix)
        ]

    def generate_signed_url(self, bucket_name: str, blob_name: str,
                             expiration_minutes: int = 60) -> str:
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        url = blob.generate_signed_url(expiration=timedelta(minutes=expiration_minutes),
                                        method="GET", version="v4")
        return url

    def delete_blob(self, bucket_name: str, blob_name: str) -> Dict:
        self.client.bucket(bucket_name).blob(blob_name).delete()
        logger.info(f"Deleted gs://{bucket_name}/{blob_name}")
        return {"deleted": True, "blob": blob_name}


# ═══════════════════════════════════════════════════════════════════════════════
# IN-MEMORY SIMULATION FALLBACK
# ═══════════════════════════════════════════════════════════════════════════════

class SimulatedCloudStorage:
    """
    In-memory simulation of S3/GCS when boto3/credentials are unavailable.
    Implements the same interface — just stores data in a dict.
    """

    def __init__(self, provider: str = "simulated-s3"):
        self.provider = provider
        self._buckets: Dict[str, Dict] = {}
        logger.info(f"Using in-memory cloud storage simulation (provider: {provider})")

    def create_bucket(self, bucket_name: str, **kwargs) -> Dict:
        if bucket_name not in self._buckets:
            self._buckets[bucket_name] = {"objects": {}, "versioning": False,
                                          "created": datetime.now().isoformat()}
        return {"bucket": bucket_name, "status": "created", "provider": self.provider}

    def enable_versioning(self, bucket_name: str) -> Dict:
        self._buckets[bucket_name]["versioning"] = True
        return {"bucket": bucket_name, "versioning": "Enabled"}

    def upload_file(self, bucket_name: str, key: str, data: bytes, **kwargs) -> Dict:
        etag = hashlib.md5(data).hexdigest()
        self._buckets[bucket_name]["objects"][key] = {
            "data": data, "size": len(data), "etag": etag,
            "uploaded": datetime.now().isoformat(),
            "content_type": kwargs.get("content_type", "application/octet-stream"),
            "metadata": kwargs.get("metadata", {}),
        }
        logger.info(f"[SIM] Uploaded {key} ({len(data):,} bytes) to {bucket_name}")
        return {"bucket": bucket_name, "key": key, "size": len(data), "etag": etag}

    def download_file(self, bucket_name: str, key: str) -> bytes:
        return self._buckets[bucket_name]["objects"][key]["data"]

    def list_objects(self, bucket_name: str, prefix: str = "") -> List[Dict]:
        return [
            {"key": k, "size": v["size"], "etag": v["etag"], "last_modified": v["uploaded"]}
            for k, v in self._buckets[bucket_name]["objects"].items()
            if k.startswith(prefix)
        ]

    def delete_object(self, bucket_name: str, key: str) -> Dict:
        self._buckets[bucket_name]["objects"].pop(key, None)
        return {"deleted": True, "key": key}

    def generate_presigned_url(self, bucket_name: str, key: str, expiration: int = 3600) -> str:
        token = hashlib.md5(f"{bucket_name}/{key}/{expiration}".encode()).hexdigest()
        return f"https://{bucket_name}.s3.amazonaws.com/{key}?X-Amz-Expires={expiration}&X-Amz-Signature={token}"

    def set_lifecycle_policy(self, bucket_name: str, rules: List[Dict]) -> Dict:
        self._buckets[bucket_name]["lifecycle"] = rules
        return {"bucket": bucket_name, "rules_applied": len(rules)}

    def get_bucket_size(self, bucket_name: str) -> Dict:
        objects = list(self._buckets[bucket_name]["objects"].values())
        total = sum(o["size"] for o in objects)
        return {"bucket": bucket_name, "object_count": len(objects),
                "total_size_bytes": total, "total_size_mb": round(total / (1024 * 1024), 4)}


# ═══════════════════════════════════════════════════════════════════════════════
# FACTORY — auto-selects real or simulated client
# ═══════════════════════════════════════════════════════════════════════════════

def get_s3_client() -> tuple:
    """Return (client, mode_string) for S3/MinIO."""
    if BOTO3_AVAILABLE:
        # Try MinIO first (no AWS account needed, just Docker)
        try:
            client = S3Client(use_minio=True)
            client.s3.list_buckets()
            logger.info("✅ Connected to real MinIO at %s", MINIO_ENDPOINT)
            return client, "MinIO (real)"
        except Exception as e:
            logger.warning("MinIO not reachable (%s). Trying AWS S3...", e)
        # Try real AWS S3
        try:
            client = S3Client(use_minio=False)
            client.s3.list_buckets()
            logger.info("✅ Connected to real AWS S3")
            return client, "AWS S3 (real)"
        except (NoCredentialsError, Exception) as e:
            logger.warning("AWS S3 not available (%s). Using simulation.", e)

    return SimulatedCloudStorage("simulated-s3"), "Simulation"


def get_gcs_client() -> tuple:
    """Return (client, mode_string) for GCS."""
    if GCS_AVAILABLE and os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        try:
            client = GCSClient()
            client.client.list_buckets(max_results=1)
            logger.info("✅ Connected to real Google Cloud Storage")
            return client, "GCS (real)"
        except Exception as e:
            logger.warning("GCS not available (%s). Using simulation.", e)
    return SimulatedCloudStorage("simulated-gcs"), "Simulation"


# ═══════════════════════════════════════════════════════════════════════════════
# STORAGE CLASS + BEST PRACTICES REFERENCE
# ═══════════════════════════════════════════════════════════════════════════════

STORAGE_CLASSES = {
    "aws_s3": {
        "STANDARD": "Frequently accessed data, < 1ms latency",
        "INTELLIGENT_TIERING": "Unknown/changing access patterns (auto-tiers)",
        "STANDARD_IA": "Infrequent access, same latency, lower cost",
        "ONE_ZONE_IA": "Infrequent, single AZ, 20% cheaper than STANDARD_IA",
        "GLACIER_INSTANT": "Archive, ms retrieval",
        "GLACIER_FLEXIBLE": "Archive, minutes-hours retrieval",
        "GLACIER_DEEP_ARCHIVE": "Long-term archive, 12-48h retrieval, lowest cost",
    },
    "gcs": {
        "STANDARD": "Frequently accessed data",
        "NEARLINE": "Monthly access (~$0.01/GB/month)",
        "COLDLINE": "Quarterly access (~$0.004/GB/month)",
        "ARCHIVE": "Yearly access (~$0.0012/GB/month)",
    },
    "azure_blob": {
        "Hot": "Frequent access, higher storage cost",
        "Cool": "Infrequent (30+ days), lower storage cost",
        "Cold": "Rarely accessed (90+ days)",
        "Archive": "Offline, hours to rehydrate",
    },
}

LIFECYCLE_RULES_EXAMPLE = [
    {
        "ID": "move-to-ia-after-30-days",
        "Status": "Enabled",
        "Filter": {"Prefix": "resumes/"},
        "Transitions": [
            {"Days": 30, "StorageClass": "STANDARD_IA"},
            {"Days": 90, "StorageClass": "GLACIER_INSTANT"},
            {"Days": 365, "StorageClass": "GLACIER_DEEP_ARCHIVE"},
        ],
    },
    {
        "ID": "delete-temp-files",
        "Status": "Enabled",
        "Filter": {"Prefix": "tmp/"},
        "Expiration": {"Days": 7},
    },
]


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN DEMO
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    print("\n" + "=" * 60)
    print("TASK 25: Cloud Storage — S3 / MinIO / GCS")
    print("=" * 60)

    # ── S3 / MinIO ────────────────────────────────────────────────────────────
    s3, s3_mode = get_s3_client()
    print(f"\n▶  S3 Mode: {s3_mode}")

    BUCKET = "resume-analyzer-data"
    s3.create_bucket(BUCKET)
    s3.enable_versioning(BUCKET)

    # Upload sample files
    uploads = []
    files = {
        "data/candidates.csv": (b"id,name,score\n1,Alice,92\n2,Bob,85\n3,Carol,78", "text/csv"),
        "data/stats.json": (json.dumps({"total": 3, "avg_score": 85}).encode(), "application/json"),
        "resumes/candidate_001.pdf": (b"%PDF-1.4 " + b"sample" * 200, "application/pdf"),
        "resumes/candidate_002.pdf": (b"%PDF-1.4 " + b"example" * 150, "application/pdf"),
        "processed/results.json": (json.dumps({"processed": True}).encode(), "application/json"),
    }

    print("\n⬆  Uploading files...")
    for key, (data, ctype) in files.items():
        result = s3.upload_file(BUCKET, key, data,
                                metadata={"pipeline": "resume-analyzer"},
                                content_type=ctype)
        uploads.append(result)
        print(f"   ✅ {key}: {result['size']:,} bytes  ETag={result['etag'][:12]}...")

    # List objects
    print("\n📋 Listing s3://resume-analyzer-data/resumes/:")
    for obj in s3.list_objects(BUCKET, prefix="resumes/"):
        print(f"   {obj['key']:45s}  {obj['size']:>8,} bytes")

    # Download
    data = s3.download_file(BUCKET, "data/candidates.csv")
    print(f"\n📥 Downloaded data/candidates.csv ({len(data)} bytes):")
    print("   " + data.decode()[:80].replace("\n", "\n   "))

    # Presigned URL
    url = s3.generate_presigned_url(BUCKET, "resumes/candidate_001.pdf", 3600)
    print(f"\n🔗 Presigned URL (1h): {url[:80]}...")

    # Lifecycle policy
    s3.set_lifecycle_policy(BUCKET, LIFECYCLE_RULES_EXAMPLE)
    print(f"\n⏱  Lifecycle policy applied ({len(LIFECYCLE_RULES_EXAMPLE)} rules)")

    # Bucket stats
    stats = s3.get_bucket_size(BUCKET)
    print(f"\n📊 Bucket stats: {stats['object_count']} objects, "
          f"{stats['total_size_mb']} MB total")

    # ── GCS ───────────────────────────────────────────────────────────────────
    gcs, gcs_mode = get_gcs_client()
    print(f"\n▶  GCS Mode: {gcs_mode}")

    GCS_BUCKET = "resume-analyzer-gcs"
    gcs.create_bucket(GCS_BUCKET, location="US", storage_class="STANDARD")

    gcs_uploads = []
    for sc, data_bytes in [("STANDARD", b"Frequently accessed data " * 10),
                            ("NEARLINE", b"Monthly access data " * 10),
                            ("COLDLINE", b"Quarterly backup data " * 10)]:
        result = gcs.upload_file(GCS_BUCKET, f"archive/{sc.lower()}_data.csv",
                                  data_bytes, content_type="text/csv",
                                  metadata={"storage_class": sc})
        gcs_uploads.append(result)
        print(f"   ✅ GCS {sc}: {result['size']} bytes")

    # ── Storage class reference ───────────────────────────────────────────────
    print("\n📚 Storage Class Reference:")
    for provider, classes in STORAGE_CLASSES.items():
        print(f"\n  {provider.upper()}:")
        for cls, desc in list(classes.items())[:3]:
            print(f"    {cls:25s} → {desc}")

    print("\n✅ Task 25 complete")
    return {
        "s3_mode": s3_mode,
        "gcs_mode": gcs_mode,
        "s3_uploads": uploads,
        "gcs_uploads": gcs_uploads,
        "storage_classes": STORAGE_CLASSES,
        "lifecycle_rules": LIFECYCLE_RULES_EXAMPLE,
    }


if __name__ == "__main__":
    main()
