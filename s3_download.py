import csv
import os
import sys
import warnings

warnings.filterwarnings("ignore")

import boto3

BUCKET = os.environ.get("S3_BUCKET") or (sys.argv[1] if len(sys.argv) > 1 else None)
if not BUCKET:
    raise SystemExit("Usage: S3_BUCKET=<bucket-name> python3 s3_download.py  (or pass the bucket name as argv[1])")

PREFIX = "bronze/"
KEY = "bronze/netflix_titles.csv"
LOCAL_PATH = os.path.join(os.path.dirname(__file__), "data", "netflix_titles.csv")


def main():
    s3 = boto3.client("s3")

    print(f"[1] list   s3://{BUCKET}/{PREFIX}")
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)
    for obj in resp.get("Contents", []):
        print(f"    {obj['Key']:<40} {obj['Size']:,} bytes")

    os.makedirs(os.path.dirname(LOCAL_PATH), exist_ok=True)
    print(f"[2] download s3://{BUCKET}/{KEY} -> {os.path.relpath(LOCAL_PATH)}")
    s3.download_file(BUCKET, KEY, LOCAL_PATH)
    print(f"    downloaded ({os.path.getsize(LOCAL_PATH):,} bytes)")

    with open(LOCAL_PATH, newline="", encoding="utf-8") as f:
        row_count = sum(1 for _ in csv.reader(f)) - 1  # exclude header

    print(f"[3] rows   {row_count:,}")


if __name__ == "__main__":
    main()
