"""Adds or replaces one identity in the SeaweedFS S3 configuration Secret.

Reads the Secret as JSON (kubectl get secret seaweedfs-s3-config -o json) on
stdin and writes it back, modified, on stdout (for kubectl replace). The keys
come from the environment, never from the command line:

    S3_IDENTITY  identity name, e.g. streaming
    S3_ACCESS    access key
    S3_SECRET    secret key

The identity can read, write and list the warehouse bucket only, like the
identities created by create.sh. An identity of the same name is replaced:
running the script again after a failure gives the same result.
Used by create.sh (group "streaming").
"""

import base64
import json
import os
import sys

CONFIG_KEY = "seaweedfs_s3_config"
BUCKET_RW = ["Read:warehouse", "Write:warehouse", "List:warehouse"]


def add_identity(secret: dict, name: str, access: str, secret_key: str) -> dict:
    config = json.loads(base64.b64decode(secret["data"][CONFIG_KEY]))
    identities = [i for i in config.get("identities", []) if i.get("name") != name]
    identities.append(
        {"name": name, "credentials": [{"accessKey": access, "secretKey": secret_key}], "actions": BUCKET_RW}
    )
    config["identities"] = identities
    secret["data"][CONFIG_KEY] = base64.b64encode(json.dumps(config, separators=(",", ":")).encode()).decode()
    return secret


def main() -> None:
    secret = json.load(sys.stdin)
    updated = add_identity(secret, os.environ["S3_IDENTITY"], os.environ["S3_ACCESS"], os.environ["S3_SECRET"])
    json.dump(updated, sys.stdout)


if __name__ == "__main__":
    main()
