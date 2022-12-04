import logging
from typing import Optional
import json
from google.cloud import secretmanager


def get_secret() -> Optional[dict]:
    client = secretmanager.SecretManagerServiceClient()
    secret_detail = "projects/702739857141/secrets/quora_secret/versions/latest"
    response = client.access_secret_version(name=secret_detail)
    data = response.payload.data.decode("utf-8")
    data = json.loads(data)
    return data


def save_secret(token_data: Optional[dict] = None) -> dict:
    client = secretmanager.SecretManagerServiceClient()
    parent = client.secret_path("702739857141", "quora_secret")
    token_data = json.dumps(token_data, indent=4, separators=(",", ": ")).encode(
        "utf-8"
    )
    response = client.add_secret_version(
        request={
            "parent": parent,
            "payload": {"data": token_data},
        }
    )
    logging.info("Added new secret version : %s", response.name)

    return token_data
