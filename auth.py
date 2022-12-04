import json
import logging
from typing import Any, Dict
import requests
from secret import get_secret, save_secret


def refresh_token() -> Dict[str, Any]:
    """
    Function to refresh token and return token_dictionary
    """
    secret_dict = get_secret()

    if secret_dict is None:
        raise ValueError("Invalid token")

    response = requests.post(
        "https://www.quora.com/_/oauth/token",
        {
            "client_id": secret_dict["client_id"],
            "client_secret": secret_dict["client_secret"],
            "grant_type": "refresh_token",
            "redirect_uri": secret_dict["redirect_url"],
            "refresh_token": secret_dict["refresh_token"],
        },
        timeout=10,
    )

    updated_token = json.loads(response.text)
    secret_dict["refresh_token"] = updated_token["refresh_token"]
    secret_dict["access_token"] = updated_token["access_token"]
    secret_dict["expires_in"] = updated_token["expires_in"]
    try:
        save_secret(token_data=secret_dict)
    except Exception as exc:
        logging.exception("token %s, exception: %s", secret_dict, exc)
    return secret_dict
