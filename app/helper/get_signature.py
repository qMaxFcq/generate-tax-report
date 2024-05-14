import hmac
from hashlib import sha256


async def get_signature(params: dict, secret_key: str):
    query_string = "&".join([f"{key}={value}" for key, value in params.items()])
    signature = hmac.new(
        secret_key.encode("utf-8"), query_string.encode("utf-8"), sha256
    ).hexdigest()
    return query_string, signature
