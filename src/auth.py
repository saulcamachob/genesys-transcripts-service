import requests
from config import GENESYS_CLIENT_ID, GENESYS_CLIENT_SECRET, GENESYS_REGION

def get_access_token():
    """Autentica en Genesys Cloud usando Client Credentials."""
    url = f"https://login.{GENESYS_REGION}/oauth/token"
    data = {"grant_type": "client_credentials"}
    resp = requests.post(url, data=data, auth=(GENESYS_CLIENT_ID, GENESYS_CLIENT_SECRET))
    resp.raise_for_status()
    return resp.json()["access_token"]
