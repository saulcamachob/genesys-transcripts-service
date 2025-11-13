import os
import json
import requests
from config import GENESYS_REGION, OUTPUT_DIR

def get_transcript_url(token: str, conversation_id: str, communication_id: str) -> str | None:
    """
    Obtiene la URL pre-firmada (caduca ~10 minutos).
    """
    url = (
        f"https://api.{GENESYS_REGION}/api/v2/speechandtextanalytics/"
        f"conversations/{conversation_id}/communications/{communication_id}/transcripturl"
    )
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(url, headers=headers)
    if r.status_code == 429:
        retry = int(r.headers.get("Retry-After", "1"))
        print(f"⏳ Rate limit URL (429). Esperando {retry}s…")
        import time; time.sleep(retry)
        return get_transcript_url(token, conversation_id, communication_id)
    if r.status_code == 404:
        print(f"⚠️ No hay transcriptURL para {conversation_id}/{communication_id}")
        return None
    r.raise_for_status()
    data = r.json() or {}
    return data.get("url")

def download_transcript_json(presigned_url: str, dest_path: str):
    """
    Descarga el JSON de la URL pre-firmada. NO usar Bearer aquí.
    """
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    r = requests.get(presigned_url, timeout=120)
    r.raise_for_status()
    with open(dest_path, "wb") as f:
        f.write(r.content)
