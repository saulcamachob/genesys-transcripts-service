import requests
from config import GENESYS_REGION, PAGE_SIZE, MEDIA_TYPE

SEARCH_URL = f"https://api.{GENESYS_REGION}/api/v2/speechandtextanalytics/transcripts/search"

def search_transcripts(token: str, start_iso: str, end_iso: str):
    """
    Itera todas las páginas de /transcripts/search y rinde
    (conversationId, communicationId, mediaType).
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    page_number = 1
    while True:
        payload = {
            "pageSize": PAGE_SIZE,
            "pageNumber": page_number,
            "types": ["transcripts"],
            "returnFields": ["conversationId", "communicationId", "mediaType"],
            "interval": f"{start_iso}/{end_iso}",
        }

        # Si el usuario definió MEDIA_TYPE, aplicamos filtro
        if MEDIA_TYPE.lower() != "any":
            payload["query"] = [
                {"type": "EXACT", "fields": ["mediaType"], "value": MEDIA_TYPE}
            ]

        resp = requests.post(SEARCH_URL, headers=headers, json=payload)
        if resp.status_code == 429:
            retry = int(resp.headers.get("Retry-After", "1"))
            print(f"⏳ Rate limit (429). Esperando {retry}s…")
            import time; time.sleep(retry)
            continue

        resp.raise_for_status()
        data = resp.json() or {}
        results = data.get("results", []) or data.get("conversations", []) or []

        if not results:
            break

        for item in results:
            c_id = item.get("conversationId")
            comm_id = item.get("communicationId")
            media_type = (item.get("mediaType") or MEDIA_TYPE or "unknown").upper()
            if c_id and comm_id:
                yield c_id, comm_id, media_type

        if len(results) < PAGE_SIZE:
            break
        page_number += 1
