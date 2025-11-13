import requests
from config import GENESYS_REGION

def get_conversations_with_transcripts(token, start_date, end_date):
    """Obtiene las conversaciones que tienen transcripciones disponibles."""
    url = f"https://api.{GENESYS_REGION}/api/v2/analytics/conversations/details/query"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {
        "interval": f"{start_date}/{end_date}",
        "order": "asc",
        "paging": {"pageSize": 100, "pageNumber": 1}
    }

    conversations = []
    page = 1
    while True:
        print(f"🔹 Solicitando página {page} de resultados...")
        r = requests.post(url, headers=headers, json=payload)
        print(f"   ↳ Respuesta {r.status_code}")
        if r.status_code != 200:
            print(f"⚠️ Error al obtener conversaciones (HTTP {r.status_code})")
            break

        data = r.json()
        entities = data.get("conversations", [])
        for c in entities:
            if c.get("hasTranscription", False):
                conversations.append(c["conversationId"])

        if not data.get("conversations") or len(entities) < 100:
            break

        page += 1
        payload["paging"]["pageNumber"] = page

    return conversations
