from auth import get_access_token
from api.search_transcripts import search_transcripts
from api.transcripts import get_transcript_url, download_transcript_json
from config import DATE_START, DATE_END, OUTPUT_DIR, MEDIA_TYPE
from utils.logger import setup_logger
import os

def main():
    logger = setup_logger()
    print("\n🚀 Extracción de transcripciones (Search → URL → Download)\n")

    if not (DATE_START and DATE_END):
        raise RuntimeError("DATE_START y DATE_END son obligatorios en .env (ISO-8601 con milisegundos).")

    token = get_access_token()
    print(f"🔑 Token OK | Intervalo: {DATE_START} → {DATE_END} | mediaType={MEDIA_TYPE}\n")

    count_found = 0
    count_saved = 0
    for conv_id, comm_id, media_type in search_transcripts(token, DATE_START, DATE_END):
        count_found += 1
        print(f"🔎 {count_found:04d} → {conv_id} | {comm_id} | {media_type}")

        url = get_transcript_url(token, conv_id, comm_id)
        if not url:
            continue

        # Añadimos el sufijo de tipo VOICE/TEXT al nombre
        fname = f"{conv_id}__{comm_id}__{media_type}.json"
        path = os.path.join(OUTPUT_DIR, fname)
        try:
            download_transcript_json(url, path)
            count_saved += 1
            print(f"   ✅ Guardado: {path}")
        except Exception as e:
            print(f"   ❌ Error al descargar {conv_id}/{comm_id}: {e}")

    print(f"\n✅ Terminado. Encontrados: {count_found} | Guardados: {count_saved} | Carpeta: {OUTPUT_DIR}\n")

if __name__ == "__main__":
    main()
