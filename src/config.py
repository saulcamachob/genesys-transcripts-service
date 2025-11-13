import os
from dotenv import load_dotenv

load_dotenv()

GENESYS_CLIENT_ID = os.getenv("GENESYS_CLIENT_ID")
GENESYS_CLIENT_SECRET = os.getenv("GENESYS_CLIENT_SECRET")
GENESYS_REGION = os.getenv("GENESYS_REGION", "mypurecloud.com")

OUTPUT_DIR = os.getenv("OUTPUT_DIR", "./output")

# Fechas ISO-8601 completas con milisegundos (recomendado por la API)
DATE_START = os.getenv("DATE_START")  # ej: 2025-11-10T00:00:00.000Z
DATE_END   = os.getenv("DATE_END")    # ej: 2025-11-12T23:59:59.999Z

# Filtros de búsqueda
MEDIA_TYPE = os.getenv("MEDIA_TYPE", "call")  # call | chat | email | message
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "25"))  # límite real recomendado por la API
