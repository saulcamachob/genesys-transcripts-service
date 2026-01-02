from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
import os
import threading

import pendulum
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.task_group import TaskGroup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

GENESYS_CLIENT_ID = "d2c69c59-c404-41b3-8da2-578934976898"
GENESYS_CLIENT_SECRET = "82ZD784Ipuh2d_O7KJrlQzxfSLEUZ_WEgzornvnPBHI"
GENESYS_REGION = "mypurecloud.com"

OUTPUT_DIR = "/opt/airflow/dags/llcr/data/"
S3_BUCKET = "report360-datalake-prodution"
S3_PREFIX = ""

DATE_START = ""
DATE_END = ""
TEST_MODE = True

MEDIA_TYPE = "any"
PAGE_SIZE = 100
MAX_TOTAL_RESULTS = 1000

SEARCH_URL = f"https://api.{GENESYS_REGION}/api/v2/speechandtextanalytics/transcripts/search"


def _format_iso(dt: pendulum.DateTime) -> str:
    return dt.format("YYYY-MM-DD[T]HH:mm:ss.SSS[Z]")


def _format_query_dt(dt: pendulum.DateTime) -> str:
    return dt.format("YYYY-MM-DD[T]HH:mm:ss")


def _parse_datetime(value: str) -> pendulum.DateTime:
    return pendulum.parse(value, tz="UTC")


def _normalize_input_date(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, pendulum.DateTime):
        return _format_iso(value)
    if isinstance(value, str) and not value.strip():
        return None
    return str(value)


def _default_date_range() -> dict:
    today_start = pendulum.now("UTC").start_of("day")
    yesterday_start = today_start.subtract(days=1)
    return {
        "date_start": _format_iso(yesterday_start),
        "date_end": _format_iso(today_start),
    }


def _normalize_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "si", "sí"}
    return bool(value)


def resolve_date_range(**context) -> dict:
    dag_run = context.get("dag_run")
    conf = (dag_run.conf or {}) if dag_run else {}

    input_start = _normalize_input_date(
        conf.get("DATE_START") or conf.get("date_start") or DATE_START
    )
    input_end = _normalize_input_date(
        conf.get("DATE_END") or conf.get("date_end") or DATE_END
    )
    if "TEST_MODE" in conf or "test_mode" in conf:
        input_test_mode = _normalize_bool(
            conf.get("TEST_MODE") if "TEST_MODE" in conf else conf.get("test_mode")
        )
    else:
        input_test_mode = None

    defaults = _default_date_range()
    date_start = input_start or defaults["date_start"]
    date_end = input_end or defaults["date_end"]

    return {
        "date_start": date_start,
        "date_end": date_end,
        "test_mode": TEST_MODE if input_test_mode is None else input_test_mode,
    }


def request_access_token() -> str:
    url = f"https://login.{GENESYS_REGION}/oauth/token"
    data = {"grant_type": "client_credentials"}
    response = requests.post(
        url, data=data, auth=(GENESYS_CLIENT_ID, GENESYS_CLIENT_SECRET)
    )
    response.raise_for_status()
    return response.json()["access_token"]


def _fetch_total_for_range(
    token: str, start_dt: pendulum.DateTime, end_dt: pendulum.DateTime
) -> int:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    payload = {
        "pageSize": PAGE_SIZE,
        "pageNumber": 1,
        "types": ["transcripts"],
        "returnFields": ["conversationId", "communicationId", "mediaType"],
        "query": [
            {
                "type": "DATE_RANGE",
                "fields": ["conversationStartTime"],
                "startValue": _format_query_dt(start_dt),
                "endValue": _format_query_dt(end_dt),
                "dateFormat": "yyyy-MM-dd'T'HH:mm:ss",
            }
        ],
    }
    print(f"Payload for balance in Genesys Cloud")
    print(payload)
    response = requests.post(SEARCH_URL, headers=headers, json=payload)
    while response.status_code == 429:
        retry = int(response.headers.get("Retry-After", "1"))
        print(f"⏳ Rate limit total (429). Esperando {retry}s…")
        import time

        time.sleep(retry)
        response = requests.post(SEARCH_URL, headers=headers, json=payload)
    response.raise_for_status()
    data = response.json() or {}
    print(f"Total regs {int(data.get("total", 0))}", )
    
    return int(data.get("total", 0))


def build_balanced_date_ranges(**context) -> list[dict]:
    date_range = context["ti"].xcom_pull(task_ids="init.resolve_date_range")
    if _normalize_bool((date_range or {}).get("test_mode", TEST_MODE)):
        start_dt = _parse_datetime(date_range["date_start"])
        end_dt = _parse_datetime(date_range["date_end"])
        return [
            {
                "date_start": _format_query_dt(start_dt),
                "date_end": _format_query_dt(end_dt),
                "total": None,
            }
        ]
    token = context["ti"].xcom_pull(task_ids="01_auth.fetch_token")

    start_dt = _parse_datetime(date_range["date_start"])
    end_dt = _parse_datetime(date_range["date_end"])

    ranges_to_process = [(start_dt, end_dt)]
    balanced_ranges = []
    min_span = pendulum.duration(minutes=1)

    while ranges_to_process:
        current_start, current_end = ranges_to_process.pop(0)
        total = _fetch_total_for_range(token, current_start, current_end)
        span = current_end - current_start

        if total > MAX_TOTAL_RESULTS and span > min_span:
            midpoint = current_start.add(seconds=span.total_seconds() / 2)
            ranges_to_process.append((current_start, midpoint))
            ranges_to_process.append((midpoint, current_end))
            continue

        if total > MAX_TOTAL_RESULTS and span <= min_span:
            print(
                "⚠️ Rango mínimo aún supera el máximo permitido. "
                f"Inicio={_format_query_dt(current_start)} "
                f"Fin={_format_query_dt(current_end)} Total={total}"
            )

        balanced_ranges.append(
            {
                "date_start": _format_query_dt(current_start),
                "date_end": _format_query_dt(current_end),
                "total": total,
            }
        )

    return balanced_ranges


def search_transcripts_available(**context) -> list[dict]:
    runtime_config = context["ti"].xcom_pull(task_ids="init.resolve_date_range") or {}
    date_ranges = context["ti"].xcom_pull(task_ids="02_balance_ranges.balance_ranges")
    token = context["ti"].xcom_pull(task_ids="01_auth.fetch_token")
    test_mode = _normalize_bool(runtime_config.get("test_mode", TEST_MODE))

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    results = []
    for date_range in date_ranges or []:
        page_number = 1
        total = 0
        while True:
            if test_mode and page_number > 4:
                print("🧪 TEST_MODE activo: se limita a las primeras 4 páginas.")
                break
            payload = {
                "pageSize": PAGE_SIZE,
                "pageNumber": page_number,
                "types": ["transcripts"],
                "returnFields": ["conversationId", "communicationId", "mediaType"],
                "query": [
                    {
                        "type": "DATE_RANGE",
                        "fields": ["conversationStartTime"],
                        "startValue": date_range["date_start"],
                        "endValue": date_range["date_end"],
                        "dateFormat": "yyyy-MM-dd'T'HH:mm:ss",
                    }
                ],
            }

            if MEDIA_TYPE.lower() != "any":
                payload["query"].append(
                    {
                        "type": "EXACT",
                        "fields": ["mediaType"],
                        "value": MEDIA_TYPE,
                    }
                )

            print("Payload for search in Genesys Cloud")
            print(payload)

            response = requests.post(SEARCH_URL, headers=headers, json=payload)
            if response.status_code == 429:
                retry = int(response.headers.get("Retry-After", "1"))
                print(f"⏳ Rate limit (429). Esperando {retry}s…")
                import time

                time.sleep(retry)
                continue

            response.raise_for_status()
            data = response.json() or {}
            total = int(data.get("total", total))
            page_results = data.get("results", []) or data.get("conversations", []) or []
            if not page_results:
                break

            for item in page_results:
                conv_id = item.get("conversationId")
                comm_id = item.get("communicationId")
                media_type = (item.get("mediaType") or MEDIA_TYPE or "unknown").upper()
                if conv_id and comm_id:
                    results.append(
                        {
                            "conversation_id": conv_id,
                            "communication_id": comm_id,
                            "media_type": media_type,
                        }
                    )

            if total and page_number * PAGE_SIZE >= total:
                break
            if len(page_results) < PAGE_SIZE:
                break
            page_number += 1

    return results


def resolve_transcript_urls(**context) -> list[dict]:
    token = context["ti"].xcom_pull(task_ids="01_auth.fetch_token")
    transcripts = context["ti"].xcom_pull(
        task_ids="02_get_data_available.search_transcripts"
    )
    resolved = []
    if not transcripts:
        return resolved

    max_workers = int(os.getenv("RESOLVE_URLS_MAX_WORKERS", "12"))
    log_every = int(os.getenv("RESOLVE_URLS_LOG_EVERY", "500"))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for index, result in enumerate(
            executor.map(lambda item: _resolve_transcript_url(item, token), transcripts),
            start=1,
        ):
            if result:
                resolved.append(result)
            if log_every and index % log_every == 0:
                print(f"🔎 URLs resueltas: {index}/{len(transcripts)}")

    return resolved


def _build_retry_session() -> requests.Session:
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=2,
        status_forcelist={429, 500, 502, 503, 504},
        allowed_methods={"GET"},
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


_thread_local = threading.local()


def _thread_session() -> requests.Session:
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = _build_retry_session()
        _thread_local.session = session
    return session


def _resolve_transcript_url(item: dict, token: str) -> dict | None:
    conv_id = item["conversation_id"]
    comm_id = item["communication_id"]
    url = (
        f"https://api.{GENESYS_REGION}/api/v2/speechandtextanalytics/"
        f"conversations/{conv_id}/communications/{comm_id}/transcripturl"
    )
    headers = {"Authorization": f"Bearer {token}"}
    response = _thread_session().get(url, headers=headers, timeout=(10, 30))
    if response.status_code == 404:
        print(f"⚠️ No hay transcriptURL para {conv_id}/{comm_id}")
        return None
    response.raise_for_status()
    payload = response.json() or {}
    transcript_url = payload.get("url")
    if not transcript_url:
        return None
    return {**item, "url": transcript_url}


def download_transcripts(**context) -> dict:
    transcripts = context["ti"].xcom_pull(
        task_ids="03_get_url_transcripts.resolve_urls"
    )
    count_found = 0
    count_saved = 0
    saved_files = []
    for item in transcripts or []:
        count_found += 1
        conv_id = item["conversation_id"]
        comm_id = item["communication_id"]
        media_type = item["media_type"]
        presigned_url = item["url"]

        filename = f"{conv_id}__{comm_id}__{media_type}.json"
        dest_path = os.path.join(OUTPUT_DIR, filename)
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)

        response = requests.get(presigned_url, timeout=120)
        response.raise_for_status()
        with open(dest_path, "wb") as file_handle:
            file_handle.write(response.content)
        count_saved += 1
        saved_files.append(dest_path)

    return {
        "found": count_found,
        "saved": count_saved,
        "output_dir": OUTPUT_DIR,
        "saved_files": saved_files,
    }


def upload_transcripts_to_s3(**context) -> dict:
    payload = context["ti"].xcom_pull(
        task_ids="03_get_url_transcripts.download_transcripts"
    )
    saved_files = (payload or {}).get("saved_files", [])
    hook = S3Hook(aws_conn_id="aws_default")

    prefix = S3_PREFIX.strip("/")
    uploaded = 0
    skipped_missing = 0
    for file_path in saved_files:
        if not os.path.exists(file_path):
            print(
                "⚠️ Archivo no encontrado para carga en S3. Se omite y continúa: "
                f"{file_path}"
            )
            skipped_missing += 1
            continue
        filename = os.path.basename(file_path)
        key = f"{prefix}/{filename}" if prefix else filename
        hook.load_file(
            filename=file_path,
            key=key,
            bucket_name=S3_BUCKET,
            replace=True,
        )
        os.remove(file_path)
        uploaded += 1

    return {
        "uploaded": uploaded,
        "skipped_missing": skipped_missing,
        "bucket": S3_BUCKET,
        "prefix": prefix,
    }


default_args = {
    "owner": "genesys",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="genesys_download_transcripts_process",
    description="DAG unificado para descargar transcripciones desde Genesys",
    default_args=default_args,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["genesys", "transcripts"],
):
    with TaskGroup(group_id="init", tooltip="Carga inicial y validación") as init_group:
        resolve_date_range_task = PythonOperator(
            task_id="resolve_date_range",
            python_callable=resolve_date_range,
        )

    with TaskGroup(group_id="01_auth", tooltip="Autenticación") as auth_group:
        fetch_token_task = PythonOperator(
            task_id="fetch_token",
            python_callable=request_access_token,
        )

    with TaskGroup(
        group_id="02_balance_ranges",
        tooltip="Balanceo de rangos por límite de resultados",
    ) as balance_group:
        balance_ranges_task = PythonOperator(
            task_id="balance_ranges",
            python_callable=build_balanced_date_ranges,
        )

    with TaskGroup(
        group_id="02_get_data_available",
        tooltip="Búsqueda de transcripciones disponibles",
    ) as search_group:
        search_transcripts_task = PythonOperator(
            task_id="search_transcripts",
            python_callable=search_transcripts_available,
        )

    with TaskGroup(
        group_id="03_get_url_transcripts",
        tooltip="Resolución de URLs y descarga de transcripciones",
    ) as download_group:
        resolve_urls_task = PythonOperator(
            task_id="resolve_urls",
            python_callable=resolve_transcript_urls,
        )
        download_transcripts_task = PythonOperator(
            task_id="download_transcripts",
            python_callable=download_transcripts,
        )
        upload_transcripts_task = PythonOperator(
            task_id="upload_transcripts",
            python_callable=upload_transcripts_to_s3,
        )

        resolve_urls_task >> download_transcripts_task >> upload_transcripts_task

    init_group >> auth_group >> balance_group >> search_group >> download_group
