import os
from datetime import timedelta

import pendulum
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.task_group import TaskGroup

GENESYS_CLIENT_ID = "11a6f722-b9ef-48f2-9531-d1f3b4d57db3"
GENESYS_CLIENT_SECRET = "u8AWwoM78AvdMypBvLLmaiTVeR4R2ZPo7Z6hTcd7uFo"
GENESYS_REGION = "mypurecloud.com"

OUTPUT_DIR = "./"
S3_BUCKET = "report360-datalake-prodution"
S3_PREFIX = ""

DATE_START = ""
DATE_END = ""

MEDIA_TYPE = "any"
PAGE_SIZE = 25
MAX_TOTAL_RESULTS = 1000

SEARCH_URL = f"https://api.{GENESYS_REGION}/api/v2/speechandtextanalytics/transcripts/search"


def _format_iso(dt: pendulum.DateTime) -> str:
    return dt.format("YYYY-MM-DD[T]HH:mm:ss.SSS[Z]")


def _format_query_dt(dt: pendulum.DateTime) -> str:
    return dt.format("YYYY-MM-DD[T]HH:mm:ss")


def _parse_datetime(value: str) -> pendulum.DateTime:
    return pendulum.parse(value, tz="UTC")


def resolve_date_range() -> dict:
    if not DATE_START and not DATE_END:
        today_start = pendulum.now("UTC").start_of("day")
        yesterday_start = today_start.subtract(days=1)
        date_start = _format_iso(yesterday_start)
        date_end = _format_iso(today_start)
        return {"date_start": date_start, "date_end": date_end}

    if not DATE_START or not DATE_END:
        raise ValueError("DATE_START y DATE_END deben suministrarse juntos en .env")

    return {"date_start": DATE_START, "date_end": DATE_END}


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
        "pageSize": 1,
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
    response = requests.post(SEARCH_URL, headers=headers, json=payload)
    if response.status_code == 429:
        retry = int(response.headers.get("Retry-After", "1"))
        print(f"⏳ Rate limit total (429). Esperando {retry}s…")
        import time

        time.sleep(retry)
        response = requests.post(SEARCH_URL, headers=headers, json=payload)
    response.raise_for_status()
    data = response.json() or {}
    return int(data.get("total", 0))


def build_balanced_date_ranges(**context) -> list[dict]:
    date_range = context["ti"].xcom_pull(task_ids="init.resolve_date_range")
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
    date_ranges = context["ti"].xcom_pull(task_ids="02_balance_ranges.balance_ranges")
    token = context["ti"].xcom_pull(task_ids="01_auth.fetch_token")

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    results = []
    for date_range in date_ranges or []:
        page_number = 1
        total = 0
        while True:
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

    headers = {"Authorization": f"Bearer {token}"}
    resolved = []
    for item in transcripts or []:
        conv_id = item["conversation_id"]
        comm_id = item["communication_id"]
        url = (
            f"https://api.{GENESYS_REGION}/api/v2/speechandtextanalytics/"
            f"conversations/{conv_id}/communications/{comm_id}/transcripturl"
        )
        response = requests.get(url, headers=headers)
        if response.status_code == 429:
            retry = int(response.headers.get("Retry-After", "1"))
            print(f"⏳ Rate limit URL (429). Esperando {retry}s…")
            import time

            time.sleep(retry)
            response = requests.get(url, headers=headers)
        if response.status_code == 404:
            print(f"⚠️ No hay transcriptURL para {conv_id}/{comm_id}")
            continue
        response.raise_for_status()
        payload = response.json() or {}
        transcript_url = payload.get("url")
        if transcript_url:
            resolved.append({**item, "url": transcript_url})

    return resolved


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
    for file_path in saved_files:
        filename = os.path.basename(file_path)
        name, _extension = os.path.splitext(filename)
        parts = name.split("__")
        if len(parts) >= 2:
            conv_id = parts[0]
            comm_id = parts[1]
            base_key = f"{conv_id}/{comm_id}/{filename}"
        else:
            base_key = filename

        key = f"{prefix}/{base_key}" if prefix else base_key
        hook.load_file(
            filename=file_path,
            key=key,
            bucket_name=S3_BUCKET,
            replace=True,
        )
        os.remove(file_path)
        uploaded += 1

    return {"uploaded": uploaded, "bucket": S3_BUCKET, "prefix": prefix}


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
