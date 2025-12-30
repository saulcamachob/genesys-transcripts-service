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

SEARCH_URL = f"https://api.{GENESYS_REGION}/api/v2/speechandtextanalytics/transcripts/search"


def _format_iso(dt: pendulum.DateTime) -> str:
    return dt.format("YYYY-MM-DD[T]HH:mm:ss.SSS[Z]")


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


def search_transcripts_available(**context) -> list[dict]:
    date_range = context["ti"].xcom_pull(task_ids="init.resolve_date_range")
    token = context["ti"].xcom_pull(task_ids="01_auth.fetch_token")

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    page_number = 1
    results = []
    while True:
        payload = {
            "pageSize": PAGE_SIZE,
            "pageNumber": page_number,
            "types": ["transcripts"],
            "returnFields": ["conversationId", "communicationId", "mediaType"],
            "interval": f"{date_range['date_start']}/{date_range['date_end']}",
        }

        if MEDIA_TYPE.lower() != "any":
            payload["query"] = [
                {"type": "EXACT", "fields": ["mediaType"], "value": MEDIA_TYPE}
            ]

        print(f"Payload for search in Genesys Cloud")
        print(f"{payload}")

        response = requests.post(SEARCH_URL, headers=headers, json=payload)
        if response.status_code == 429:
            retry = int(response.headers.get("Retry-After", "1"))
            print(f"⏳ Rate limit (429). Esperando {retry}s…")
            import time

            time.sleep(retry)
            continue

        response.raise_for_status()
        data = response.json() or {}
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

    init_group >> auth_group >> search_group >> download_group
