from datetime import timedelta
import json
import os
import random
import time

import pendulum
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator, get_current_context
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.task_group import TaskGroup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

connection_genesys = BaseHook.get_connection("llcr_genesys_transcr")
GENESYS_CLIENT_ID = connection_genesys.login
GENESYS_CLIENT_SECRET = connection_genesys.password
GENESYS_REGION = connection_genesys.description
GENESYS_EXTRAS = connection_genesys.extra

S3_BUCKET = "report360-datalake-prodution"
S3_PREFIX = ""

DATE_START = ""
DATE_END = ""
TEST_MODE = True

MEDIA_TYPE = "any"
PAGE_SIZE = 100
MAX_TOTAL_RESULTS = 1000
MAX_RETRY_AFTER_SECONDS = 30
MAX_TRANSCRIPT_URL_ATTEMPTS = 4

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


def _conversation_start_date(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, pendulum.DateTime):
        dt_value = value
    elif isinstance(value, (int, float)):
        dt_value = pendulum.from_timestamp(value / 1000, tz="UTC")
    elif isinstance(value, str):
        trimmed = value.strip()
        if not trimmed:
            return None
        if trimmed.isdigit():
            dt_value = pendulum.from_timestamp(int(trimmed) / 1000, tz="UTC")
        else:
            dt_value = pendulum.parse(trimmed, tz="UTC")
    else:
        return None
    return dt_value.format("YYYY-MM-DD")


def resolve_date_range(**context) -> dict:
    if not context:
        try:
            context = get_current_context()
        except Exception:
            context = {}
    dag_run = context.get("dag_run")
    conf = (dag_run.conf or {}) if dag_run else {}
    if isinstance(conf, str):
        try:
            conf = json.loads(conf)
        except json.JSONDecodeError:
            conf = {}
    if not isinstance(conf, dict):
        conf = {}
    normalized_conf = {str(key).strip().lower(): value for key, value in conf.items()}

    input_start = _normalize_input_date(
        normalized_conf.get("date_start") or DATE_START
    )
    input_end = _normalize_input_date(
        normalized_conf.get("date_end") or DATE_END
    )
    if "test_mode" in normalized_conf:
        input_test_mode = _normalize_bool(
            normalized_conf.get("test_mode")
        )
    else:
        input_test_mode = None

    defaults = _default_date_range()
    date_start = input_start or defaults.get("date_start")
    date_end = input_end or defaults.get("date_end")
    if not date_start or not date_end:
        defaults = _default_date_range()
        date_start = date_start or defaults["date_start"]
        date_end = date_end or defaults["date_end"]

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
        "returnFields": ["conversationId", "communicationId", "mediaType","conversationStartTime"],
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
    if not context:
        try:
            context = get_current_context()
        except Exception:
            context = {}
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
    if not context:
        try:
            context = get_current_context()
        except Exception:
            context = {}
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
            if test_mode and page_number > 10:
                print("🧪 TEST_MODE activo: se limita a las primeras 4 páginas.")
                break
            payload = {
                "pageSize": PAGE_SIZE,
                "pageNumber": page_number,
                "types": ["transcripts"],
                "returnFields": ["conversationId", "communicationId", "mediaType","conversationStartTime"],
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
                conversation_start_time = item.get("conversationStartTime")
                if conv_id and comm_id:
                    results.append(
                        {
                            "conversation_id": conv_id,
                            "communication_id": comm_id,
                            "media_type": media_type,
                            "conversation_start_time": conversation_start_time,
                        }
                    )

            if total and page_number * PAGE_SIZE >= total:
                break
            if len(page_results) < PAGE_SIZE:
                break
            page_number += 1

    return results


def _build_retry_session() -> requests.Session:
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=2,
        status_forcelist={500, 502, 503, 504},
        allowed_methods={"GET"},
        respect_retry_after_header=False,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

def _resolve_transcript_url(
    item: dict, token: str, session: requests.Session | None = None
) -> dict | None:
    if session is None:
        session = _build_retry_session()
    conv_id = item["conversation_id"]
    comm_id = item["communication_id"]
    url = (
        f"https://api.{GENESYS_REGION}/api/v2/speechandtextanalytics/"
        f"conversations/{conv_id}/communications/{comm_id}/transcripturl"
    )
    headers = {"Authorization": f"Bearer {token}"}
    for attempt in range(1, MAX_TRANSCRIPT_URL_ATTEMPTS + 1):
        response = session.get(url, headers=headers, timeout=(10, 30))
        if response.status_code == 404:
            print(f"⚠️ No hay transcriptURL para {conv_id}/{comm_id}")
            return None
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", "1"))
            sleep_seconds = min(retry_after, MAX_RETRY_AFTER_SECONDS)
            jitter = random.uniform(0, 1)
            print(
                "⏳ Rate limit (429) al resolver transcriptURL para "
                f"{conv_id}/{comm_id}. Intento {attempt}/"
                f"{MAX_TRANSCRIPT_URL_ATTEMPTS}. Esperando {sleep_seconds:.1f}s."
            )
            time.sleep(sleep_seconds + jitter)
            continue
        response.raise_for_status()
        payload = response.json() or {}
        transcript_url = payload.get("url")
        if not transcript_url:
            return None
        return {**item, "url": transcript_url}
    print(
        "⚠️ Se agotaron los reintentos para resolver transcriptURL "
        f"{conv_id}/{comm_id}."
    )
    return None


@task
def chunk_transcripts(transcripts: list[dict], batch_size: int = 300) -> list[list[dict]]:
    if not transcripts:
        return []
    return [
        transcripts[index : index + batch_size]
        for index in range(0, len(transcripts), batch_size)
    ]


@task
def resolve_and_stream_batch_to_s3(transcripts_batch: list[dict], token: str) -> dict:
    hook = S3Hook(aws_conn_id="aws_default")
    s3_client = hook.get_conn()
    prefix = S3_PREFIX.strip("/")
    session = _build_retry_session()
    count_found = 0
    count_resolved = 0
    count_uploaded = 0
    count_failed = 0
    for item in transcripts_batch or []:
        count_found += 1
        try:
            resolved_item = _resolve_transcript_url(item, token, session)
        except requests.RequestException as exc:
            count_failed += 1
            conv_id = item.get("conversation_id")
            comm_id = item.get("communication_id")
            print(
                "⚠️ Error al resolver transcriptURL. Se continúa. "
                f"{conv_id}/{comm_id} error={exc}"
            )
            continue
        if not resolved_item:
            continue
        count_resolved += 1
        conv_id = resolved_item["conversation_id"]
        comm_id = resolved_item["communication_id"]
        media_type = resolved_item["media_type"]
        presigned_url = resolved_item["url"]
        conversation_start_time = resolved_item.get("conversation_start_time")

        filename_actual = f"{conv_id}__{comm_id}__{media_type}.json"
        folder_date = _conversation_start_date(conversation_start_time) or "unknown-date"
        base_prefix = "transcripts"
        full_prefix = "/".join(
            part for part in (prefix, folder_date, base_prefix) if part
        )
        filename = f"genesys_transcript_id_{filename_actual}"
        key = f"{full_prefix}/{filename}" if full_prefix else filename
        try:
            with session.get(presigned_url, stream=True, timeout=(10, 120)) as response:
                response.raise_for_status()
                response.raw.decode_content = True
                s3_client.upload_fileobj(
                    response.raw,
                    S3_BUCKET,
                    key,
                )
            count_uploaded += 1
        except requests.RequestException as exc:
            count_failed += 1
            print(
                "⚠️ No se pudo descargar/subir el transcript. Se continúa. "
                f"{conv_id}/{comm_id} error={exc}"
            )

    return {
        "found": count_found,
        "resolved": count_resolved,
        "uploaded": count_uploaded,
        "failed": count_failed,
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
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Puerto_Rico"),
    schedule="0 3 * * *",
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
        chunk_transcripts_task = chunk_transcripts(
            search_transcripts_task.output,
            batch_size=300,
        )
        stream_transcripts_task = resolve_and_stream_batch_to_s3.partial(
            token=fetch_token_task.output,
        ).expand(
            transcripts_batch=chunk_transcripts_task,
        )

        chunk_transcripts_task >> stream_transcripts_task

    init_group >> auth_group >> balance_group >> search_group >> download_group
