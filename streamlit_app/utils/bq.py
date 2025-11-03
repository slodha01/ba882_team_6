import json
import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

@st.cache_resource(show_spinner=False)
def get_bq_client():
    info = st.secrets.get("gcp_service_account", None)
    # Accept both a JSON string (from triple-quoted TOML) or a dict
    if isinstance(info, str):
        info = json.loads(info)
    if not isinstance(info, dict):
        raise RuntimeError("Missing gcp_service_account in Streamlit secrets.")
    for k in ("client_email", "token_uri", "private_key", "project_id"):
        if k not in info:
            raise RuntimeError(f"Service account secret missing field: {k}")
    creds = service_account.Credentials.from_service_account_info(info)
    project_id = st.secrets.get("gcp_project_id") or info.get("project_id")
    if not project_id:
        raise RuntimeError("Missing gcp_project_id in secrets and service account.")
    return bigquery.Client(project=project_id, credentials=creds)

@st.cache_data(ttl=600)
def run_query(sql: str, params: dict | None = None) -> pd.DataFrame:
    try:
        client = get_bq_client()
        if params:
            from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter
            job = client.query(
                sql,
                job_config=QueryJobConfig(
                    query_parameters=[ScalarQueryParameter(k, "STRING", v) for k, v in params.items()]
                ),
            )
        else:
            job = client.query(sql)
        return job.result().to_dataframe(create_bqstorage_client=True)
    except Exception as e:
        st.info(f"BigQuery query failed: {e}")
        return pd.DataFrame()
