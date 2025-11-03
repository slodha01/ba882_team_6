# streamlit_app/utils/bq.py
import json
import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

@st.cache_resource(show_spinner=False)
def get_bq_client():
    info = st.secrets.get("gcp_service_account", None)
    # If secrets holds a JSON string (triple-quoted), parse it:
    if isinstance(info, str):
        try:
            info = json.loads(info)
        except Exception as e:
            raise RuntimeError(f"Service account JSON could not be parsed: {e}")
    # Expect a dict now
    if not isinstance(info, dict):
        raise RuntimeError("Missing gcp_service_account in Streamlit secrets.")
    # minimal sanity check
    for k in ("client_email", "token_uri", "private_key"):
        if k not in info:
            raise RuntimeError("Service account secret is missing required fields.")
    creds = service_account.Credentials.from_service_account_info(info)
    project_id = st.secrets.get("gcp_project_id")  # must exist
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
                    query_parameters=[
                        ScalarQueryParameter(k, "STRING", v) for k, v in params.items()
                    ]
                ),
            )
        else:
            job = client.query(sql)
        return job.result().to_dataframe(create_bqstorage_client=True)
    except Exception as e:
        st.info(f"BigQuery query failed: {e}")
        return pd.DataFrame()

