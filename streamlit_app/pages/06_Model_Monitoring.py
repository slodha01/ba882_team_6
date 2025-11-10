import streamlit as st
from utils.bq import run_query

st.title("Model Monitoring (last 30 days)")
df = run_query("SELECT * FROM `ml_artifacts.v_monitoring_30d`")
if df.empty:
    st.info("No monitoring rows yet.")
    st.stop()

st.line_chart(df.set_index("snapshot_date")[["auc","f1"]])
st.subheader("Volumes & Positives")
st.line_chart(df.set_index("snapshot_date")[["n_scored","pos_rate_pred","pos_rate_true"]])
