import streamlit as st
import pandas as pd
import glob
import os

st.set_page_config(page_title="Entity Resolution", layout="wide")

st.title("Distributed Entity Resolution and Record Linkage")
st.write("Big Data Project using Apache Spark")

st.subheader("Project Overview")
st.write(
    "This project identifies duplicate customer records from CSV and JSON "
    "datasets using data preprocessing, blocking, and similarity scoring."
)

def load_spark_csv(folder):
    files = glob.glob(os.path.join(folder, "part-*.csv"))
    if not files:
        return pd.DataFrame()
    return pd.concat(
        [pd.read_csv(file) for file in files],
        ignore_index=True
    )

results = load_spark_csv("output/match_results")
performance = load_spark_csv("results/performance")

if not results.empty:
    st.subheader("Matching Results")
    st.metric("Candidate Pairs", len(results))

    if "match_status" in results.columns:
        statuses = results["match_status"].astype(str).str.upper()
        col1, col2 = st.columns(2)
        col1.metric("Matched Pairs", int((statuses == "MATCH").sum()))
        col2.metric("Pairs for Review", int((statuses == "REVIEW").sum()))

    st.dataframe(results, use_container_width=True)

    if "confidence" in results.columns:
        st.subheader("Confidence Scores")
        st.bar_chart(results["confidence"].dropna())
else:
    st.warning(
        "Matching results not found. Run src/matching.py first."
    )

if not performance.empty:
    st.subheader("Performance Comparison")
    st.dataframe(performance, use_container_width=True)

    if "Method" in performance.columns and \
       "ExecutionTimeSeconds" in performance.columns:
        st.bar_chart(
            performance.set_index("Method")[["ExecutionTimeSeconds"]]
        )
else:
    st.info("Performance results will appear after running src/performance.py.")

st.subheader("Technologies")
st.write("Apache Spark | Python | CSV | JSON | Streamlit")

st.subheader("Team Responsibilities")
st.write("Member 1: Data collection and preprocessing")
st.write("Member 2: Record matching and confidence scoring")
st.write("Member 3: Evaluation, performance testing and dashboard")

st.caption("Demonstration results use a small sample dataset.")
