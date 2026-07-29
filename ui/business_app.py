import time
from datetime import datetime

import pandas as pd
import streamlit as st

from ui.common import (
    DAG_LAYERS,
    ENTITIES,
    SOURCE_LABELS,
    get_dag_overview,
    get_kafka_topic_stats,
    get_kafka_topics,
    get_platform_health,
    get_risk_run_history,
    get_table_counts,
    kafka_ods_dag_id,
    kafka_stage_dag_id,
    nessie_ui_url,
    read_risk_metrics,
)

REFRESH_SECONDS = 30

st.set_page_config(page_title="Risk Analytics Dashboard", layout="wide")
st.title("Risk Analytics Business Dashboard")
st.caption("Real-time risk metrics and pipeline monitoring")

# Sidebar for filtering
st.sidebar.header("Filters")
as_of_date = st.sidebar.date_input("As of Date", value=datetime.now().date())
auto_refresh = st.sidebar.checkbox(f"Auto-refresh ({REFRESH_SECONDS}s)", value=False)

health = get_platform_health()
if health.get("status") == "ok":
    st.sidebar.success("Platform healthy")
else:
    st.sidebar.warning(f"Platform status: {health.get('status', 'unknown')}")
for component, detail in (health.get("components") or {}).items():
    st.sidebar.caption(f"{component}: {detail.get('status', detail)}")
st.sidebar.markdown(f"[Nessie catalog UI]({nessie_ui_url()})")

# Main tabs
tab1, tab2, tab3, tab4 = st.tabs(["Risk Metrics", "Pipeline Status", "Streaming Monitor", "Historical Runs"])

# Tab 1: Risk Metrics
with tab1:
    st.header("Current Risk Exposure")

    try:
        data = read_risk_metrics()

        # Filter by as_of_date if available
        if "as_of_date" in data.columns:
            data["as_of_date"] = pd.to_datetime(data["as_of_date"]).dt.date
            filtered_data = data[data["as_of_date"] == as_of_date]
        else:
            filtered_data = data

        if filtered_data.empty:
            st.warning(f"No data found for {as_of_date}. Showing all available data.")
            filtered_data = data

        # Overall metrics
        st.subheader("Portfolio Summary")
        metrics_col1, metrics_col2, metrics_col3, metrics_col4 = st.columns(4)
        metrics_col1.metric("Total PFE", f"${filtered_data['pfe'].sum():,.0f}", delta="Potential Future Exposure")
        metrics_col2.metric("Total VaR", f"${filtered_data['var'].sum():,.0f}", delta="Value at Risk")
        metrics_col3.metric("Netting Exposure", f"${filtered_data['netting_exposure'].sum():,.0f}", delta="After Netting")
        metrics_col4.metric("Total Records", f"{len(filtered_data):,}")

        # Customer filtering
        if "customer_name" in filtered_data.columns:
            customers = sorted(filtered_data["customer_name"].dropna().unique().tolist())
            selected_customers = st.multiselect("Filter by Customer", customers, default=customers)
            customer_filtered = filtered_data[filtered_data["customer_name"].isin(selected_customers)]
        else:
            customer_filtered = filtered_data

        # Metrics by customer
        if not customer_filtered.empty and "customer_name" in customer_filtered.columns:
            st.subheader("Exposure by Customer")
            customer_metrics = customer_filtered.groupby("customer_name")[["pfe", "var", "netting_exposure"]].sum()
            st.bar_chart(customer_metrics)

            # Detailed table
            st.subheader("Detailed Risk Metrics")
            display_columns = ["customer_name", "netting_set_id", "gross_exposure", "netting_exposure",
                              "collateral_value_after_haircut", "pfe", "var", "as_of_date"]
            available_columns = [col for col in display_columns if col in customer_filtered.columns]
            st.dataframe(
                customer_filtered[available_columns].sort_values(["customer_name"] if "customer_name" in customer_filtered.columns else []),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.dataframe(filtered_data, use_container_width=True, hide_index=True)

    except Exception as error:
        st.error(f"Risk metrics are unavailable. Bootstrap the platform first. Details: {error}")

# Tab 2: Pipeline Status
with tab2:
    st.header("Pipeline Execution Status")

    filter_col1, filter_col2 = st.columns(2)
    selected_layers = filter_col1.multiselect("Pipeline layer", DAG_LAYERS, default=list(DAG_LAYERS))
    selected_sources = filter_col2.multiselect(
        "Source", [*SOURCE_LABELS, "kafka", "-"], default=[*SOURCE_LABELS, "kafka", "-"]
    )

    st.subheader("Latest state per DAG")
    overview = [row for row in get_dag_overview(tuple(selected_layers)) if row["source"] in selected_sources]
    if overview:
        overview_df = pd.DataFrame(overview)[
            ["dag_id", "layer", "source", "entity", "registered", "paused", "last_run_state", "last_run_start"]
        ]
        state_counts = overview_df["last_run_state"].value_counts().to_dict()
        summary_cols = st.columns(max(len(state_counts), 1))
        for index, (state, count) in enumerate(sorted(state_counts.items())):
            summary_cols[index].metric(state, count)
        st.dataframe(overview_df, use_container_width=True, hide_index=True)
        missing = [row["dag_id"] for row in overview if not row["registered"]]
        if missing:
            st.warning(f"Not registered in Airflow (is the scheduler parsing the DAG folder?): {', '.join(missing)}")
    else:
        st.info("No DAGs match the selected filters, or Airflow is unavailable.")

    st.divider()
    st.subheader("Data Freshness")
    if st.button("Check Data Freshness"):
        try:
            counts = get_table_counts(as_of_date.isoformat())
            if counts.get("status") == "success":
                table_counts = counts.get("counts", {})
                freshness = [
                    {"Table": table.split(".")[-1], "Rows": count, "Populated": count > 0}
                    for table, count in table_counts.items()
                ]
                st.dataframe(pd.DataFrame(freshness), use_container_width=True, hide_index=True)
            else:
                st.error(f"Failed to check data freshness: {counts.get('message')}")
        except Exception as error:
            st.error(f"Failed to check data freshness: {error}")

# Tab 3: Streaming Monitor
with tab3:
    st.header("Kafka Streaming Monitor")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Kafka Topics")
        if st.button("Refresh Topics"):
            try:
                topics = get_kafka_topics()
                if topics:
                    risk_topics = [t for t in topics if "risk" in t.get("name", "").lower()]

                    if risk_topics:
                        st.success(f"Found {len(risk_topics)} risk-related topics")

                        for topic in risk_topics:
                            with st.expander(f"Topic: {topic.get('name')}"):
                                st.write(f"**Partitions:** {topic.get('partitions', 'N/A')}")
                                st.write(f"**Internal:** {'Yes' if topic.get('internal') else 'No'}")

                                if st.button(f"View Stats - {topic.get('name')}", key=f"stats_{topic.get('name')}"):
                                    stats = get_kafka_topic_stats(topic.get('name'))
                                    if "error" not in stats:
                                        st.json(stats)
                                    else:
                                        st.warning(f"Could not fetch stats: {stats.get('error')}")
                    else:
                        st.info("No risk-related topics found")
                else:
                    st.warning("No topics found or Kafka unavailable")
            except Exception as error:
                st.error(f"Failed to fetch topics: {error}")

    with col2:
        st.subheader("Event-driven DAG state")
        st.caption("Each ingest topic feeds ra_kafka_<entity>_stage, which chains into the ODS load and risk metrics.")

        streaming_rows = {row["dag_id"]: row for row in get_dag_overview(("kafka-stage", "kafka-ods"))}
        streaming_table = []
        for entity in ENTITIES:
            stage_row = streaming_rows.get(kafka_stage_dag_id(entity), {})
            ods_row = streaming_rows.get(kafka_ods_dag_id(entity), {})
            streaming_table.append(
                {
                    "Entity": entity,
                    "Ingest topic": f"risk.{entity}.ingest",
                    "Stage DAG state": stage_row.get("last_run_state", "-"),
                    "Stage sensor paused": stage_row.get("paused"),
                    "ODS DAG state": ods_row.get("last_run_state", "-"),
                }
            )
        st.dataframe(pd.DataFrame(streaming_table), use_container_width=True, hide_index=True)

        selected_entity = st.selectbox("Inspect ingest topic", ENTITIES)
        if st.button("Check topic offsets"):
            stats = get_kafka_topic_stats(f"risk.{selected_entity}.ingest")
            if "error" in stats:
                st.warning(f"Topic check failed: {stats.get('error')}")
            else:
                st.json(stats)

# Tab 4: Historical Runs
with tab4:
    st.header("Historical Risk Runs")

    if st.button("Load Run History"):
        try:
            history = get_risk_run_history(limit=20)

            if history:
                st.success(f"Found {len(history)} historical runs")

                # Convert to DataFrame for display
                history_df = pd.DataFrame(history)

                # Format the data for display
                if not history_df.empty:
                    history_df['calculation_timestamp'] = pd.to_datetime(history_df['calculation_timestamp'])
                    history_df = history_df.sort_values('calculation_timestamp', ascending=False)

                    # Display summary metrics
                    st.subheader("Run Summary")
                    summary_col1, summary_col2, summary_col3 = st.columns(3)
                    summary_col1.metric("Total Runs", len(history_df))
                    summary_col2.metric("Avg Records/Run", f"{history_df['record_count'].mean():,.0f}")
                    summary_col3.metric("Latest PFE", f"${history_df.iloc[0]['total_pfe']:,.0f}" if len(history_df) > 0 else "N/A")

                    # Detailed table
                    st.subheader("Run Details")
                    display_cols = ['risk_run_id', 'as_of_date', 'calculation_timestamp', 'record_count', 'total_pfe', 'total_var']
                    available_cols = [col for col in display_cols if col in history_df.columns]
                    st.dataframe(history_df[available_cols], use_container_width=True, hide_index=True)

                    # Trend visualization
                    if len(history_df) > 1:
                        st.subheader("PFE Trend Over Time")
                        trend_data = history_df[['calculation_timestamp', 'total_pfe']].sort_values('calculation_timestamp')
                        st.line_chart(trend_data.set_index('calculation_timestamp'))
                else:
                    st.info("No historical run data available")
            else:
                st.info("No run history found. Run the risk pipeline to generate history.")
        except Exception as error:
            st.error(f"Failed to load run history: {error}")

# Auto-refresh logic: sleep first, otherwise the rerun loop starves the browser.
if auto_refresh:
    st.sidebar.success(f"Auto-refresh enabled ({REFRESH_SECONDS}s)")
    time.sleep(REFRESH_SECONDS)
    st.rerun()

