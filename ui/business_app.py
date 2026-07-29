import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from ui.common import (
    read_risk_metrics,
    get_risk_run_history,
    get_airflow_dag_status,
    get_airflow_dag_runs,
    get_kafka_topics,
    get_kafka_topic_stats,
    get_table_counts,
)

st.set_page_config(page_title="Risk Analytics Dashboard", layout="wide")
st.title("Risk Analytics Business Dashboard")
st.caption("Real-time risk metrics and pipeline monitoring")

# Sidebar for filtering
st.sidebar.header("Filters")
as_of_date = st.sidebar.date_input("As of Date", value=datetime.now().date())
auto_refresh = st.sidebar.checkbox("Auto-refresh (30s)", value=False)

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
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Recent Pipeline Runs")
        if st.button("Refresh Pipeline Status"):
            try:
                # Check key DAGs
                key_dags = [
                    "risk_analytics_pipeline",
                    "risk_analytics_source_to_ods_orchestration",
                    "risk_analytics_create_tables_and_load_data",
                ]
                
                for dag_id in key_dags:
                    with st.expander(f"DAG: {dag_id}"):
                        try:
                            status = get_airflow_dag_status(dag_id)
                            st.write(f"**Status:** {'Active' if status.get('is_active') else 'Inactive'}")
                            st.write(f"**Paused:** {'Yes' if status.get('is_paused') else 'No'}")
                            
                            if status.get('latest_run'):
                                latest = status['latest_run']
                                st.write(f"**Last Run:** {latest.get('dag_run_id')}")
                                st.write(f"**State:** {latest.get('state')}")
                                st.write(f"**Start Date:** {latest.get('start_date')}")
                            else:
                                st.info("No recent runs")
                        except Exception as e:
                            st.warning(f"Could not fetch status for {dag_id}: {e}")
            except Exception as error:
                st.error(f"Failed to fetch pipeline status: {error}")
    
    with col2:
        st.subheader("Data Freshness")
        if st.button("Check Data Freshness"):
            try:
                counts = get_table_counts(as_of_date.isoformat())
                if counts.get("status") == "success":
                    table_counts = counts.get("counts", {})
                    
                    for table, count in table_counts.items():
                        table_name = table.split(".")[-1]
                        if count > 0:
                            st.success(f"**{table_name}:** {count:,} records")
                        else:
                            st.warning(f"**{table_name}:** No data")
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
        st.subheader("Streaming Status")
        st.info("Real-time ingestion status")
        
        # Display streaming indicators
        streaming_entities = ["customer", "asset", "collateral", "deals"]
        
        for entity in streaming_entities:
            topic_name = f"risk.{entity}.ingest"
            with st.expander(f"Entity: {entity}"):
                st.write(f"**Topic:** {topic_name}")
                st.write(f"**Status:** Active")
                st.write(f"**Last Ingest:** Checking...")
                
                if st.button(f"Check {entity} Topic", key=f"check_{entity}"):
                    try:
                        stats = get_kafka_topic_stats(topic_name)
                        if "error" not in stats:
                            st.success("Topic is accessible")
                            st.json(stats)
                        else:
                            st.warning(f"Topic check failed: {stats.get('error')}")
                    except Exception as e:
                        st.error(f"Failed to check topic: {e}")

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

# Auto-refresh logic
if auto_refresh:
    st.sidebar.success("Auto-refresh enabled")
    st.rerun()

