from pathlib import Path
import json
import streamlit as st
import yaml
import pandas as pd
from ui.common import (
    execute_transform_pipeline,
    list_transform_pipelines,
    load_pipeline_yaml_text,
    nessie_references,
    nessie_ui_url,
    BOOTSTRAP_DAG_ID,
    publish_kafka_event,
    preview_transform_pipeline,
    save_pipeline_yaml_text,
    trigger_source_to_ods,
    trigger_airflow_dag,
    validate_transform_pipeline,
    get_docker_platform_status,
    start_docker_platform,
    stop_docker_platform,
    list_airflow_dags,
    get_airflow_dag_status,
    get_airflow_dag_runs,
    get_table_counts,
    get_table_preview,
    get_kafka_topics,
    get_kafka_topic_stats,
    get_risk_run_history,
    trigger_bootstrap_dag,
    trigger_risk_metrics_dag,
)

st.set_page_config(page_title="Risk Analytics Developer Control Plane", layout="wide")
st.title("Risk Analytics Developer Control Plane")

# Sidebar navigation
page = st.sidebar.selectbox(
    "Navigation",
    [
        "Platform Management",
        "Data Pipeline",
        "Data Viewer",
        "Airflow Monitoring",
        "Kafka Streaming",
        "Pipeline Studio",
    ],
)

as_of_date = st.sidebar.date_input("Risk as-of date")

# Page 1: Platform Management
if page == "Platform Management":
    st.header("Docker Platform Management")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("Check Status", type="secondary"):
            with st.spinner("Checking platform status..."):
                status = get_docker_platform_status()
                if status.get("status") == "success":
                    st.success(f"Platform running: {status.get('running')}/{status.get('total')} services")
                    st.json(status.get("services", []))
                else:
                    st.error(f"Status check failed: {status.get('message')}")
    
    with col2:
        build_mode = st.checkbox("Rebuild images", value=False)
        if st.button("Start Platform", type="primary"):
            with st.spinner("Starting platform..."):
                mode = "build" if build_mode else "up"
                result = start_docker_platform(mode)
                if result.get("status") == "success":
                    st.success("Platform started successfully")
                else:
                    st.error(f"Failed to start platform: {result.get('message')}")
    
    with col3:
        if st.button("Stop Platform", type="secondary"):
            with st.spinner("Stopping platform..."):
                result = stop_docker_platform()
                if result.get("status") == "success":
                    st.success("Platform stopped successfully")
                else:
                    st.error(f"Failed to stop platform: {result.get('message')}")
    
    st.divider()
    
    # Auto-refresh status
    if st.checkbox("Auto-refresh status", value=False):
        status = get_docker_platform_status()
        if status.get("status") == "success":
            st.subheader(f"Platform Status: {status.get('running')}/{status.get('total')} services running")
            
            # Create service status table
            services_data = []
            for service in status.get("services", []):
                services_data.append({
                    "Service": service.get("Name", ""),
                    "State": service.get("State", ""),
                    "Ports": service.get("Publishers", ""),
                })
            
            if services_data:
                df = pd.DataFrame(services_data)
                st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.warning(f"Could not fetch status: {status.get('message')}")

# Page 2: Data Pipeline
elif page == "Data Pipeline":
    st.header("Data Pipeline Execution")
    
    # Pipeline steps
    st.subheader("Pipeline Orchestration")
    
    pipeline_tab1, pipeline_tab2, pipeline_tab3 = st.tabs(["Bootstrap", "Source-to-ODS", "Risk Metrics"])
    
    with pipeline_tab1:
        st.write("Create tables and load seed data")
        col1, col2 = st.columns(2)
        if col1.button("Run Bootstrap", type="primary"):
            with st.spinner("Running bootstrap..."):
                try:
                    result = trigger_bootstrap_dag(as_of_date.isoformat())
                    st.success(f"Bootstrap triggered: {result.get('dag_run_id')}")
                except Exception as error:
                    st.error(f"Bootstrap failed: {error}")
        
        if col2.button("Run Bootstrap via Airflow"):
            with st.spinner("Triggering via Airflow..."):
                try:
                    result = trigger_airflow_dag(BOOTSTRAP_DAG_ID, f"{as_of_date.isoformat()}T00:00:00Z")
                    st.success(f"Airflow DAG triggered: {result.get('dag_run_id')}")
                except Exception as error:
                    st.error(f"Airflow trigger failed: {error}")
    
    with pipeline_tab2:
        st.write("Stage and ODS transformations")
        
        # Entity selection
        entities = ["customer", "asset", "collateral", "deals"]
        sources = ["sourcea", "sourceb"]
        
        col1, col2, col3 = st.columns(3)
        selected_entities = col1.multiselect("Select Entities", entities, default=entities)
        selected_source = col2.selectbox("Source", sources)
        
        with st.expander("SourceB File Paths"):
            customer_sourceb_path = st.text_input("customer_sourceb_path", value="/opt/risk_analytics/data/sourceb/customer/*.csv")
            asset_sourceb_path = st.text_input("asset_sourceb_path", value="/opt/risk_analytics/data/sourceb/asset/*.json")
            product_sourceb_path = st.text_input("product_sourceb_path", value="/opt/risk_analytics/data/sourceb/product/*.json")
            trans_sourceb_path = st.text_input("trans_sourceb_path", value="/opt/risk_analytics/data/sourceb/trans/*.csv")
            collateral_sourceb_path = st.text_input("collateral_sourceb_path", value="/opt/risk_analytics/data/sourceb/collateral/*.json")
        
        col3.write("")
        col3.write("")
        run_all_button = col3.button("Run All Selected Entities", type="primary")
        
        if run_all_button:
            progress_bar = st.progress(0)
            results = []
            
            for i, entity in enumerate(selected_entities):
                try:
                    # Run stage
                    with st.spinner(f"Running stage for {entity}..."):
                        result = trigger_source_to_ods(
                            mode="stage",
                            entity=entity,
                            source=selected_source,
                            as_of_date=as_of_date.isoformat(),
                            paths={
                                "customer_sourceb_path": customer_sourceb_path,
                                "asset_sourceb_path": asset_sourceb_path,
                                "product_sourceb_path": product_sourceb_path,
                                "trans_sourceb_path": trans_sourceb_path,
                                "collateral_sourceb_path": collateral_sourceb_path,
                            } if selected_source == "sourceb" else {},
                        )
                        results.append({"entity": entity, "stage": result})
                    
                    # Run ODS
                    with st.spinner(f"Running ODS for {entity}..."):
                        result = trigger_source_to_ods(
                            mode="ods",
                            entity=entity,
                            source=selected_source,
                            as_of_date=as_of_date.isoformat(),
                            paths={
                                "customer_sourceb_path": customer_sourceb_path,
                                "asset_sourceb_path": asset_sourceb_path,
                                "product_sourceb_path": product_sourceb_path,
                                "trans_sourceb_path": trans_sourceb_path,
                                "collateral_sourceb_path": collateral_sourceb_path,
                            } if selected_source == "sourceb" else {},
                        )
                        results[-1]["ods"] = result
                    
                    progress_bar.progress((i + 1) / len(selected_entities))
                    
                except Exception as error:
                    results.append({"entity": entity, "error": str(error)})
            
            st.success("Pipeline execution completed")
            st.json(results)
    
    with pipeline_tab3:
        st.write("Risk metrics calculation")
        col1, col2 = st.columns(2)
        
        data_model = col1.selectbox("Data Model", ["source-to-ods", "legacy"])
        
        if col2.button("Run Risk Metrics", type="primary"):
            with st.spinner("Calculating risk metrics..."):
                try:
                    result = trigger_risk_metrics_dag(as_of_date.isoformat(), data_model)
                    st.success(f"Risk metrics triggered: {result.get('dag_run_id')}")
                except Exception as error:
                    st.error(f"Risk metrics failed: {error}")

# Page 3: Data Viewer
elif page == "Data Viewer":
    st.header("Data Lakehouse Viewer")
    
    # Table counts
    st.subheader("Table Row Counts")
    if st.button("Refresh Counts"):
        with st.spinner("Fetching table counts..."):
            counts_result = get_table_counts(as_of_date.isoformat())
            if counts_result.get("status") == "success":
                counts = counts_result.get("counts", {})
                
                # Display as metrics
                metrics_cols = st.columns(len(counts))
                for i, (table, count) in enumerate(counts.items()):
                    table_name = table.split(".")[-1]
                    metrics_cols[i].metric(table_name, f"{count:,}")
                
                # Display as table
                counts_df = pd.DataFrame([
                    {"Table": table.split(".")[-1], "Count": count}
                    for table, count in counts.items()
                ])
                st.dataframe(counts_df, use_container_width=True, hide_index=True)
            else:
                st.error(f"Failed to fetch counts: {counts_result.get('message')}")
    
    # Table preview
    st.divider()
    st.subheader("Table Preview")
    
    tables = [
        "nessie.risk_analytics_ods.customer",
        "nessie.risk_analytics_ods.asset",
        "nessie.risk_analytics_ods.collateral",
        "nessie.risk_analytics_ods.deals",
        "nessie.risk_analytics_ods.risk_metrics",
    ]
    
    selected_table = st.selectbox("Select Table", tables)
    row_limit = st.slider("Row Limit", 5, 100, 20)
    
    if st.button("Preview Table"):
        with st.spinner("Loading table preview..."):
            preview_result = get_table_preview(selected_table, row_limit)
            if preview_result.get("status") == "success":
                st.info(f"Total rows in table: {preview_result.get('row_count'):,}")
                st.dataframe(preview_result.get("preview"), use_container_width=True, hide_index=True)
            else:
                st.error(f"Failed to load preview: {preview_result.get('message')}")

# Page 4: Airflow Monitoring
elif page == "Airflow Monitoring":
    st.header("Airflow DAG Monitoring")
    
    # List DAGs
    if st.button("Refresh DAG List"):
        with st.spinner("Fetching DAGs..."):
            dags = list_airflow_dags()
            if dags:
                st.success(f"Found {len(dags)} DAGs")
                
                # Filter for risk analytics DAGs
                risk_dags = [dag for dag in dags if "risk_analytics" in dag.get("dag_id", "")]
                
                for dag in risk_dags:
                    with st.expander(f"{dag.get('dag_id')}"):
                        col1, col2, col3 = st.columns(3)
                        col1.write(f"**Active:** {dag.get('is_active')}")
                        col2.write(f"**Paused:** {dag.get('is_paused')}")
                        col3.write(f"**Last Parsed:** {dag.get('last_parsed')}")
                        
                        if st.button(f"Check Status - {dag.get('dag_id')}", key=f"status_{dag.get('dag_id')}"):
                            status = get_airflow_dag_status(dag.get("dag_id"))
                            st.json(status)
                        
                        if st.button(f"View Runs - {dag.get('dag_id')}", key=f"runs_{dag.get('dag_id')}"):
                            runs = get_airflow_dag_runs(dag.get("dag_id"), limit=5)
                            if runs:
                                runs_df = pd.DataFrame(runs)
                                st.dataframe(runs_df, use_container_width=True, hide_index=True)
                            else:
                                st.info("No runs found")
            else:
                st.warning("No DAGs found or Airflow unavailable")
    
    # Risk run history
    st.divider()
    st.subheader("Risk Run History")
    
    if st.button("Load Run History"):
        with st.spinner("Loading run history..."):
            history = get_risk_run_history(limit=10)
            if history:
                history_df = pd.DataFrame(history)
                st.dataframe(history_df, use_container_width=True, hide_index=True)
            else:
                st.info("No run history found")

# Page 5: Kafka Streaming
elif page == "Kafka Streaming":
    st.header("Kafka Streaming Controls")
    
    # Kafka topics
    st.subheader("Kafka Topics")
    if st.button("Refresh Topics"):
        with st.spinner("Fetching topics..."):
            topics = get_kafka_topics()
            if topics:
                st.success(f"Found {len(topics)} topics")
                topics_df = pd.DataFrame(topics)
                st.dataframe(topics_df, use_container_width=True, hide_index=True)
            else:
                st.warning("No topics found or Kafka unavailable")
    
    # Topic stats
    st.divider()
    st.subheader("Topic Statistics")
    
    topic_name = st.text_input("Topic Name", value="risk.deals.ingest")
    if st.button("Get Topic Stats"):
        with st.spinner("Fetching topic stats..."):
            stats = get_kafka_topic_stats(topic_name)
            if "error" not in stats:
                st.json(stats)
            else:
                st.error(f"Failed to fetch stats: {stats.get('error')}")
    
    # Publish events
    st.divider()
    st.subheader("Publish Kafka Events")
    
    kafka_col1, kafka_col2 = st.columns(2)
    kafka_entity = kafka_col1.selectbox("Entity", ["trades", "customer", "asset", "collateral"], index=0)
    kafka_trigger_pipeline = kafka_col2.checkbox("Trigger pipeline after publish", value=False)
    
    kafka_payload_text = st.text_area(
        "Kafka payload JSON",
        value=json.dumps({"record_id": "demo-001", "product_type": "SWAP", "status": "ACTIVE"}, indent=2),
        height=120,
    )
    
    if st.button("Publish Event", type="primary"):
        try:
            payload = json.loads(kafka_payload_text) if kafka_payload_text.strip() else {}
            result = publish_kafka_event(
                entity=kafka_entity,
                payload=payload,
                as_of_date=as_of_date.isoformat(),
                trigger_pipeline=kafka_trigger_pipeline,
            )
            st.success(f"Published to {result.get('topic')}")
            if result.get("pipeline_triggered"):
                st.info(f"Triggered pipeline: {result.get('pipeline_dag_run_id')}")
        except Exception as error:
            st.error(f"Publish failed: {error}")

# Page 6: Pipeline Studio (existing functionality)
elif page == "Pipeline Studio":
    st.header("Metadata Pipeline Studio")
    
    # Runtime configuration
    config_path = Path(__file__).resolve().parent.parent / "config" / "platform.yaml"
    st.subheader("Runtime configuration")
    st.code(config_path.read_text(encoding="utf-8"), language="yaml")
    
    st.divider()
    
    # Nessie references
    st.subheader("Nessie catalog references")
    st.markdown(f"[Open the Nessie catalog UI]({nessie_ui_url()})")
    try:
        st.dataframe(nessie_references(), use_container_width=True, hide_index=True)
    except Exception as error:
        st.warning(f"Nessie is unavailable: {error}")
    
    st.divider()
    
    # Pipeline YAML editor
    try:
        available = list_transform_pipelines()
    except Exception as error:
        available = []
        st.warning(f"Unable to list pipeline YAML files from API: {error}")

    if available:
        selected_pipeline = st.selectbox("Select pipeline YAML", available)
        editor_key = f"editor_{selected_pipeline}"

        if editor_key not in st.session_state:
            try:
                st.session_state[editor_key] = load_pipeline_yaml_text(selected_pipeline)
            except Exception as error:
                st.session_state[editor_key] = ""
                st.error(f"Unable to load pipeline file: {error}")

        st.caption("Edit YAML, validate, then execute the selected pipeline.")
        edited_yaml = st.text_area("Pipeline YAML", key=editor_key, height=420)

        default_param_json = json.dumps({"as_of_date": as_of_date.isoformat()}, indent=2)
        params_text = st.text_area("Runtime parameters (JSON)", value=default_param_json, height=120)

        col1, col2, col3, col4 = st.columns(4)

        if col1.button("Save YAML"):
            try:
                yaml.safe_load(edited_yaml)
                save_pipeline_yaml_text(selected_pipeline, edited_yaml)
                st.success("Pipeline YAML saved.")
            except Exception as error:
                st.error(f"Save failed: {error}")

        if col2.button("Validate YAML"):
            try:
                params = json.loads(params_text) if params_text.strip() else {}
                response = validate_transform_pipeline(selected_pipeline, params)
                if response.get("valid"):
                    st.success(f"Validation successful: {response.get('summary', {})}")
                else:
                    st.error(response.get("error", "Validation failed."))
            except Exception as error:
                st.error(f"Validation failed: {error}")

        if col3.button("Execute YAML", type="primary"):
            try:
                params = json.loads(params_text) if params_text.strip() else {}
                response = execute_transform_pipeline(selected_pipeline, params)
                if response.get("status") == "success":
                    st.success(f"Execution completed: {response.get('target_row_counts', {})}")
                else:
                    st.error(response.get("error", "Execution failed."))
            except Exception as error:
                st.error(f"Execution failed: {error}")

        if col4.button("Preview Rendered"):
            try:
                params = json.loads(params_text) if params_text.strip() else {}
                response = preview_transform_pipeline(selected_pipeline, params)
                if response.get("ok"):
                    st.success(f"Preview successful: {response.get('summary', {})}")
                    st.code(yaml.safe_dump(response.get("rendered", {}), sort_keys=False), language="yaml")
                else:
                    st.error(response.get("error", "Preview failed."))
            except Exception as error:
                st.error(f"Preview failed: {error}")
    else:
        st.info("No transform YAML pipelines found.")

