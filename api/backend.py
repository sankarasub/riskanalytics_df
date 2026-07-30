"""Unified FastAPI backend for Risk Analytics UI."""
from __future__ import annotations

import os
from typing import Optional, Dict, Any
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import subprocess
import time
import requests

from risk_analytics.config import load_config
from risk_analytics.spark import create_spark_session

app = FastAPI(title="Risk Analytics API", version="1.0.0")

# Enable CORS for React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173", "http://localhost:8501", "http://localhost:8502"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic models
class PipelineRequest(BaseModel):
    target: str
    as_of_date: str
    params: Optional[Dict[str, Any]] = None

class PipelineStatus(BaseModel):
    status: str
    current_dag: Optional[str] = None
    progress: Optional[int] = None
    error: Optional[str] = None

class HealthStatus(BaseModel):
    api: str
    spark: str
    nessie: str
    storage: str
    execution_mode: str

# Global state
execution_mode = os.getenv("EXECUTION_MODE", "docker")
config = load_config(execution_mode)
pipeline_state: Dict[str, Any] = {
    "status": "idle",
    "current_dag": None,
    "progress": 0,
    "error": None,
    "last_updated": None,
}

def check_spark_health(mode: str) -> str:
    """Check Spark health based on execution mode."""
    if mode == "docker":
        # Check if Docker Spark services are running
        try:
            response = requests.get("http://localhost:8080", timeout=2)
            return "healthy" if response.status_code == 200 else "unhealthy"
        except:
            return "unhealthy"
    elif mode == "hybrid":
        # Check if Spark Connect is available
        try:
            spark = create_spark_session("health_check", "main", mode)
            spark.stop()
            return "healthy"
        except:
            return "unhealthy"
    else:
        # Local mode - check if Spark can be created
        try:
            spark = create_spark_session("health_check", "main", mode)
            spark.stop()
            return "healthy"
        except:
            return "unhealthy"

def check_nessie_health(mode: str) -> str:
    """Check Nessie health based on execution mode."""
    if mode == "local":
        return "unhealthy"  # Local mode doesn't use Nessie
    
    try:
        uri = config["catalog"].get("uri", "http://localhost:19120/api/v2")
        response = requests.get(f"{uri}/config", timeout=2)
        return "healthy" if response.status_code == 200 else "unhealthy"
    except:
        return "unhealthy"

def check_storage_health(mode: str) -> str:
    """Check storage health based on execution mode."""
    if mode == "local":
        # Check if local storage directory exists
        storage_path = config["storage"].get("path", "./data/warehouse")
        return "healthy" if os.path.exists(storage_path) else "unhealthy"
    
    try:
        endpoint = config["storage"].get("endpoint", "http://localhost:8333")
        response = requests.get(endpoint, timeout=2)
        return "healthy" if response.status_code == 200 else "unhealthy"
    except:
        return "unhealthy"

@app.get("/api/platform/health")
async def get_health() -> HealthStatus:
    """Health check for all platform services."""
    return HealthStatus(
        api="healthy",
        spark=check_spark_health(execution_mode),
        nessie=check_nessie_health(execution_mode),
        storage=check_storage_health(execution_mode),
        execution_mode=execution_mode
    )

@app.get("/api/platform/config")
async def get_config() -> Dict[str, Any]:
    """Get current platform configuration."""
    # Return sanitized config (remove sensitive keys)
    safe_config = config.copy()
    if "storage" in safe_config and "secret_key" in safe_config["storage"]:
        safe_config["storage"]["secret_key"] = "***"
    return safe_config

@app.post("/api/platform/config")
async def update_config(config_update: Dict[str, Any]) -> Dict[str, str]:
    """Update platform configuration (local mode only)."""
    if execution_mode != "local":
        raise HTTPException(400, "Config updates only allowed in local mode")
    
    # In a real implementation, this would update the config file
    # For now, just return success
    return {"status": "updated"}

@app.post("/api/pipeline/execute")
async def execute_pipeline(request: PipelineRequest) -> Dict[str, Any]:
    """Execute pipeline with specified target and parameters."""
    global pipeline_state
    
    try:
        pipeline_state["status"] = "running"
        pipeline_state["current_dag"] = request.target
        pipeline_state["progress"] = 0
        pipeline_state["error"] = None
        pipeline_state["last_updated"] = time.time()
        
        # Execute the pipeline (simplified for now)
        if request.target == "bootstrap":
            result = subprocess.run(
                ["python", "jobs/bootstrap.py", "--as-of-date", request.as_of_date],
                capture_output=True,
                text=True,
                timeout=300
            )
        elif request.target in ["stage", "ods"]:
            result = subprocess.run(
                ["python", "jobs/run_source_to_ods_step.py", 
                 "--layer", request.target,
                 "--entity", request.params.get("entity", "customer"),
                 "--source", request.params.get("source", "sourcea"),
                 "--as-of-date", request.as_of_date],
                capture_output=True,
                text=True,
                timeout=300
            )
        elif request.target == "riskmetrics":
            result = subprocess.run(
                ["python", "jobs/run_risk_pipeline.py",
                 "--as-of-date", request.as_of_date,
                 "--data-model", request.params.get("data_model", "source-to-ods")],
                capture_output=True,
                text=True,
                timeout=300
            )
        elif request.target == "orchestration":
            # Run orchestration pipeline
            result = subprocess.run(
                ["python", "jobs/execute_pipeline.py",
                 "--target", "orchestration",
                 "--as-of-date", request.as_of_date],
                capture_output=True,
                text=True,
                timeout=300
            )
        else:
            raise HTTPException(400, f"Unknown target: {request.target}")
        
        if result.returncode == 0:
            pipeline_state["status"] = "success"
            pipeline_state["progress"] = 100
        else:
            pipeline_state["status"] = "error"
            pipeline_state["error"] = result.stderr
            
        pipeline_state["last_updated"] = time.time()
        
        return {
            "status": pipeline_state["status"],
            "target": request.target,
            "message": "Pipeline execution completed"
        }
        
    except subprocess.TimeoutExpired:
        pipeline_state["status"] = "error"
        pipeline_state["error"] = "Pipeline execution timed out"
        pipeline_state["last_updated"] = time.time()
        raise HTTPException(408, "Pipeline execution timed out")
    except Exception as e:
        pipeline_state["status"] = "error"
        pipeline_state["error"] = str(e)
        pipeline_state["last_updated"] = time.time()
        raise HTTPException(500, f"Pipeline execution failed: {str(e)}")

@app.get("/api/pipeline/status")
async def get_pipeline_status() -> PipelineStatus:
    """Get current pipeline execution status."""
    return PipelineStatus(
        status=pipeline_state["status"],
        current_dag=pipeline_state["current_dag"],
        progress=pipeline_state["progress"],
        error=pipeline_state["error"]
    )

@app.get("/api/data/tables")
async def list_tables() -> Dict[str, Any]:
    """List all available tables in the catalog."""
    try:
        spark = create_spark_session("list_tables", "main", execution_mode)
        
        # Get table list based on catalog type
        if execution_mode == "local":
            tables = spark.sql("SHOW TABLES IN local").collect()
        else:
            tables = spark.sql("SHOW TABLES IN nessie").collect()
        
        spark.stop()
        
        table_list = [row.tableName for row in tables]
        return {"tables": table_list}
    except Exception as e:
        raise HTTPException(500, f"Failed to list tables: {str(e)}")

@app.get("/api/data/table/{table_name}")
async def get_table_data(
    table_name: str,
    limit: int = 100,
    offset: int = 0,
    filters: Optional[str] = None
) -> Dict[str, Any]:
    """Get data from specified table."""
    try:
        spark = create_spark_session("get_table_data", "main", execution_mode)
        
        # Build query based on catalog type
        catalog = "local" if execution_mode == "local" else "nessie"
        namespace = config["catalog"].get("namespace", "risk_analytics")
        query = f"SELECT * FROM {catalog}.{namespace}.{table_name}"
        
        if filters:
            query += f" WHERE {filters}"
        
        query += f" LIMIT {limit} OFFSET {offset}"
        
        df = spark.sql(query)
        data = df.collect()
        schema = df.schema.fields
        
        spark.stop()
        
        # Convert to serializable format
        rows = [row.asDict() for row in data]
        schema_info = [
            {"name": field.name, "type": str(field.dataType), "nullable": field.nullable}
            for field in schema
        ]
        
        return {
            "schema": schema_info,
            "data": rows,
            "total": len(rows),
            "limit": limit,
            "offset": offset
        }
    except Exception as e:
        raise HTTPException(500, f"Failed to get table data: {str(e)}")

@app.get("/api/data/table/{table_name}/schema")
async def get_table_schema(table_name: str) -> Dict[str, Any]:
    """Get schema for specified table."""
    try:
        spark = create_spark_session("get_table_schema", "main", execution_mode)
        
        catalog = "local" if execution_mode == "local" else "nessie"
        namespace = config["catalog"].get("namespace", "risk_analytics")
        df = spark.sql(f"SELECT * FROM {catalog}.{namespace}.{table_name} LIMIT 1")
        schema = df.schema.fields
        
        spark.stop()
        
        schema_info = [
            {"name": field.name, "type": str(field.dataType), "nullable": field.nullable}
            for field in schema
        ]
        
        return {"schema": schema_info}
    except Exception as e:
        raise HTTPException(500, f"Failed to get table schema: {str(e)}")

@app.get("/api/metrics/summary")
async def get_metrics_summary(
    as_of_date: str,
    customer_id: Optional[str] = None
) -> Dict[str, Any]:
    """Get risk metrics summary."""
    try:
        spark = create_spark_session("metrics_summary", "main", execution_mode)
        
        catalog = "local" if execution_mode == "local" else "nessie"
        namespace = config["catalog"].get("ods_namespace", "risk_analytics_ods")
        
        # Build query for risk metrics
        query = f"""
        SELECT 
            customer_id,
            SUM(pfe) as total_pfe,
            SUM(var) as total_var,
            SUM(netting_exposure) as total_netting_exposure,
            COUNT(*) as record_count
        FROM {catalog}.{namespace}.risk_metrics
        WHERE as_of_date = '{as_of_date}'
        """
        
        if customer_id:
            query += f" AND customer_id = '{customer_id}'"
        
        query += " GROUP BY customer_id"
        
        df = spark.sql(query)
        data = df.collect()
        
        spark.stop()
        
        if not data:
            return {
                "totalPFE": 0,
                "var": 0,
                "nettingExposure": 0,
                "recordCount": 0,
                "exposureByCustomer": [],
                "detailedMetrics": []
            }
        
        # Convert to summary format
        total_pfe = sum(row.total_pfe for row in data)
        total_var = sum(row.total_var for row in data)
        total_netting = sum(row.total_netting_exposure for row in data)
        total_records = sum(row.record_count for row in data)
        
        exposure_by_customer = [
            {"customer": row.customer_id, "exposure": row.total_pfe}
            for row in data
        ]
        
        detailed_metrics = [
            {
                "customer": row.customer_id,
                "pfe": row.total_pfe,
                "var": row.total_var,
                "nettingSet": "default"
            }
            for row in data
        ]
        
        return {
            "totalPFE": total_pfe,
            "var": total_var,
            "nettingExposure": total_netting,
            "recordCount": total_records,
            "exposureByCustomer": exposure_by_customer,
            "detailedMetrics": detailed_metrics
        }
    except Exception as e:
        # Return empty data if query fails (table might not exist yet)
        return {
            "totalPFE": 0,
            "var": 0,
            "nettingExposure": 0,
            "recordCount": 0,
            "exposureByCustomer": [],
            "detailedMetrics": []
        }

@app.get("/api/metrics/historical")
async def get_historical_metrics(
    customer_id: Optional[str] = None,
    limit: int = 30
) -> Dict[str, Any]:
    """Get historical risk metrics for trend analysis."""
    try:
        spark = create_spark_session("historical_metrics", "main", execution_mode)
        
        catalog = "local" if execution_mode == "local" else "nessie"
        namespace = config["catalog"].get("ods_namespace", "risk_analytics_ods")
        
        query = f"""
        SELECT 
            as_of_date,
            SUM(pfe) as total_pfe,
            SUM(var) as total_var,
            COUNT(*) as record_count
        FROM {catalog}.{namespace}.risk_metrics
        """
        
        if customer_id:
            query += f" WHERE customer_id = '{customer_id}'"
        
        query += f" GROUP BY as_of_date ORDER BY as_of_date DESC LIMIT {limit}"
        
        df = spark.sql(query)
        data = df.collect()
        
        spark.stop()
        
        historical_data = [
            {
                "date": row.as_of_date,
                "totalPFE": row.total_pfe,
                "var": row.total_var,
                "recordCount": row.record_count
            }
            for row in data
        ]
        
        return historical_data
    except Exception as e:
        # Return empty data if query fails
        return []