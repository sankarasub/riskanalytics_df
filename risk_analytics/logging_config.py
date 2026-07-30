"""Centralized logging configuration for the Risk Analytics platform."""
from __future__ import annotations

import logging
import logging.config
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any
import json

# Centralized logging configuration for Splunk consumption
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
LOG_FILE = Path(__file__).resolve().parent.parent / "logs" / "pipeline_execution.log"

# Create logs directory if it doesn't exist
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

# Structured logging configuration for JSON/Splunk consumption
LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": LOG_FORMAT,
            "datefmt": LOG_DATE_FORMAT,
        },
        "detailed": {
            "format": "%(asctime)s | %(levelname)s | %(name)s | %(filename)s:%(lineno)d | %(funcName)s | %(message)s",
            "datefmt": LOG_DATE_FORMAT,
        },
        "splunk": {
            "format": '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}',
            "datefmt": "%Y-%m-%dT%H:%M:%S",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "INFO",
            "formatter": "standard",
            "stream": "ext://sys.stdout",
        },
        "file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "INFO",
            "formatter": "detailed",
            "filename": str(LOG_FILE),
            "maxBytes": 10485760,  # 10MB
            "backupCount": 5,
        },
        "splunk": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "INFO",
            "formatter": "splunk",
            "filename": str(LOG_FILE.parent / "splunk_pipeline.log"),
            "maxBytes": 10485760,  # 10MB
            "backupCount": 5,
        },
    },
    "loggers": {
        "": {
            "level": "INFO",
            "handlers": ["console", "file", "splunk"],
            "propagate": False,
        },
        "risk_analytics": {
            "level": "DEBUG",
            "handlers": ["console", "file", "splunk"],
            "propagate": False,
        },
        "jobs": {
            "level": "DEBUG",
            "handlers": ["console", "file", "splunk"],
            "propagate": False,
        },
    },
}

def setup_logging() -> None:
    """Configure centralized logging for the application."""
    logging.config.dictConfig(LOGGING_CONFIG)
    logging.info("Logging system initialized")


class PipelineLogger:
    """Structured logger for pipeline execution with performance metrics."""
    
    def __init__(self, name: str, execution_mode: str = None):
        self.logger = logging.getLogger(f"pipeline.{name}")
        self.execution_mode = execution_mode or os.getenv("EXECUTION_MODE", "docker")
        self.pipeline_start_time = time.time()
        self.step_start_time = time.time()
        self.metrics = {
            "pipeline_name": name,
            "execution_mode": self.execution_mode,
            "start_time": datetime.now().isoformat(),
            "steps": [],
            "total_records_processed": 0,
            "total_duration_seconds": 0,
            "status": "started",
            "errors": []
        }
    
    def log_pipeline_start(self, params: dict[str, Any]) -> None:
        """Log pipeline start with parameters."""
        self.logger.info("=" * 80)
        self.logger.info(f"PIPELINE START: {self.metrics['pipeline_name']}")
        self.logger.info(f"Execution Mode: {self.execution_mode}")
        self.logger.info(f"Parameters: {json.dumps(params, indent=2)}")
        self.logger.info(f"Start Time: {self.metrics['start_time']}")
        self.logger.info("=" * 80)
        
        # Log connection details
        self._log_connection_details()
    
    def _log_connection_details(self) -> None:
        """Log connection and environment details."""
        try:
            from risk_analytics.config import load_config
            config = load_config(self.execution_mode)
            
            self.logger.info("CONNECTION DETAILS:")
            self.logger.info(f"  Catalog: {config.get('catalog', {}).get('name', 'unknown')}")
            self.logger.info(f"  Catalog Type: {config.get('catalog', {}).get('type', 'unknown')}")
            self.logger.info(f"  Catalog URI: {config.get('catalog', {}).get('uri', 'N/A')}")
            self.logger.info(f"  Storage Type: {config.get('storage', {}).get('type', 'unknown')}")
            self.logger.info(f"  Storage Endpoint: {config.get('storage', {}).get('endpoint', 'N/A')}")
            self.logger.info(f"  Spark Mode: {config.get('spark_mode', 'unknown')}")
            self.logger.info(f"  Orchestration: {config.get('orchestration', 'unknown')}")
            
            # Log Spark settings if available
            spark_remote = os.getenv("SPARK_REMOTE")
            if spark_remote:
                self.logger.info(f"  Spark Remote: {spark_remote}")
            
        except Exception as e:
            self.logger.warning(f"Could not log connection details: {e}")
    
    def log_step_start(self, step_name: str, details: dict[str, Any] = None) -> None:
        """Log the start of a pipeline step."""
        self.step_start_time = time.time()
        step_info = {
            "step_name": step_name,
            "start_time": datetime.now().isoformat(),
            "details": details or {},
            "status": "started"
        }
        self.metrics["steps"].append(step_info)
        
        self.logger.info("-" * 60)
        self.logger.info(f"STEP START: {step_name}")
        if details:
            self.logger.info(f"Details: {json.dumps(details, indent=2)}")
        self.logger.info("-" * 60)
    
    def log_step_complete(self, step_name: str, records_processed: int = 0, duration_ms: float = None) -> None:
        """Log the completion of a pipeline step with metrics."""
        duration_ms = duration_ms or ((time.time() - self.step_start_time) * 1000)
        
        # Update step metrics
        for step in self.metrics["steps"]:
            if step["step_name"] == step_name:
                step["status"] = "completed"
                step["end_time"] = datetime.now().isoformat()
                step["duration_ms"] = duration_ms
                step["records_processed"] = records_processed
                break
        
        self.metrics["total_records_processed"] += records_processed
        
        self.logger.info("-" * 60)
        self.logger.info(f"STEP COMPLETE: {step_name}")
        self.logger.info(f"Records Processed: {records_processed:,}")
        self.logger.info(f"Duration: {duration_ms:.2f}ms ({duration_ms/1000:.2f}s)")
        self.logger.info("-" * 60)
    
    def log_step_error(self, step_name: str, error: Exception) -> None:
        """Log a step error."""
        duration_ms = (time.time() - self.step_start_time) * 1000
        
        error_info = {
            "step_name": step_name,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "timestamp": datetime.now().isoformat(),
            "duration_ms": duration_ms
        }
        
        self.metrics["errors"].append(error_info)
        
        # Update step status
        for step in self.metrics["steps"]:
            if step["step_name"] == step_name:
                step["status"] = "failed"
                step["end_time"] = datetime.now().isoformat()
                step["duration_ms"] = duration_ms
                step["error"] = str(error)
                break
        
        self.logger.error("-" * 60)
        self.logger.error(f"STEP FAILED: {step_name}")
        self.logger.error(f"Error Type: {type(error).__name__}")
        self.logger.error(f"Error Message: {str(error)}")
        self.logger.error(f"Duration: {duration_ms:.2f}ms")
        self.logger.error("-" * 60)
    
    def log_table_operation(self, operation: str, table_name: str, details: dict[str, Any] = None) -> None:
        """Log table-level operations with connection details."""
        self.logger.info(f"TABLE OPERATION: {operation} | Table: {table_name}")
        if details:
            self.logger.info(f"Details: {json.dumps(details, indent=2)}")
    
    def log_spark_operation(self, operation: str, details: dict[str, Any] = None) -> None:
        """Log Spark operations with configuration details."""
        self.logger.info(f"SPARK OPERATION: {operation}")
        if details:
            self.logger.info(f"Details: {json.dumps(details, indent=2)}")
    
    def log_pipeline_complete(self, success: bool = True) -> None:
        """Log pipeline completion with final metrics."""
        total_duration = time.time() - self.pipeline_start_time
        self.metrics["total_duration_seconds"] = total_duration
        self.metrics["end_time"] = datetime.now().isoformat()
        self.metrics["status"] = "completed" if success else "failed"
        
        self.logger.info("=" * 80)
        self.logger.info(f"PIPELINE {'COMPLETED' if success else 'FAILED'}: {self.metrics['pipeline_name']}")
        self.logger.info(f"Execution Mode: {self.execution_mode}")
        self.logger.info(f"Total Duration: {total_duration:.2f}s ({total_duration/60:.2f}min)")
        self.logger.info(f"Total Records Processed: {self.metrics['total_records_processed']:,}")
        self.logger.info(f"Steps Completed: {len([s for s in self.metrics['steps'] if s['status'] == 'completed'])}/{len(self.metrics['steps'])}")
        self.logger.info(f"Steps Failed: {len(self.metrics['errors'])}")
        
        if self.metrics["errors"]:
            self.logger.warning(f"Errors encountered: {len(self.metrics['errors'])}")
            for error in self.metrics["errors"]:
                self.logger.warning(f"  - {error['step_name']}: {error['error_type']}: {error['error_message']}")
        
        self.logger.info("=" * 80)
        
        # Write structured metrics to separate file for Splunk
        self._write_structured_metrics()
    
    def _write_structured_metrics(self) -> None:
        """Write structured metrics to JSON file for Splunk consumption."""
        metrics_file = LOG_FILE.parent / "pipeline_metrics.json"
        try:
            with metrics_file.open("w") as f:
                json.dump(self.metrics, f, indent=2, default=str)
            self.logger.info(f"Structured metrics written to: {metrics_file}")
        except Exception as e:
            self.logger.error(f"Failed to write structured metrics: {e}")