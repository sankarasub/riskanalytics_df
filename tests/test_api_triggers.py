from __future__ import annotations

import importlib
import os
import sys
import types
import unittest
from unittest.mock import patch

from tests.support import build_fake_pyspark


class ApiTriggerTests(unittest.TestCase):
    def _import_module(self):
        fake_modules, _builder = build_fake_pyspark()

        fake_fastapi = types.ModuleType("fastapi")

        class FakeHTTPException(Exception):
            def __init__(self, status_code: int, detail: str):
                super().__init__(detail)
                self.status_code = status_code
                self.detail = detail

        class FakeFastAPI:
            def __init__(self, *args, **kwargs):
                self.args = args
                self.kwargs = kwargs

            def get(self, *args, **kwargs):
                def decorator(fn):
                    return fn

                return decorator

            def post(self, *args, **kwargs):
                def decorator(fn):
                    return fn

                return decorator

            def put(self, *args, **kwargs):
                def decorator(fn):
                    return fn

                return decorator

        fake_fastapi.FastAPI = FakeFastAPI
        fake_fastapi.HTTPException = FakeHTTPException

        fake_responses = types.ModuleType("fastapi.responses")

        class FakeHTMLResponse(str):
            pass

        fake_responses.HTMLResponse = FakeHTMLResponse

        fake_pydantic = types.ModuleType("pydantic")

        def Field(default=None, **kwargs):
            if "default_factory" in kwargs and callable(kwargs["default_factory"]):
                return kwargs["default_factory"]()
            if default is ...:
                return None
            return default

        class BaseModel:
            def __init__(self, **kwargs):
                annotations = getattr(self.__class__, "__annotations__", {})
                for key in annotations:
                    if key in kwargs:
                        setattr(self, key, kwargs[key])
                    elif hasattr(self.__class__, key):
                        setattr(self, key, getattr(self.__class__, key))

        fake_pydantic.BaseModel = BaseModel
        fake_pydantic.Field = Field

        sys.modules.pop("api.app", None)
        # Avoid optional dependency import errors during module initialization.
        os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "")

        with patch.dict(
            sys.modules,
            {
                **fake_modules,
                "fastapi": fake_fastapi,
                "fastapi.responses": fake_responses,
                "pydantic": fake_pydantic,
            },
            clear=False,
        ):
            module = importlib.import_module("api.app")
        return module

    def test_trigger_source_to_ods_routes_to_expected_dag(self) -> None:
        module = self._import_module()
        request = module.SourceToOdsTriggerRequest(
            mode="stage",
            entity="customer",
            source="sourcea",
            as_of_date="2026-07-18",
        )

        with patch.object(module, "_trigger_airflow_dag", return_value={"dag_run_id": "manual__1"}) as trigger_mock:
            response = module.trigger_source_to_ods(request)

        self.assertEqual(response["status"], "triggered")
        self.assertEqual(response["dag_id"], "ra_sourceA_customer_stage")
        trigger_mock.assert_called_once()

    def test_pipeline_execute_routes_each_target_to_its_dag(self) -> None:
        module = self._import_module()
        expectations = {
            "bootstrap": "ra_createtables_and_data",
            "orchestration": "ra_stage_to_ods_orchestration",
            "riskmetrics": "ra_riskmetrics_eval_ods",
            "stage": "ra_sourceB_deals_stage",
            "ods": "ra_sourceB_deals_ods",
        }

        for target, expected_dag_id in expectations.items():
            request = module.PipelineExecuteRequest(
                target=target,
                entity="deals",
                source="sourceb",
                as_of_date="2026-07-18",
            )
            with patch.object(module, "_trigger_airflow_dag", return_value={"dag_run_id": "manual__3"}):
                response = module.execute_pipeline_run(request)

            self.assertEqual(response["status"], "triggered")
            self.assertEqual(response["dag_id"], expected_dag_id)

    def test_pipeline_execute_rejects_unknown_target(self) -> None:
        module = self._import_module()
        request = module.PipelineExecuteRequest(target="nonsense")

        with self.assertRaises(Exception) as raised:
            module.execute_pipeline_run(request)

        self.assertEqual(raised.exception.status_code, 400)

    def test_publish_kafka_event_with_pipeline_trigger(self) -> None:
        module = self._import_module()

        produced: list[tuple[str, bytes]] = []

        class FakeProducer:
            def __init__(self, _config):
                self._config = _config

            def produce(self, topic, value):
                produced.append((topic, value))

            def flush(self, _timeout):
                return 0

        fake_kafka = types.ModuleType("confluent_kafka")
        fake_kafka.Producer = FakeProducer

        request = module.KafkaPublishRequest(
            entity="deals",
            payload={"deal_id": "D001", "status": "ACTIVE"},
            trigger_pipeline=True,
            as_of_date="2026-07-18",
        )

        with (
            patch.dict(sys.modules, {"confluent_kafka": fake_kafka}, clear=False),
            patch.object(module, "_trigger_airflow_dag", return_value={"dag_run_id": "manual__2"}) as trigger_mock,
        ):
            response = module.publish_kafka_event(request)

        self.assertEqual(response["status"], "published")
        self.assertEqual(response["topic"], "risk.deals.ingest")
        self.assertTrue(response["pipeline_triggered"])
        self.assertEqual(response["pipeline_dag_run_id"], "manual__2")
        self.assertEqual(len(produced), 1)
        trigger_mock.assert_called_once_with(
            "ra_riskmetrics_eval_ods",
            {"as_of_date": "2026-07-18"},
        )


if __name__ == "__main__":
    unittest.main()
