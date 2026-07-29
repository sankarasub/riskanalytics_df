from __future__ import annotations

from dataclasses import dataclass, field
from types import ModuleType


@dataclass
class FakeSparkBuilder:
    app_name: str | None = None
    remote_url: str | None = None
    configs: list[tuple[str, str]] = field(default_factory=list)
    get_or_create_called: bool = False

    def appName(self, name: str) -> FakeSparkBuilder:
        self.app_name = name
        return self

    def remote(self, url: str) -> FakeSparkBuilder:
        self.remote_url = url
        return self

    def config(self, key: str, value: str) -> FakeSparkBuilder:
        self.configs.append((key, value))
        return self

    def getOrCreate(self) -> dict[str, object]:
        self.get_or_create_called = True
        return {
            "app_name": self.app_name,
            "remote_url": self.remote_url,
            "configs": list(self.configs),
        }


class _FakeType:
    def __init__(self, *args, **kwargs) -> None:
        self.args = args
        self.kwargs = kwargs


class _FakeField(_FakeType):
    pass


class _FakeStructType(_FakeType):
    pass


def build_fake_pyspark() -> tuple[dict[str, ModuleType], FakeSparkBuilder]:
    builder = FakeSparkBuilder()

    spark_session_module = type("FakeSparkSession", (), {"builder": builder})
    window_module = type("FakeWindow", (), {"partitionBy": staticmethod(lambda *args, **kwargs: None)})
    sql_module = ModuleType("pyspark.sql")
    sql_module.SparkSession = spark_session_module
    sql_module.Window = window_module
    sql_module.DataFrame = type("FakeDataFrame", (), {})
    functions_module = ModuleType("pyspark.sql.functions")
    functions_module.col = staticmethod(lambda name: ("col", name))
    functions_module.lit = staticmethod(lambda value: ("lit", value))
    functions_module.trim = staticmethod(lambda value: ("trim", value))
    functions_module.lpad = staticmethod(lambda value, length, pad: ("lpad", value, length, pad))
    functions_module.rpad = staticmethod(lambda value, length, pad: ("rpad", value, length, pad))
    functions_module.concat = staticmethod(lambda *values: ("concat", values))
    functions_module.upper = staticmethod(lambda value: ("upper", value))
    functions_module.lower = staticmethod(lambda value: ("lower", value))
    functions_module.coalesce = staticmethod(lambda *values: ("coalesce", values))
    functions_module.to_json = staticmethod(lambda value: type("FakeExpression", (), {"cast": lambda self, _dtype: ("to_json_cast", value)})())
    functions_module.greatest = staticmethod(lambda *values: ("greatest", values))
    functions_module.least = staticmethod(lambda *values: ("least", values))
    functions_module.current_timestamp = staticmethod(lambda: ("current_timestamp",))
    functions_module.expr = staticmethod(lambda clause: ("expr", clause))
    sql_module.functions = functions_module

    types_module = ModuleType("pyspark.sql.types")
    types_module.BooleanType = type("BooleanType", (_FakeType,), {})
    types_module.DateType = type("DateType", (_FakeType,), {})
    types_module.DecimalType = type("DecimalType", (_FakeType,), {})
    types_module.MapType = type("MapType", (_FakeType,), {})
    types_module.StringType = type("StringType", (_FakeType,), {})
    types_module.StructField = type("StructField", (_FakeField,), {})
    types_module.StructType = type("StructType", (_FakeStructType,), {})
    sql_module.types = types_module

    pyspark_module = ModuleType("pyspark")
    pyspark_module.sql = sql_module

    return {"pyspark": pyspark_module, "pyspark.sql": sql_module, "pyspark.sql.types": types_module}, builder


class FakeTask:
    """Minimal Airflow operator stand-in that records dependency wiring."""

    def __init__(self, **kwargs) -> None:
        self.task_id = kwargs.get("task_id", "")
        self.kwargs = kwargs
        self.upstream: list[FakeTask] = []
        self.downstream: list[FakeTask] = []
        dag = _ACTIVE_DAGS[-1] if _ACTIVE_DAGS else None
        if dag is not None:
            dag.tasks[self.task_id] = self
        group = _ACTIVE_GROUPS[-1] if _ACTIVE_GROUPS else None
        if group is not None:
            group.tasks.append(self)

    def set_downstream(self, other: FakeTask | FakeTaskGroup) -> FakeTask | FakeTaskGroup:
        if isinstance(other, FakeTaskGroup):
            other.set_upstream(self)
            return other
        self.downstream.append(other)
        other.upstream.append(self)
        return other

    def set_upstream(self, other: FakeTask | FakeTaskGroup) -> FakeTask | FakeTaskGroup:
        other.set_downstream(self)
        return other

    def __rshift__(self, other: FakeTask | FakeTaskGroup) -> FakeTask | FakeTaskGroup:
        return self.set_downstream(other)

    def __lshift__(self, other: FakeTask | FakeTaskGroup) -> FakeTask | FakeTaskGroup:
        return self.set_upstream(other)


class FakeTaskGroup:
    """Task group stand-in that fans dependencies out to its roots and leaves."""

    def __init__(self, **kwargs) -> None:
        self.group_id = kwargs.get("group_id", "")
        self.kwargs = kwargs
        self.tasks: list[FakeTask] = []

    def __enter__(self) -> FakeTaskGroup:
        _ACTIVE_GROUPS.append(self)
        return self

    def __exit__(self, *_exc_info) -> bool:
        _ACTIVE_GROUPS.pop()
        return False

    @property
    def roots(self) -> list[FakeTask]:
        return [task for task in self.tasks if not any(up in self.tasks for up in task.upstream)]

    @property
    def leaves(self) -> list[FakeTask]:
        return [task for task in self.tasks if not any(down in self.tasks for down in task.downstream)]

    def set_downstream(self, other: FakeTask) -> FakeTask:
        for leaf in self.leaves:
            leaf.set_downstream(other)
        return other

    def set_upstream(self, other: FakeTask) -> FakeTask:
        for root in self.roots:
            other.set_downstream(root)
        return other

    def __rshift__(self, other: FakeTask) -> FakeTask:
        return self.set_downstream(other)

    def __lshift__(self, other: FakeTask) -> FakeTask:
        return self.set_upstream(other)


class FakeDag:
    """Captures a DAG definition so tests can assert ids and dependencies."""

    def __init__(self, **kwargs) -> None:
        self.dag_id = kwargs.get("dag_id", "")
        self.kwargs = kwargs
        self.tasks: dict[str, FakeTask] = {}

    def __enter__(self) -> FakeDag:
        _ACTIVE_DAGS.append(self)
        _COLLECTED_DAGS[self.dag_id] = self
        return self

    def __exit__(self, *_exc_info) -> bool:
        _ACTIVE_DAGS.pop()
        return False


_ACTIVE_DAGS: list[FakeDag] = []
_ACTIVE_GROUPS: list[FakeTaskGroup] = []
_COLLECTED_DAGS: dict[str, FakeDag] = {}


def build_fake_airflow() -> tuple[dict[str, ModuleType], dict[str, FakeDag]]:
    """Provide importable Airflow stubs plus the registry DAG files write into."""
    _ACTIVE_DAGS.clear()
    _ACTIVE_GROUPS.clear()
    _COLLECTED_DAGS.clear()

    modules: dict[str, ModuleType] = {}

    airflow_module = ModuleType("airflow")
    airflow_module.DAG = FakeDag
    modules["airflow"] = airflow_module

    task_group_module = ModuleType("airflow.utils.task_group")
    task_group_module.TaskGroup = FakeTaskGroup
    modules["airflow.utils.task_group"] = task_group_module

    for path, names in {
        "airflow.operators.bash": ("BashOperator",),
        "airflow.operators.empty": ("EmptyOperator",),
        "airflow.operators.python": ("PythonOperator", "ShortCircuitOperator"),
        "airflow.operators.trigger_dagrun": ("TriggerDagRunOperator",),
        "airflow.providers.apache.kafka.sensors.kafka": ("AwaitMessageSensor",),
    }.items():
        module = ModuleType(path)
        for name in names:
            setattr(module, name, type(name, (FakeTask,), {}))
        modules[path] = module

    # Intermediate packages so `import a.b.c` style imports resolve.
    for package in (
        "airflow.operators",
        "airflow.providers",
        "airflow.providers.apache",
        "airflow.providers.apache.kafka",
        "airflow.providers.apache.kafka.sensors",
        "airflow.utils",
    ):
        modules.setdefault(package, ModuleType(package))

    return modules, _COLLECTED_DAGS
