from __future__ import annotations

from dataclasses import dataclass, field
from types import ModuleType


@dataclass
class FakeSparkBuilder:
    app_name: str | None = None
    remote_url: str | None = None
    configs: list[tuple[str, str]] = field(default_factory=list)
    get_or_create_called: bool = False

    def appName(self, name: str) -> "FakeSparkBuilder":
        self.app_name = name
        return self

    def remote(self, url: str) -> "FakeSparkBuilder":
        self.remote_url = url
        return self

    def config(self, key: str, value: str) -> "FakeSparkBuilder":
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
