"""Expression parser and selector helpers for YAML transformations."""
from __future__ import annotations

from functools import reduce
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from risk_analytics.transformations.common import ComponentExecutionError, config_value, required_str


def apply_select(df: DataFrame, select_spec: list[Any], config: dict[str, Any]) -> DataFrame:
    """Translate a YAML select list into Spark columns and named expressions."""
    select_columns = []
    for item in select_spec:
        if isinstance(item, str):
            select_columns.append(F.col(item))
        elif isinstance(item, dict):
            name = required_str(item, "name")
            expression = item.get("expression")
            if expression is None:
                raise ComponentExecutionError("Select item with object syntax requires 'expression'.")
            select_columns.append(build_expression(expression, config).alias(name))
        else:
            raise ComponentExecutionError("Select entries must be strings or objects.")
    return df.select(*select_columns)


def build_condition(spec: Any, config: dict[str, Any]):
    if isinstance(spec, str):
        return F.expr(spec)
    return build_expression(spec, config)


def build_expression(spec: Any, config: dict[str, Any]):
    """Compile the restricted YAML expression language into a Spark column.

    Expressions are constructed from a whitelist rather than evaluated as Python
    code. That preserves declarative authoring while keeping execution behavior
    explicit, testable, and safe for pipeline configuration.
    """
    if isinstance(spec, dict):
        if "expression" in spec:
            return build_expression(spec["expression"], config)
        if "col" in spec:
            return F.col(str(spec["col"]))
        if "lit" in spec:
            return F.lit(spec["lit"])
        if "config" in spec:
            return F.lit(config_value(config, str(spec["config"])))
        if "op" in spec:
            op = str(spec["op"]).lower()
            args = [build_expression(value, config) for value in spec.get("args", [])]
            return apply_expression_op(op, args, spec, config)
        raise ComponentExecutionError("Expression object must include one of: col, lit, config, op.")

    if isinstance(spec, (int, float, bool)):
        return F.lit(spec)

    if spec is None:
        return F.lit(None)

    if isinstance(spec, str):
        return F.col(spec)

    raise ComponentExecutionError(f"Unsupported expression type '{type(spec).__name__}'.")


def apply_expression_op(op: str, args: list[Any], raw_spec: dict[str, Any], config: dict[str, Any]):
    """Map a validated expression operator to its Spark equivalent."""
    if op == "trim":
        require_arg_count(op, args, 1)
        return F.trim(args[0])
    if op == "lpad":
        require_arg_count(op, args, 3)
        raw_args = raw_spec.get("args", [])
        return F.lpad(args[0], literal_int(raw_args[1], config), literal_str(raw_args[2], config))
    if op == "rpad":
        require_arg_count(op, args, 3)
        raw_args = raw_spec.get("args", [])
        return F.rpad(args[0], literal_int(raw_args[1], config), literal_str(raw_args[2], config))
    if op == "concat":
        return F.concat(*args)
    if op == "upper":
        require_arg_count(op, args, 1)
        return F.upper(args[0])
    if op == "lower":
        require_arg_count(op, args, 1)
        return F.lower(args[0])
    if op == "coalesce":
        return F.coalesce(*args)
    if op == "cast":
        require_arg_count(op, args, 1)
        data_type = raw_spec.get("type")
        if not isinstance(data_type, str):
            raise ComponentExecutionError("Expression op 'cast' requires string field 'type'.")
        return args[0].cast(data_type)
    if op == "greatest":
        return F.greatest(*args)
    if op == "least":
        return F.least(*args)
    if op == "add":
        return reduce_binary(args, lambda left, right: left + right, op)
    if op == "sub":
        return reduce_binary(args, lambda left, right: left - right, op)
    if op == "mul":
        return reduce_binary(args, lambda left, right: left * right, op)
    if op == "div":
        return reduce_binary(args, lambda left, right: left / right, op)
    if op == "eq":
        require_arg_count(op, args, 2)
        return args[0] == args[1]
    if op == "ne":
        require_arg_count(op, args, 2)
        return args[0] != args[1]
    if op == "gt":
        require_arg_count(op, args, 2)
        return args[0] > args[1]
    if op == "gte":
        require_arg_count(op, args, 2)
        return args[0] >= args[1]
    if op == "lt":
        require_arg_count(op, args, 2)
        return args[0] < args[1]
    if op == "lte":
        require_arg_count(op, args, 2)
        return args[0] <= args[1]
    if op == "and":
        return reduce_binary(args, lambda left, right: left & right, op)
    if op == "or":
        return reduce_binary(args, lambda left, right: left | right, op)
    if op == "not":
        require_arg_count(op, args, 1)
        return ~args[0]
    if op == "when":
        # A three-part conditional avoids ambiguous null/default behavior in YAML.
        if len(args) != 3:
            raise ComponentExecutionError("Expression op 'when' requires exactly 3 args: condition, true_value, false_value.")
        return F.when(args[0], args[1]).otherwise(args[2])
    if op == "current_timestamp":
        require_arg_count(op, args, 0)
        return F.current_timestamp()
    if op == "map_from_json":
        require_arg_count(op, args, 1)
        return F.to_json(args[0]).cast("map<string,string>")

    raise ComponentExecutionError(f"Unsupported expression op '{op}'.")


def reduce_binary(args: list[Any], operator, op: str):
    if len(args) < 2:
        raise ComponentExecutionError(f"Expression op '{op}' requires at least 2 args.")
    return reduce(operator, args)


def require_arg_count(op: str, args: list[Any], size: int) -> None:
    if len(args) != size:
        raise ComponentExecutionError(f"Expression op '{op}' requires exactly {size} args.")


def literal_int(raw_value: Any, config: dict[str, Any]) -> int:
    value = raw_literal_value(raw_value, config)
    try:
        return int(value)
    except (TypeError, ValueError) as error:
        raise ComponentExecutionError(f"Expected integer literal, got '{value}'.") from error


def literal_str(raw_value: Any, config: dict[str, Any]) -> str:
    value = raw_literal_value(raw_value, config)
    return str(value)


def raw_literal_value(raw_value: Any, config: dict[str, Any]) -> Any:
    if isinstance(raw_value, dict):
        if "lit" in raw_value:
            return raw_value["lit"]
        if "config" in raw_value:
            return config_value(config, str(raw_value["config"]))
    return raw_value
