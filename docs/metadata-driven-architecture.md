# Metadata-Driven Architecture and YAML Patterns

> Quick links: [Overview](index.md) | [Architecture](architecture.md) | [Data Model](data-model-risk-metrics.md) | [Runbook](runbooks.md) | [Production Setup](production_setup.md) | [Testing](testing.md)

This platform uses declarative YAML pipelines to define transformation behavior while keeping execution logic in shared Python components.

## Why Metadata-Driven

- Business logic is configured in YAML rather than embedded in one-off scripts.
- Execution behavior is consistent across pipelines through a shared executor.
- Validation and preview can run before data writes.
- Runtime parameters (`as_of_date`, `risk_run_id`, `source_branch`) are injected safely.

## Execution Model

1. Load YAML definition.
2. Render runtime templates.
3. Validate required sections (`sources`, `steps`, `targets`).
4. Materialize named sources.
5. Execute each step via component dispatcher.
6. Emit named datasets for reuse.
7. Write target datasets with append/overwrite/merge behavior.

Key implementation files:

- `risk_analytics/yaml_executor.py`
- `risk_analytics/transformations/components.py`

## Supported Transformation Components

Based on `risk_analytics/transformations/components.py`, the executor supports:

- `join`
- `lookup`
- `rollup`
- `normalize`
- `dedup`
- `filter`
- `reformat`

## How to Read a YAML Pipeline

A typical pipeline has these sections:

- `sources`: where data enters (table/sql/config map/file).
- `steps`: ordered transformations.
- `targets`: write contracts.

The output of one step is referenced by name in later steps using `emit.output`.

## Complete Sample YAML

This sample includes all supported transformation types in one flow for reference.

```yaml
version: 1
name: sample_metadata_pipeline

sources:
  - name: deals_raw
    type: table
    table: nessie.risk_analytics_ods.deals
    filters:
      - as_of_date = '{{as_of_date}}'

  - name: customer_raw
    type: table
    table: nessie.risk_analytics_ods.customer

  - name: haircut_map
    type: config_map
    config_path: risk.collateral_haircuts
    key_column: asset_class
    value_column: haircut

steps:
  - id: dedup_deals
    type: dedup
    input: deals_raw
    keys: [deal_id, as_of_date]
    order_by:
      - column: ingestion_ts
        desc: true
    emit:
      output: deals_latest

  - id: normalize_columns
    type: normalize
    input: deals_latest
    columns:
      - name: customer_id
        operation: trim_upper
      - name: currency
        operation: trim_upper
    emit:
      output: deals_normalized

  - id: filter_active
    type: filter
    input: deals_normalized
    conditions:
      - status = 'ACTIVE'
    emit:
      output: deals_active

  - id: join_customer
    type: join
    input: deals_active
    joins:
      - dataset: customer_raw
        on: [customer_id]
        how: left
    emit:
      output: deals_customer

  - id: lookup_haircut
    type: lookup
    input: deals_customer
    lookup: haircut_map
    keys:
      - left: collateral_type
        right: asset_class
    values:
      - right: haircut
        name: haircut
    emit:
      output: deals_haircut

  - id: enrich_metrics
    type: reformat
    input: deals_haircut
    operations:
      - op: add_column
        name: deal_exposure
        expression:
          op: greatest
          args:
            - col: mark_to_market
            - lit: 0
      - op: add_column
        name: haircut_effective
        expression:
          op: coalesce
          args:
            - col: haircut
            - config: risk.collateral_haircuts.OTHER
    emit:
      output: deals_enriched

  - id: rollup_customer_netting
    type: rollup
    input: deals_enriched
    group_by: [customer_id, netting_set_id]
    aggregations:
      - name: gross_exposure
        op: sum
        col: deal_exposure
      - name: mtm_sum
        op: sum
        col: mark_to_market
    emit:
      output: customer_rollup

  - id: finalize
    type: reformat
    input: customer_rollup
    operations:
      - op: add_column
        name: netting_exposure
        expression:
          op: greatest
          args:
            - col: mtm_sum
            - lit: 0
    drop: [mtm_sum]
    emit:
      output: output_metrics

targets:
  - name: metrics_target
    dataset: output_metrics
    table: nessie.risk_analytics_ods.risk_metrics
    mode: append
```

## YAML Construction Guidelines

- Keep every step focused on one transformation intent.
- Prefer explicit `emit.output` names to keep lineage readable.
- Use `config_map` plus `lookup` for business constants and reference mappings.
- Keep row-level derivation in `reformat` operations and group math in `rollup`.
- Use runtime placeholders only where run-specific values are needed.

## Runtime Parameters and Templates

The executor resolves placeholders like:

- `{{as_of_date}}`
- `{{risk_run_id}}`
- `{{source_branch}}`

Execution fails early when placeholders remain unresolved.

## Validation and Preview Workflow

Before executing a pipeline in production-like runs:

1. Validate structure (`validate_pipeline_yaml`).
2. Preview rendered payload (`preview_pipeline_yaml`).
3. Execute with explicit runtime parameters (`run_pipeline_from_yaml`).

This keeps metadata quality high and reduces runtime failures.

## Real Pipeline Reference

See active risk pipeline definition:

- `transform/source_to_ods/risk_metrics_pipeline_source_to_ods.yaml`

It demonstrates production usage of `join`, `lookup`, `rollup`, and `reformat` with configuration-driven risk assumptions.
