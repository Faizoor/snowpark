<<<<<<< HEAD
# Snowpark Data Quality Testing Framework

This lightweight Python framework lets you define and execute **sanity** and
**functional** data‑quality checks against Snowflake using the Snowpark API.
It's configuration driven (YAML) and produces result reports in CSV format.

---

## 🔧 Features

- Sanity checks: metadata validations such as table/stage existence
- Functional checks: data validations (nulls, duplicates, etc.)
- Extensible registry for adding new rule types
- Uses Snowpark DataFrame API -- no raw SQL required
- Configurable via a single YAML file
- Outputs results to `test_result.csv` (can be customized)

## 📁 Project Structure

```
Framework/
  ├─ config.yaml         # sample configuration file
  ├─ main.py             # entry point and orchestration logic
  ├─ readme.md           # this document
  └─ lib/
      └─ checks.py       # SQL/DF generation for each rule type
```

## 🧠 How It Works

1. **Load configuration** — `main.py` reads YAML specifying rules.
2. **Sanity checks** — verify existence of tables/stages 
3. **Functional checks** — run data quality rules on specified tables/columns
4. **Collect results** — each rule returns PASS/FAIL/ERROR and messages
5. **Export output** — results written to CSV for auditing

## ⚙️ YAML Configuration

A minimal config example:

```yaml
application: Maximo
database: UDX_CORE
schema: Conform

sanity_checks:
  rules:
    - rule_id: S01
      name: Collection Table Exists
      rule_type: table_exists
      object: UUDX_CORE.STAGE.TABLE_THAT_DOES_NOT_EXIST
      object_type: table

functional_checks:
  rules:
    - rule_id: F01
      name: Null check on key fields
      rule_type: null_check
      threshold: 0
      object:
        - name: UDX_CORE.STAGE.STG_DATA
          columns: [object_id, source_id]
      object_type: table
```

### Rule Fields

- `rule_id` _(string)_ – unique identifier
- `name` _(string)_ – human‑readable description
- `rule_type` _function_name_ – key in `checks.sanity_registry`
- `object` – fully qualified name of the object
- `object_type` _(table|stage|schema)_ – currently informational
- `threshold` _(int)_ – for functional rules, expected failure count

## 📥 Running the Framework

1. Activate your Python virtualenv with Snowpark installed.
2. Set Snowflake credentials via environment variables using .env file:
   `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`,
   `SNOWFLAKE_ROLE`, etc.
3. Execute:


Summaries are logged to the console and results are saved to `test_result.csv`.

## 📊 Output Format

The CSV includes the following columns:
`category,rule_id,name,object,rule_type,status,object_type,error_message`

STATUS values: `PASS`, `FAIL`, `ERROR`.

## ✅ Extending the Framework

- Add new check logic in `lib/checks.py`
- Register the new function in `sanity_registry` dictionary
- Update YAML to reference the new `rule_type`

You can implement DataFrame‑based checks by accepting a
`session` argument and returning a Snowpark `DataFrame` with a
`result` column.


This framework is intentionally simple and modular. Feel free to adapt it
for your organization's data‑quality needs!  

---
=======
2026-02-26 15:52:54,741 | INFO | Sanity checks summary: {'total': 3, 'passed': 2, 'failed': 0, 'error': 1}
2026-02-26 15:52:54,741 | INFO | Functional checks summary: {'total': 2, 'passed': 0, 'failed': 1, 'error': 1}

2026-02-26 16:41:23,591 | INFO | Sanity checks summary: {'total': 3, 'passed': 2, 'failed': 0, 'error': 1}
2026-02-26 16:41:23,591 | INFO | Functional checks summary: {'total': 2, 'passed': 0, 'failed': 1, 'error': 1}


add time stamp to file name
>>>>>>> Feature
