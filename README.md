# Elastic Logs Validator

A lightweight Python validation tool for Elasticsearch data streams. It enforces stream health across three core pillars: **Recency / Staleness**, **Field Type / Schema Mapping**, and **Volume / Throughput**.

---

## Key Features

* **Age Validation**: Ensures data streams receive logs within a configured staleness window (e.g., 24h, 168h).
* **Schema / Type Validation**: Validates field dynamic mapping against target schema specs to catch mapping collisions or unexpected types (`keyword` vs `float`, etc.).
* **Volume Monitoring**: Enforces minimum document counts over a 7-day window. Supports global defaults and wildcard pattern matching per stream (`min_weekly_docs`).
* **Aligned Tabular Logging**: Fixed-width log outputs for easy scanning in CI/CD pipelines or console outputs.
* **HTML Report Generation**: Generates standalone HTML summary reports after execution.

---

## Installation

Requires **Python 3.10+** and [Hatch](https://hatch.pypa.io/latest/)

```bash
git clone [https://github.com/City-of-Helsinki/elastic-logs-validator](https://github.com/City-of-Helsinki/elastic-logs-validator)
cd elastic-logs-validator

# Environment setup and dependencies are handled automatically by Hatch
hatch env create
```

---

## Configuration (`config.json`)

The validator is driven by a JSON configuration file validated against `datastream_config_schema.json`.

### Config Example

```json
{
  "version": 1,
  "default_stale": 168.0,
  "default_min_weekly_docs": 24,
  "streams": [
    {
      "stream": "explicit-datastream-name",
      "stale": 24.0,
      "min_weekly_docs": 100
    },
    {
      "stream": "*-dev-v1",
      "stale": 168.0
    }
  ]
}
```

### Threshold Resolution Logic

1. **Wildcard Stream Matching**: Concrete stream names (e.g., `explicit-datastream-name`) are evaluated against patterns defined in `streams[]` using `fnmatch`.
2. **Stream Overrides**: If a matching stream block defines `stale` or `min_weekly_docs`, that value takes precedence.
3. **Global Fallbacks**: Omitted stream properties fall back to `default_stale` and `default_min_weekly_docs`. If `min_weekly_docs` is not defined at either level, volume checking is safely skipped for that stream.

---


## Environment Variables
Credentials are read from shell environment variables or a `.env` file (copy `.env.sample` to `.env` to configure locally):
```
bash cp .env.sample .env
```

| Variable | Description
|---|---|
`ES_HOST` | Target Elasticsearch cluster URL (e.g., https://localhost:9200).
`ES_API_KEY` | Base64-encoded API key for Elasticsearch authentication.

## CLI Usage

Run the main validation entrypoint.

```bash
hatch run start --generate-report html
```

### Options

| Flag | Required | Description |
|---|---|---|
| `--generate-report` | No | External report format (`html`, `json`).  |
| `--output-dir` | No | Output directory for reports (default: `./reports`). |

---

## Output Format Example

### Terminal Console Output

```text
2026-08-25 12:00:01 [INFO   ] --- Age Validation Summary ---
2026-08-25 12:00:01 [WARNING] Age validation failed: 2 / 14 streams stale/empty
2026-08-25 12:00:01 [WARNING]   └─ STALE:   datastream-name-here-1           | Age: 286.7h   | Threshold: 168.0h  | Last Seen: 2026-08-12T11:41:53Z
2026-08-25 12:00:01 [ERROR  ]   └─ EMPTY:   datastream-name-here-2           | Age: N/A      | Threshold: N/A     | Last Seen: NEVER (0 docs)

2026-08-25 12:00:02 [INFO   ] --- Type Mapping Validation Summary ---
2026-08-25 12:00:02 [INFO   ] Total: 14 | Valid: 14 | Invalid: 0 | Unmapped: 0 | Skipped: 0 | Violations: 0
2026-08-25 12:00:02 [INFO   ] Type validation passed: All 14 mapped streams matched target field types

2026-08-25 12:00:03 [INFO   ] --- Volume Validation Summary ---
2026-08-25 12:00:03 [WARNING] Volume validation failed: 5 / 13 streams below threshold
2026-08-25 12:00:03 [INFO   ]   └─ PASS: datastream-name-here-1               | Count: 140    | Target: 24     | Surplus: +116
2026-08-25 12:00:03 [WARNING]   └─ FAIL: datastream-name-here-2               | Count: 0      | Target: 50     | Deficit: -50
2026-08-25 12:00:03 [WARNING]   └─ FAIL: datastream-name-here-3               | Count: 0      | Target: 50     | Deficit: -50

2026-08-25 12:00:03 [INFO   ] Report successfully written to: ./reports/stream_review_20260825_120003.html
2026-08-25 12:00:03 [ERROR  ] Validation failed.
```

---

## Exit Codes

Designed for integration into continuous monitoring jobs and CI pipelines:

* **`0`**: All streams passed age, schema mapping, and volume checks.
* **`1`**: One or more streams failed validation (stale streams, empty streams, field type mismatches, or volume deficits below target).
