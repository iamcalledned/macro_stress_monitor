# Macro Stress Monitor

## Overview

Macro Stress Monitor is the foundational market data and state engine for a broader market intelligence system. It runs structural and intraday jobs, gathers practical macro and cross-asset context, computes deterministic stress/state summaries, and stores Redis snapshots for downstream consumers.

This layer is intended to be boring, durable, and auditable. Future layers such as Market Sniffer, dashboards, advisor agents, portfolio logic, and alerts should consume this layer's stored outputs rather than adding their own ad hoc data-fetch or scoring logic.

Core responsibilities:

- fetch FRED and Polygon market data
- compute structural macro, rates, credit, equity, volatility, breadth, and cross-asset context
- compute intraday preview signals from market proxies
- preserve latest state, history, run-scoped snapshots, and health in Redis
- expose stable reader helpers and lightweight API endpoints
- keep enough source, input, config, freshness, and version metadata to audit each run

## Data Contract

Run-scoped snapshots stored in Redis are the authoritative contract of this system.

Downstream components such as dashboards, agents, alerting, or portfolio logic must consume these snapshots rather than re-fetching or recomputing source data.

This ensures:

- deterministic behavior
- consistent scoring across consumers
- reproducibility of historical states

## Truth Hierarchy

The system maintains a clear separation of concerns:

1. Source data: FRED and Polygon/Massive inputs.
2. Indicator inputs: normalized numeric signals.
3. Scoring outputs: subscores, regime, and composite score.
4. Market context: macro/rates, credit, equities, sectors, volatility, breadth, relationships, and stretch proxies.
5. Snapshot metadata: health, freshness, config, deltas, and versioning.

Higher layers should not bypass lower layers. Do not compute indicators directly from source data in downstream systems; consume stored snapshot outputs through Redis, reader services, or API endpoints.

## Architecture / Role In Larger System

The monitor is designed as a state-producing engine:

1. Data source modules fetch external inputs.
2. Indicator modules convert raw time series into compact signal dictionaries.
3. Scoring modules convert indicators into subscores and composite regime state.
4. Market-context services build a broad twice-daily market-state package.
5. Job modules orchestrate structural and preview updates.
6. Redis stores latest state, history, run snapshots, and health.
7. Reader/health/delta services provide stable read and comparison helpers.
8. **Interpretation Layer ("Brain Stem")**: Deterministically builds briefs, comparisons, alerts, and prioritizations from the stored state.
9. The Flask app serves dashboard/API consumers with normalized DOM bindings and interpretation payloads.

Redis snapshots are the durable handoff between this foundation and every higher layer.

## Deterministic Interpretation Layer ("Brain Stem")

The system now includes an intermediate interpretation layer situated between the raw data foundation and the future "brain" (LLM) layer. 

**What it does:**
- Identifies and ranks dominant market factors, top risks, and supports (`prioritization.py`).
- Deterministically compares intraday preview signals against structural regimes to highlight divergence (`comparisons.py`).
- Generates factual, context-aware headlines and current-state briefs without hallucination (`briefs.py`).
- Flags critical data quality gaps and stale data caveats (`caveats.py`).
- Surfaces watch items based on cross-asset relationships (`alerts.py`).

**What it does NOT do:**
- It does not fetch live data. It relies purely on the stored structural and preview snapshots.
- It is not an LLM. All text is deterministically generated based on strict rules and state-matching.
- It does not invent facts or narrative.

The outputs of this layer are available natively in the UI's "The Brief" panel, and programmatically via `/api/brief/current`, `/api/brief/compare`, and `/api/brief/alerts`.

## Update Modes

Run commands from the parent directory:

```bash
cd /home/ned/Documents/market_monitor_bot
```

### Structural Full Update

Entrypoint:

```bash
/home/ned/venv/bin/python -m macro_stress_monitor.jobs.update_full
```

Purpose:

- fetches full FRED and market source set
- computes existing structural indicators and composite score
- builds the expanded `market_context` package
- updates legacy latest/history keys
- writes a canonical run-scoped structural snapshot
- evaluates alert rules
- writes system health

Primary Redis outputs:

- `msm:latest`
- `msm:history`
- `msm:latest_run_id`
- `msm:latest_structural_run_id`
- `msm:run:<run_id>:snapshot`
- `msm:health`

### Intraday Preview Update

Entrypoint:

```bash
/home/ned/venv/bin/python -m macro_stress_monitor.jobs.update_preview
```

Purpose:

- fetches market proxy tickers only
- computes preview component states for loans, financials, JPY, and regional banks
- writes a canonical run-scoped preview snapshot
- records session context
- computes preview delta versus the previous preview run
- writes system health

Primary Redis outputs:

- `msm:latest_preview_run_id`
- `msm:run:<run_id>:preview_snapshot`
- `msm:health`

### Legacy Entrypoint

```bash
/home/ned/venv/bin/python -m macro_stress_monitor.jobs.update
```

This remains a compatibility entrypoint and currently defaults to the structural full update.

## Redis Key Schema

Existing keys remain in place for backward compatibility:

- `msm:latest`: latest structural data object used by legacy readers
- `msm:history`: structural history list, newest first
- `msm:latest_run_id`: legacy latest structural run pointer
- `msm:latest_structural_run_id`: latest structural run pointer
- `msm:run:<run_id>:snapshot`: canonical structural run-scoped snapshot
- `msm:latest_preview_run_id`: latest preview run pointer
- `msm:run:<run_id>:preview_snapshot`: canonical preview run-scoped snapshot
- `msm:alerts:last`: latest alert state
- `msm:health`: compact system health record

No Redis migration is required. The hardened schema expands run-scoped snapshots by adding fields; it does not remove established keys.

### Latest Vs Run Snapshots

- `msm:latest` is a convenience and legacy key for quick structural reads.
- `msm:run:<run_id>:snapshot` is the canonical, fully-auditable structural record.
- `msm:run:<run_id>:preview_snapshot` is the canonical, fully-auditable preview record.

All new consumers should prefer run-scoped snapshots.

## Structural Market Context

Each structural run stores a broad market-state package under:

```json
{
  "market_context": {
    "macro_rates": {},
    "credit_liquidity": {},
    "equity_index_state": {},
    "sector_state": {},
    "volatility_stress": {},
    "flight_to_safety": {},
    "cross_asset_relationships": {},
    "breadth_participation": {},
    "positioning_stretch": {}
  }
}
```

The context is built by `macro_stress_monitor/services/market_context.py` from already fetched FRED and Polygon data. It stores compact summaries, not raw provider payloads.

### Macro / Rates / Curve

Stored under `market_context.macro_rates`.

Includes, where available:

- 2Y, 5Y, 10Y, and 30Y Treasury yields from FRED
- curve spreads: `2s10s`, `5s30s`, `10s30s`
- 5d, 20d, and 60d basis-point changes
- 1-year z-score and percentile
- deterministic state labels such as `falling_fast`, `rising_fast`, `range_bound`, `inverted`, `flat`, or `steep`
- 10Y breakeven inflation from FRED
- unemployment rate and nonfarm payroll level context from FRED
- real-rate proxy: 10Y nominal yield minus 10Y breakeven inflation
- dollar proxy: UUP ETF state

### Credit / Liquidity

Stored under `market_context.credit_liquidity`.

Includes:

- IG OAS from FRED where available
- HY OAS from FRED where available
- HY minus IG gap
- BKLN leveraged-loan proxy
- HYG, LQD, and BKLN ETF states
- HYG/LQD, HYG/IEF, LQD/IEF, and BKLN/SPY relationship states

### Equity Market State

Stored under `market_context.equity_index_state`.

Tracked baseline assets:

- SPY
- QQQ
- IWM
- DIA

For each available asset:

- latest price
- 1d, 5d, 20d, and 60d returns
- drawdown from rolling 252-day high
- distance from 50DMA and 200DMA
- 20d realized volatility
- 1-year realized-volatility percentile
- 1-year z-score
- 14d RSI
- trend, volatility, and stretch state labels

### Sector / Industry Leadership

Stored under `market_context.sector_state`.

Tracked sector and industry ETFs:

- XLF
- KRE
- XLK
- XLI
- XLE
- XLU
- XLP
- XLY
- XLV
- SMH

For each available sector:

- absolute asset state
- relative state versus SPY
- 5d, 20d, and 60d relative return snapshots
- moving-average gaps on the relative ratio
- z-score on the relative ratio
- leadership and laggard flags

### Volatility / Stress

Stored under `market_context.volatility_stress`.

Includes:

- VIXY ETF as a practical VIX futures proxy
- TLT realized volatility as a practical bond-volatility/MOVE proxy
- realized-volatility states for SPY, QQQ, TLT, and USDJPY
- stress flags for equity vol, bond vol, FX vol, and VIXY uptrend behavior

Direct VIX and MOVE index levels are not required by the current provider model. This section is explicitly proxy-based.

### Flight To Safety / Defensive Rotation

Stored under `market_context.flight_to_safety`.

Includes:

- Treasury proxies: TLT, IEF, SHY
- gold proxy: GLD
- dollar proxy: UUP
- JPY proxy: USDJPY behavior
- defensive versus cyclical sector relationships such as XLU/XLY and XLP/XLY
- TLT/SPY, GLD/SPY, and UUP/SPY relationships
- defensive-versus-cyclical rotation summary from tracked sector ETFs

### Cross-Asset / Relative Relationships

Stored under `market_context.cross_asset_relationships`.

Relationships:

- XLF/SPY
- KRE/SPY
- HYG/LQD
- HYG/IEF
- IWM/SPY
- QQQ/SPY
- SMH/SPY
- XLU/XLY
- XLP/XLY
- GLD/TLT
- RSP/SPY

For each available relationship:

- latest ratio
- 5d, 20d, and 60d ratio returns
- distance from 50DMA and 200DMA
- 1-year z-score
- relative state label
- stretch state label

### Breadth / Participation

Stored under `market_context.breadth_participation`.

This is proxy-based breadth, not exchange-level breadth.

Includes:

- tracked ETF basket count
- count and percentage above 50DMA
- count and percentage above 200DMA
- count and percentage with positive 20d trend
- 1d and 5d advancer counts across the tracked basket
- sector count above 50DMA and 200DMA
- RSP/SPY equal-weight versus cap-weight proxy

### Positioning / Stretch Proxies

Stored under `market_context.positioning_stretch`.

This is deterministic price/volatility stretch, not institutional positioning data.

Includes:

- 14d RSI
- distance from 50DMA and 200DMA
- 1-year z-score
- drawdown from rolling 252-day high
- overbought/oversold/neutral stretch labels
- stretch states for key assets and cross-asset relationships

## Exact Vs Proxy-Based Data

Exact or direct provider series:

- Treasury yields from FRED: DGS2, DGS5, DGS10, DGS30
- IG and HY OAS from FRED where available
- 10Y breakeven inflation from FRED
- unemployment rate and nonfarm payroll series from FRED
- ETF and FX daily bars from Polygon where available

Proxy-based sections:

- dollar state uses UUP ETF
- VIX state uses VIXY ETF and realized volatility
- MOVE/bond-vol state uses TLT realized volatility
- breadth uses tracked ETF basket participation, not exchange-level breadth
- defensive rotation uses sector ETF relationships
- positioning uses RSI, moving-average distance, drawdown, z-score, and realized-volatility proxies

Unsupported data is not invented. If a category is not directly available from current providers, the snapshot labels the practical proxy used.

## Snapshot Schema

Run-scoped snapshots are the preferred read contract for downstream systems. They preserve the fields needed to audit what happened during a run: source status, numeric inputs, config, score output, market context, deltas, schema version, and stale markers.

Every structural and preview snapshot includes:

```json
{
  "foundation_scope": {
    "complete_for_v1": true,
    "run_frequency": "twice_daily",
    "intended_consumers": [
      "dashboard",
      "market_sniffer",
      "advisor_bot",
      "portfolio_overlay",
      "alerts"
    ]
  }
}
```

They also include market-truth and audit fields:

- `market_time_context`: market date, trading-day flag, half-day flag, previous close, session open, and data cutoff
- `data_quality`: completeness score, critical-missing flag, noncritical missing count, and confidence adjustment
- `signal_summary`: dominant, secondary, and ignored factors
- `regime_history_summary`: current regime duration, previous regime, and 30-day regime-change count where available
- `anomaly_flags`: extreme move, multi-asset divergence, and volatility-spike flags
- `execution`: duration metrics for fetch, compute, write, and total run time
- `state_confidence`: explainable confidence score with drivers and penalties
- `headline_summary`: one-line state summary and risk bias

### Structural Snapshot

Stored at:

```text
msm:run:<run_id>:snapshot
```

Top-level fields:

- `run_id`
- `computed_at_utc`
- `mode`: `structural`
- `foundation_scope`
- `market_time_context`
- `data_quality`
- `signal_summary`
- `regime_history_summary`
- `anomaly_flags`
- `execution`
- `run_status`
- `sources`
- `inputs`
- `config`
- `composite_score`
- `regime`
- `regime_label`
- `confidence`
- `state_confidence`
- `headline_summary`
- `reliable`
- `primary_drivers`
- `component_subscores`
- `component_states`
- `components`
- `trigger_states`
- `component_reasons`
- `subscores`
- `indicators`
- `market_context`
- `missing_components`
- `history_key`
- `delta`
- `meta`
- `is_stale`
- `stale_reason`

`run_status` shape:

```json
{
  "status": "success | partial | failed | stale | skipped",
  "warnings": [],
  "errors": []
}
```

Structural `delta` includes:

- `available`
- `previous_run_id`
- `previous_computed_at_utc`
- `score_change`
- `regime_changed`
- `previous_regime`
- `current_regime`
- `component_state_changes`
- `component_subscore_changes`
- `primary_drivers_added`
- `primary_drivers_removed`

### Preview Snapshot

Stored at:

```text
msm:run:<run_id>:preview_snapshot
```

Top-level fields:

- `run_id`
- `computed_at_utc`
- `mode`: `preview`
- `foundation_scope`
- `market_time_context`
- `data_quality`
- `signal_summary`
- `regime_history_summary`
- `anomaly_flags`
- `execution`
- `state_confidence`
- `headline_summary`
- `run_status`
- `sources`
- `inputs`
- `config`
- `component_subscores`
- `component_states`
- `component_statuses`
- `components`
- `preview_spillover_assessment`
- `session`
- `trigger_states`
- `delta`
- `meta`
- `is_stale`
- `stale_reason`

`session` shape:

```json
{
  "market_session": "pre_market | regular | after_hours | closed",
  "market_open": true,
  "computed_at_et": "ISO-8601 timestamp"
}
```

## Source Freshness Metadata

Snapshots store compact provider metadata under `sources`.

Each source entry contains:

- `status`: `success`, `partial`, or `failed`
- `fetched_at_utc`
- `details.series` or `details.tickers`
- `details.available`
- `details.missing`
- `details.available_count`
- `details.expected_count`
- `details.latest_observation`

The system does not store full raw FRED or Polygon payloads in Redis.

## Raw Input Capture

Snapshots store compact numeric audit inputs under `inputs`. These are the values that drove scoring decisions, not a duplicate of every raw source row.

Examples:

- IG and HY latest values, changes, and z-scores
- BKLN price versus 200DMA, drawdown, and volatility z-score
- XLF/SPY and KRE/SPY ratio values, moving-average gaps, and z-scores
- 30Y yield latest value, 20-day basis point move, and z-score
- USDJPY latest value, 5-day move, volatility percentile, moving averages, and confirmation level
- cross-component values such as `hy_ig_oas_gap` and `missing_components`

The expanded `market_context` stores additional reusable numeric summaries for downstream systems.

## Config / Threshold / Version Capture

Each snapshot includes a `config` object containing material run settings:

- `mode`
- `history_years`
- structural FRED series and market tickers
- preview market tickers
- freshness thresholds
- scoring thresholds
- scoring weights
- confirmation settings such as `usdjpy_confirm_level`

Scoring constants are centralized in:

```text
macro_stress_monitor/scoring/score.py
```

Market context logic is centralized in:

```text
macro_stress_monitor/services/market_context.py
```

Version metadata is centralized in:

```text
macro_stress_monitor/services/health.py
```

Environment overrides:

- `MSM_ENGINE_VERSION`: engine version string, default `2026.04.22`
- `USDJPY_CONFIRM_LEVEL`: USDJPY confirmation level, default `145`
- `MSM_STRUCTURAL_STALE_AFTER_SECONDS`: structural stale threshold, default `129600`
- `MSM_PREVIEW_STALE_AFTER_SECONDS`: preview stale threshold, default `28800`
- `MSM_NO_NETWORK_MOCK`: use deterministic local mock data when set truthy

### LLM Intelligence Configuration
The system uses a local OpenAI-compatible vLLM server to generate plain-English Morning Briefs and Evening Wraps based *strictly* on the deterministic structural/preview snapshots.

- `MSM_LLM_ENABLED`: enable/disable LLM features, default `true`
- `MSM_LLM_BASE_URL`: local vLLM API URL, default `http://127.0.0.1:30000/v1`
- `MSM_LLM_MODEL`: model name, default `Qwen2.5-14B-Instruct-AWQ`
- `MSM_LLM_TIMEOUT_SECONDS`: request timeout, default `60`

The LLM outputs are cached in Redis:
- `msm:brief:morning:latest`
- `msm:brief:evening:latest`

## Health Record

Stored at:

```text
msm:health
```

The health record contains:

- `computed_at_utc`
- `meta`
- `latest_structural_run_id`
- `latest_structural_computed_at_utc`
- `structural_age_seconds`
- `structural_stale`
- `structural_stale_reason`
- `latest_preview_run_id`
- `latest_preview_computed_at_utc`
- `preview_age_seconds`
- `preview_stale`
- `preview_stale_reason`
- `last_successful_structural_run`
- `last_successful_preview_run`
- `last_failed_run`, if already present

A run is considered successful for health tracking when:

- required sources are fetched without critical errors
- the snapshot is written
- `run_status.status` is `success` or `partial`

The health record is rebuilt after each structural and preview run. Reader code can also build a current health view if the stored key does not exist.

## Freshness / Stale Logic

Freshness logic is centralized in:

```text
macro_stress_monitor/services/health.py
```

Default thresholds:

- structural snapshots are stale after 36 hours
- preview snapshots are stale after 8 hours

Staleness is based on `computed_at_utc` age. Invalid or missing timestamps are treated as stale.

Snapshot stale markers:

- `is_stale`
- `stale_reason`

Health stale markers:

- `structural_stale`
- `structural_stale_reason`
- `preview_stale`
- `preview_stale_reason`

## Data Retention

Structural history and run-scoped snapshots will grow over time.

Recommended strategies:

- cap `msm:history` length
- periodically archive older snapshots to disk or a database
- optionally implement TTL for preview snapshots

Redis is used for fast state access, not long-term archival storage.

## Reader Services And API

Service modules:

- `macro_stress_monitor/services/reader.py`: read helpers for latest snapshots, history, run-scoped snapshots, health, and market context
- `macro_stress_monitor/services/health.py`: freshness, stale detection, version metadata, health construction
- `macro_stress_monitor/services/delta.py`: structural and preview delta computation
- `macro_stress_monitor/services/market_context.py`: broad structural market-state package construction

Reader functions:

- `get_latest_structural_snapshot()`
- `get_latest_preview_snapshot()`
- `get_latest_market_context()`
- `get_structural_history(limit=30)`
- `get_structural_snapshot(run_id)`
- `get_preview_snapshot(run_id)`
- `get_health_snapshot()`
- `compute_structural_delta(current, previous)`
- `compute_preview_delta(current, previous)`

Existing API endpoints:

- `/api/dashboard`
- `/api/latest`
- `/api/history`

Foundation endpoints:

- `/api/health`
- `/api/macro/latest`
- `/api/macro/preview`
- `/api/macro/context`

Render Layer API:

- `/api/render/terminal` (the master composite rendering payload)

Interpretation API:

- `/api/brief/current`
- `/api/brief/compare`
- `/api/brief/alerts`

## Terminal Operator Interface

The frontend (`macro_stress_monitor/web/`) has been fully re-architected into a high-density, Bloomberg-inspired operator console. It avoids generic dashboard components in favor of an unapologetically serious, tabular inspection layout.

Key features:
- **Dumb Rendering Pipeline:** The Javascript frontend contains zero conditional logic. It blindly consumes explicit DOM bindings (`id`, `text`, `class_name`, `html`) from the Python backend via a single unified payload (`/api/render/terminal`).
- **Deterministic Briefing ("The Brief"):** Incorporates deterministic interpretation of structural regimes versus intraday previews, presenting Top Risks, Top Supports, Alignment, and factual Caveats directly into the UI.
- **LLM Intelligence (Local):** Generates and caches structured "Morning Brief" and "Evening Wrap" reports using a disciplined local vLLM, explicitly preventing hallucination or trade advice.
- **Market Context Navigator:** Explodes the deep `market_context` data into 9 dense data grids (Macro/Rates, Credit, Equity, Sectors, Volatility, Safety, Cross-Asset, Breadth, Positioning).
- **Signal-First Styling:** Deep black backgrounds, monospaced typography, and hard neon color coding (Red/Yellow/Green) for explicit anomaly and state scanning.
- **Audit Drilldown:** An integrated raw JSON drilldown allows operators to inspect the unvarnished payload backing the interface at any time.

## Scheduling

Systemd timer files are included under:

```text
macro_stress_monitor/systemd/
```

Current timers:

- `macro_stress_monitor/systemd/macro_stress_full.timer`: structural update at 6:30 PM America/New_York on weekdays
- `macro_stress_monitor/systemd/macro_stress_preview.timer`: preview update at 12:30 PM America/New_York on weekdays

Current services:

- `macro_stress_monitor/systemd/macro_stress_full.service`
- `macro_stress_monitor/systemd/macro_stress_preview.service`

Both services use the full package path:

```text
WorkingDirectory=/home/ned/Documents/market_monitor_bot
ExecStart=/home/ned/venv/bin/python -m macro_stress_monitor.jobs.update_full
ExecStart=/home/ned/venv/bin/python -m macro_stress_monitor.jobs.update_preview
```

Install or refresh timers manually as appropriate for the host:

```bash
cd /home/ned/Documents/market_monitor_bot
sudo cp macro_stress_monitor/systemd/macro_stress_full.service /etc/systemd/system/
sudo cp macro_stress_monitor/systemd/macro_stress_full.timer /etc/systemd/system/
sudo cp macro_stress_monitor/systemd/macro_stress_preview.service /etc/systemd/system/
sudo cp macro_stress_monitor/systemd/macro_stress_preview.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now macro_stress_full.timer
sudo systemctl enable --now macro_stress_preview.timer
systemctl list-timers 'macro_stress*'
```

Cron can also run the same module commands if systemd is not used.

## Local Run / Manual Commands

Project parent directory:

```bash
cd /home/ned/Documents/market_monitor_bot
```

Run structural and preview jobs:

```bash
/home/ned/venv/bin/python -m macro_stress_monitor.jobs.update_full
/home/ned/venv/bin/python -m macro_stress_monitor.jobs.update_preview
```

Run with deterministic mock data and an isolated Redis database:

```bash
REDIS_URL=redis://localhost:6379/15 MSM_NO_NETWORK_MOCK=1 /home/ned/venv/bin/python -m macro_stress_monitor.jobs.update_full
REDIS_URL=redis://localhost:6379/15 MSM_NO_NETWORK_MOCK=1 /home/ned/venv/bin/python -m macro_stress_monitor.jobs.update_preview
```

Start the web app:

```bash
REDIS_URL=redis://localhost:6379/15 MSM_WEB_PORT=5001 /home/ned/venv/bin/python -m macro_stress_monitor.web.app
```

Call API endpoints:

```bash
curl -s http://localhost:5001/api/health | python3 -m json.tool
curl -s http://localhost:5001/api/macro/latest | python3 -m json.tool
curl -s http://localhost:5001/api/macro/preview | python3 -m json.tool
curl -s http://localhost:5001/api/macro/context | python3 -m json.tool
curl -s http://localhost:5001/api/dashboard | python3 -m json.tool
```

## Quick Sanity Check

Run:

```bash
redis-cli GET msm:health | python3 -m json.tool
```

This should show:

- a fresh structural timestamp
- a fresh preview timestamp
- no stale flags

If this does not hold, the system is not healthy. If validating against isolated Redis DB 15, use:

```bash
redis-cli -n 15 GET msm:health | python3 -m json.tool
```

## Validation Checklist

Run unit tests:

```bash
cd /home/ned/Documents/market_monitor_bot
/home/ned/venv/bin/python -m unittest discover -s macro_stress_monitor/tests -p 'test_*.py'
```

Run compile checks:

```bash
cd /home/ned/Documents/market_monitor_bot
/home/ned/venv/bin/python -m py_compile macro_stress_monitor/scoring/score.py macro_stress_monitor/services/delta.py macro_stress_monitor/services/health.py macro_stress_monitor/services/reader.py macro_stress_monitor/services/market_context.py macro_stress_monitor/storage/redis_client.py macro_stress_monitor/jobs/update.py macro_stress_monitor/jobs/update_full.py macro_stress_monitor/jobs/update_preview.py macro_stress_monitor/web/app.py
```

Validate structural update:

```bash
cd /home/ned/Documents/market_monitor_bot
REDIS_URL=redis://localhost:6379/15 MSM_NO_NETWORK_MOCK=1 /home/ned/venv/bin/python -m macro_stress_monitor.jobs.update_full
redis-cli -n 15 GET msm:latest_structural_run_id
redis-cli -n 15 GET msm:latest_run_id
redis-cli -n 15 GET msm:latest | python3 -m json.tool
```

Validate structural run snapshot and market context:

```bash
RUN_ID=$(redis-cli -n 15 GET msm:latest_structural_run_id)
redis-cli -n 15 GET "msm:run:${RUN_ID}:snapshot" | python3 -m json.tool
redis-cli -n 15 GET "msm:run:${RUN_ID}:snapshot" \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["market_context"].keys())'
redis-cli -n 15 GET "msm:run:${RUN_ID}:snapshot" \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["market_time_context"]); print(d["data_quality"]); print(d["signal_summary"]); print(d["state_confidence"]); print(d["execution"])'
```

Validate preview update:

```bash
REDIS_URL=redis://localhost:6379/15 MSM_NO_NETWORK_MOCK=1 /home/ned/venv/bin/python -m macro_stress_monitor.jobs.update_preview
redis-cli -n 15 GET msm:latest_preview_run_id
```

Validate preview run snapshot:

```bash
PREVIEW_RUN_ID=$(redis-cli -n 15 GET msm:latest_preview_run_id)
redis-cli -n 15 GET "msm:run:${PREVIEW_RUN_ID}:preview_snapshot" | python3 -m json.tool
```

Validate health record:

```bash
redis-cli -n 15 GET msm:health | python3 -m json.tool
```

Validate stale behavior with a short threshold:

```bash
REDIS_URL=redis://localhost:6379/15 MSM_NO_NETWORK_MOCK=1 MSM_PREVIEW_STALE_AFTER_SECONDS=0 /home/ned/venv/bin/python -m macro_stress_monitor.jobs.update_preview
redis-cli -n 15 GET msm:health | python3 -m json.tool
```

With a zero-second threshold, the preview health view should report stale once the computed snapshot age is greater than zero.

## Future Integration

This engine is designed to support:

- Market Sniffer composite scoring
- portfolio risk analysis layers
- local advisor/analysis agents
- dashboards and alerting services

These systems should consume snapshot data via reader services or API endpoints.

## Troubleshooting Notes

Redis connection failures:

- verify Redis is running: `redis-cli ping`
- verify the selected DB and URL: `echo "$REDIS_URL"`
- for isolated validation, use `REDIS_URL=redis://localhost:6379/15`

Missing Python dependencies:

- use `/home/ned/venv/bin/python`, matching the systemd service files
- system `python3` may not have provider/data dependencies such as `pandas`

Provider data unavailable:

- confirm `FRED_API_KEY` is set for FRED data
- confirm `POLYGON_API_KEY` is set for Polygon data
- run with `MSM_NO_NETWORK_MOCK=1` to validate the pipeline without network/provider dependencies
- inspect `sources` in the latest run snapshot for missing series or tickers

Dashboard/API has no data:

- run a structural update first
- run a preview update if preview endpoints are expected to return data
- inspect `msm:latest_structural_run_id`, `msm:latest_preview_run_id`, and `msm:health`

Unexpected stale status:

- inspect `computed_at_utc` on the relevant snapshot
- inspect `structural_age_seconds` or `preview_age_seconds` in `msm:health`
- confirm threshold overrides are not set too low

Schema compatibility:

- existing Redis keys remain in place
- run-scoped snapshots now include additional fields
- strict consumers should tolerate additive fields
