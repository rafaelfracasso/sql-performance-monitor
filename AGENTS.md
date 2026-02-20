# AGENTS.md

> Project map for AI agents. Keep this file up-to-date as the project evolves.

## Project Overview
SQL Monitor is a multi-database performance monitoring tool with an interactive web dashboard, LLM-powered query analysis (Google Gemini), and an automated optimization engine supporting SQL Server, PostgreSQL, and SAP HANA.

## Tech Stack
- **Language:** Python 3.11+
- **Framework:** FastAPI + Uvicorn
- **Frontend:** Jinja2 templates + Vanilla JS + Chart.js
- **Metrics DB:** DuckDB (embedded)
- **Monitored DBs:** SQL Server, PostgreSQL, SAP HANA
- **LLM:** Google Gemini (google-genai)

## Project Structure
```
check_sql_server_performance/
├── main.py                        # Entry point: starts monitor + FastAPI server
├── config.json                    # Application settings (thresholds, intervals, etc.)
├── prompts.json                   # LLM prompt templates
├── config/
│   ├── databases.json             # Monitored database definitions (supports ${ENV_VAR})
│   └── databases.json.example    # Template for databases.json
├── sql_monitor/
│   ├── api/
│   │   ├── app.py                 # FastAPI app initialization
│   │   ├── routes.py              # All dashboard routes and API endpoints
│   │   ├── models.py              # Pydantic models for API request/response
│   │   ├── static/
│   │   │   ├── app.js             # Core JS: Utils, API client, ChartFactory (window.SQLMonitor)
│   │   │   ├── style.css          # Main styles with CSS Variables (light/dark theme)
│   │   │   ├── css/
│   │   │   │   └── settings.css   # Settings-page-specific styles
│   │   │   └── js/
│   │   │       └── settings.js    # Settings page logic (SettingsManager object)
│   │   └── templates/             # Jinja2 HTML templates (16 pages)
│   │       ├── base.html          # Base layout: nav, modal, theme toggle, hamburger menu
│   │       ├── dashboard_home.html
│   │       ├── dashboard_queries.html
│   │       ├── dashboard_users.html
│   │       ├── dashboard_hosts.html
│   │       ├── dashboard_applications.html
│   │       ├── dashboard_instances.html
│   │       ├── dashboard_alerts.html
│   │       ├── dashboard_duckdb.html
│   │       ├── dashboard_llm.html
│   │       ├── dashboard_trends.html
│   │       ├── dashboard_settings.html
│   │       ├── query_detail.html
│   │       ├── plan_detail.html
│   │       ├── plan_list.html
│   │       └── alert_detail.html
│   ├── core/                      # Abstract base classes
│   │   ├── base_collector.py
│   │   ├── base_connection.py
│   │   ├── base_extractor.py
│   │   └── database_types.py
│   ├── collectors/                # Per-SGBD metric collectors
│   ├── connections/               # Per-SGBD connection implementations
│   ├── extractors/                # Per-SGBD metadata extractors
│   ├── factories/
│   │   └── database_factory.py    # Factory: creates collectors/connections/extractors by DB type
│   ├── monitor/
│   │   ├── database_monitor.py    # Single-database monitor orchestrator
│   │   └── multi_monitor.py       # Multi-database coordinator
│   ├── optimization/              # Automated optimization engine
│   │   ├── approval_engine.py
│   │   ├── executor.py
│   │   ├── impact_analyzer.py
│   │   ├── plan_state.py
│   │   ├── risk_classifier.py
│   │   ├── scheduler.py
│   │   ├── veto_system.py
│   │   └── weekly_planner.py
│   └── utils/                     # Shared utilities (16 modules)
│       ├── metrics_store.py       # DuckDB storage for all metrics
│       ├── llm_analyzer.py        # Google Gemini integration
│       ├── query_analytics.py     # Query aggregation and analysis
│       ├── performance_checker.py # Threshold-based performance checks
│       ├── baseline_calculator.py # Historical baseline computation
│       ├── connection_pool.py     # DB connection pool management
│       ├── teams_notifier.py      # Teams webhook notifications
│       └── metadata_cache.py     # Metadata caching layer
├── tests/
│   ├── unit/                      # Unit tests (5 files)
│   ├── integration/               # Integration tests (8 files, includes settings API)
│   ├── advanced/                  # Advanced tests (API, full cycle, LLM, optimization)
│   └── e2e/                       # End-to-end tests
└── scripts/                       # Utility scripts (data cleanup, migration, validation)
```

## Key Entry Points
| File | Purpose |
|------|---------|
| `main.py` | Application entry point |
| `sql_monitor/api/app.py` | FastAPI app setup and middleware |
| `sql_monitor/api/routes.py` | All HTTP routes and API endpoints |
| `sql_monitor/monitor/multi_monitor.py` | Multi-DB monitoring coordinator |
| `sql_monitor/factories/database_factory.py` | DB-type factory pattern |
| `sql_monitor/utils/metrics_store.py` | DuckDB metrics persistence |

## Key Conventions
- Frontend JS: all utilities exposed via `window.SQLMonitor` namespace
- Toast notifications: `SQLMonitor.Utils.showToast(message, type)` — types: success, error, warning, info
- Period mapping in routes: `{"1h": 1, "6h": 6, "12h": 12, "24h": 24, "7d": 168, "30d": 720}`
- Settings forms use `SettingsManager.setBtnLoading()` for loading states
- No CSS framework — CSS Variables for theming throughout

## Documentation
| Document | Path | Description |
|----------|------|-------------|
| README | README.md | Project landing page |
| Getting Started | docs/getting-started.md | Installation, setup, first steps |
| Configuration | docs/configuration.md | config.json, databases.json, env vars |
| Databases | docs/databases.md | Per-SGBD configuration and permissions |
| Dashboard | docs/dashboard.md | Dashboard pages and usage |
| Optimization | docs/optimization.md | Optimization engine and weekly plans |
| API Reference | docs/api.md | REST API endpoints |
| Project Spec | .ai-factory/DESCRIPTION.md | Tech stack and feature overview |
| Architecture | .ai-factory/ARCHITECTURE.md | Architecture decisions and guidelines |
| Agent Map | AGENTS.md | This file |

## AI Context Files
| File | Purpose |
|------|---------|
| AGENTS.md | This file — project structure map |
| .ai-factory/DESCRIPTION.md | Project specification and tech stack |
| .ai-factory/ARCHITECTURE.md | Architecture decisions and guidelines |
