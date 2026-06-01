"""
Dagster definitions for the Stock Analytics Data Service.

Six assets: infrastructure → ingestion verify → quality gate → MV optimize
→ sector summary → benchmark report. Daily schedule at 5 PM Mon–Fri (stopped
by default — enable in the Dagster UI when ready).

Run from project root:
    dagster dev -m dagster_pipeline
    → http://localhost:3000
"""

from dagster import Definitions, load_assets_from_modules

from dagster_pipeline import assets, jobs

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    jobs=[jobs.daily_pipeline_job],
    schedules=[jobs.daily_schedule],
)
