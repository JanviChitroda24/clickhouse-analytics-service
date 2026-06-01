"""
Dagster job and schedule for the daily data service pipeline.

Cron 0 17 * * 1-5 = 5 PM Monday–Friday (after US market close), matching Week 3.
Schedule is STOPPED by default — turn on in the Dagster UI when you want auto-runs.
"""

from dagster import DefaultScheduleStatus, ScheduleDefinition, define_asset_job

daily_pipeline_job = define_asset_job(
    name="daily_data_service_pipeline",
    selection="*",
    description=(
        "Infrastructure check → ingestion verify → quality gate → "
        "MV optimize → sector summary → benchmark report"
    ),
)

daily_schedule = ScheduleDefinition(
    job=daily_pipeline_job,
    cron_schedule="0 17 * * 1-5",
    default_status=DefaultScheduleStatus.STOPPED,
    description="Daily pipeline after market close (5 PM Mon–Fri); enable in Dagster UI",
)
