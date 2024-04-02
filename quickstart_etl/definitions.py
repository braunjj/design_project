from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_package_module,
    build_last_update_freshness_checks
)
from datetime import datetime
from . import assets
from .assets.checks import * 

daily_refresh_schedule = ScheduleDefinition(
    job=define_asset_job(name="all_assets_job"), cron_schedule="0 0 * * *"
)

checks = [valid_email, valid_last_name, valid_phone, unique_userid, non_null]


defs = Definitions(
    assets=load_assets_from_package_module(assets), 
    schedules=[daily_refresh_schedule],
    asset_checks = checks
)
