from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_package_module,
)

from . import assets
from .assets.checks import * 

daily_refresh_schedule = ScheduleDefinition(
    job=define_asset_job(name="all_assets_job"), cron_schedule="0 0 * * *"
)

checks = [randomly_fails, valid_last_name, valid_email, res_id_has_no_nulls, fails_less_often]

defs = Definitions(
    assets=load_assets_from_package_module(assets), 
    schedules=[daily_refresh_schedule],
    asset_checks = checks
)
