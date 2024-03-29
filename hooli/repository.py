from dagster import (
    define_asset_job,
    load_assets_from_package_module,
    AssetSelection,
    repository,
    op,
    graph,
    job,
    sensor,
    ScheduleDefinition,
    RunRequest,
    Failure,
    AssetCheckSeverity, 
    asset_check, 
    AssetCheckResult,
    asset,
)
import random
import time

from . import assets

@repository
def quickstart_etl():
    return [
        load_assets_from_package_module(assets),
    ]
