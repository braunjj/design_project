from dagster import (
    Definitions,
    load_assets_from_package_module,
    external_assets_from_specs
)

from . import assets
from .assets.hooli import raw_logs, processed_logs
external_assets = external_assets_from_specs([raw_logs, processed_logs])

defs = Definitions(
    assets=[*external_assets + load_assets_from_package_module(assets)]
)