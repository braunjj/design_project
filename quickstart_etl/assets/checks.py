from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    asset_check,
    build_last_update_freshness_checks
)
import random
import time

from .hooli import *

@asset_check(asset=locations)
def non_null():
    is_serious = True
    coinFlip = random.choice([True, False])
    time.sleep(2.5)
    return AssetCheckResult(
        passed=coinFlip,
        severity=AssetCheckSeverity.ERROR if is_serious else AssetCheckSeverity.WARN,
    )

@asset_check(asset=locations)
def valid_email():
    is_serious = True
    coinFlip = random.choice([True, False])
    time.sleep(4)
    return AssetCheckResult(
        passed=coinFlip,
        severity=AssetCheckSeverity.ERROR if is_serious else AssetCheckSeverity.WARN,
    )

@asset_check(asset=locations)
def valid_phone():
    is_serious = False
    coinFlip = random.choice([True, False])
    return AssetCheckResult(
        passed=coinFlip,
        severity=AssetCheckSeverity.ERROR if is_serious else AssetCheckSeverity.WARN,
    )

@asset_check(asset=locations)
def unique_userid():
    is_serious = False
    coinFlip = random.choice([True, False])

    return AssetCheckResult(
        passed=coinFlip,
        severity=AssetCheckSeverity.ERROR if is_serious else AssetCheckSeverity.WARN,
    )

@asset_check(asset=locations)
def valid_last_name():
    is_serious = False
    return AssetCheckResult(
        passed=True,
        severity=AssetCheckSeverity.ERROR if is_serious else AssetCheckSeverity.WARN,
    )
