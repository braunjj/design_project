from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    asset_check,
)
import random
import time

from .hooli import *

@asset_check(asset=locations)
def randomly_fails():
    is_serious = True
    coinFlip = random.choice([True, False])
    time.sleep(2.5)
    return AssetCheckResult(
        passed=coinFlip,
        severity=AssetCheckSeverity.ERROR if is_serious else AssetCheckSeverity.WARN,
    )

@asset_check(asset=locations)
def fails_less_often():
    is_serious = True
    coinFlip = random.choice([True, False])
    time.sleep(4)
    return AssetCheckResult(
        passed=coinFlip,
        severity=AssetCheckSeverity.ERROR if is_serious else AssetCheckSeverity.WARN,
    )

@asset_check(asset=locations)
def res_id_has_no_nulls():
    is_serious = False
    coinFlip = random.choice([True, False])
    return AssetCheckResult(
        passed=coinFlip,
        severity=AssetCheckSeverity.ERROR if is_serious else AssetCheckSeverity.WARN,
    )

@asset_check(asset=locations)
def valid_email():
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
