from dagster import (
    define_asset_job,
    load_assets_from_package_module,
    AssetSelection,
    repository,
    op,
    graph,
    job,
    sensor,
    DefaultSensorStatus,
    ScheduleDefinition,
    RunRequest,
    Failure,
    AssetCheckSeverity, 
    asset_check, 
    AssetCheckResult,
    asset,
    AutoMaterializePolicy, 
    SkipReason,
    FreshnessPolicy,
    MetadataValue,
    OutputContext,
    TableColumn,
    TableMetadataValue,
    TableRecord,
    TableSchema,
)

import random
import time

from . import assets

daily_refresh_schedule = ScheduleDefinition(
    job=define_asset_job(name="hackernews_job",selection=AssetSelection.groups("hackernews")),cron_schedule="0 0 * * *"
)
daily_etl_schedule = ScheduleDefinition(
    job=define_asset_job(name="daily_etl_job",selection=AssetSelection.groups("etl")),cron_schedule="0 0 * * *"
)

@op
def my_op1():
    num = random.randint(60, 240)
    time.sleep(num)

@op
def randomNumber() -> int:
    time.sleep(60)
    num = random.randint(0, 1)
    if num > 0:
        return 1 
    else:
        return 0

@op
def falkey_op(num:int):
    time.sleep(5)
    if num > 0:
        pass 
    else:
        raise Failure(
            description="Flakey op failed",
        )

@graph
def my_graph():
    my_op1()

@graph
def might_fail():
    falkey_op(randomNumber())

count = 200
jobs = list()
bad_jobs = list()
schedules = list()

@job
def my_etl_job():
    my_graph.to_job(name="my_etl_job", description="ETL job description")

for x in range(count):
    jobs.append(my_graph.to_job(name="%s%s"%("etl_job_",str(x)), description="ETL job description"))

for x in range(count):
    schedules.append(ScheduleDefinition(job=jobs[x], cron_schedule="0 * * * *"),)

for x in range(25):
    bad_jobs.append(might_fail.to_job(name="%s%s"%("etl_job_f_",str(x)), description="This job might fail"))


@sensor(job=my_etl_job, minimum_interval_seconds=30, default_status=DefaultSensorStatus.RUNNING)
def etl_sensor(context):
    num = random.randint(1, 10)
    sleeptime = random.randint(1, 5)
    context.log.info("Hello world!")
    context.log.info("Starting evaluation")
    context.log.info("Evaluating...")
    context.log.info("Evaluation complete")
    time.sleep(sleeptime)
    context.log.info("NUM: " + str(num))

    if num > 5:
        yield RunRequest(
        )
    else:
       yield SkipReason("Skipped because coin flip was false")


@sensor(job=might_fail)
def my_etl_sensor(context):
    cursor = int(context.cursor)
    if cursor <= 2050:
        for runId in range(3):
            yield RunRequest(
                run_key = str(runId)
            )
    cursor = random.randint(2000, 2999)
    context.update_cursor(str(cursor))

my_custom_policy = AutoMaterializePolicy.lazy().with_rules(
    # AutoMaterializeRule.materialize_on_cron('*/15 * * * *', timezone='EST', all_partitions=False),
)

my_schema = TableSchema(
        columns=[
            TableColumn(name=col, type=str(pl_type), description=descriptions.get(col))
            for col, pl_type in df.schema.items()
        ]
    )

@asset(
    group_name="bronze", 
    compute_kind="sling", 
    code_version="4",
    freshness_policy = FreshnessPolicy(maximum_lag_minutes=60),
    auto_materialize_policy = my_custom_policy,
    )
def reservations_raw():
    """
    Dagster Organizations Core Table. This table contains one row for each organization.

Raw SQL:
    with sf_accounts as (
        select
            account_id,
            organization_id,
            account_type,
            account_status

        from {{ ref('stg_salesforce__accounts') }}
        where not is_deleted and organization_id regexp '\\d+'
        qualify row_number() over (partition by organization_id order by created_at desc) = 1
    ),

    stripe_customer as (
        select

            organization_id,
            stripe_customer_id

        from {{ ref('stg_postgres__customer_info') }}
    ),

    orgs as (
        select *
        from {{ ref('stg_postgres__organizations') }}
    ),

    user_orgs as (
        select

            organization_id,
            max(last_user_login) as last_user_login

        from {{ ref('user_organizations') }}
        where not is_elementl_user
        group by 1
    ),

    runs as (
        select

            organization_id,
            max(started_at) as last_run_at

        from {{ ref('stg_postgres__runs') }}
        group by 1
    ),

    final as (

        select

            orgs.organization_id,
            orgs.organization_name,

            orgs.plan_type,
            orgs.is_internal,
            orgs.has_saml_sso,

            coalesce(orgs.status, 'ACTIVE') as status,
            coalesce(orgs.status, 'ACTIVE') = 'ACTIVE' and not orgs.is_internal as is_active,

            user_orgs.last_user_login,
            runs.last_run_at,

            sf_accounts.account_id as salesforce_account_id,
            coalesce(sf_accounts.account_type, 'unset') as salesforce_account_type,
            coalesce(sf_accounts.account_status, 'unset') as salesforce_account_status,

            stripe_customer.stripe_customer_id,

            orgs.organization_metadata,
            orgs.organization_settings,

            orgs.created_at as org_created_at,
            orgs.updated_at as org_updated_at

        from orgs
        left join user_orgs using (organization_id)
        left join stripe_customer using (organization_id)
        left join runs using (organization_id)
        left join sf_accounts using (organization_id)
    )

    select * from final
    """
    coinFlip = random.choice([True, False])
    time.sleep(2.75)
    if coinFlip:
        pass
    else: 
        raise Exception("Randomly failed!")

@asset_check(asset=reservations_raw)
def randomly_fails():
    is_serious = True
    coinFlip = random.choice([True, False])
    time.sleep(2.5)
    return AssetCheckResult(
        passed=coinFlip,
        severity=AssetCheckSeverity.ERROR if is_serious else AssetCheckSeverity.WARN,
    )

@asset_check(asset=reservations_raw)
def fails_less_often():
    is_serious = True
    coinFlip = random.choice([True, False])
    time.sleep(4)
    return AssetCheckResult(
        passed=coinFlip,
        severity=AssetCheckSeverity.ERROR if is_serious else AssetCheckSeverity.WARN,
    )

@asset_check(asset=reservations_raw)
def res_id_has_no_nulls():
    is_serious = False
    coinFlip = random.choice([True, False])
    return AssetCheckResult(
        passed=coinFlip,
        severity=AssetCheckSeverity.ERROR if is_serious else AssetCheckSeverity.WARN,
    )

@asset_check(asset=reservations_raw)
def valid_email():
    is_serious = False
    coinFlip = random.choice([True, False])

    return AssetCheckResult(
        passed=coinFlip,
        severity=AssetCheckSeverity.ERROR if is_serious else AssetCheckSeverity.WARN,
    )

@asset_check(asset=reservations_raw)
def valid_last_name():
    is_serious = False
    return AssetCheckResult(
        passed=True,
        severity=AssetCheckSeverity.ERROR if is_serious else AssetCheckSeverity.WARN,
    )

@repository
def quickstart_etl():
    return [
        reservations_raw,
        valid_email,
        load_assets_from_package_module(assets),
        daily_refresh_schedule,
        daily_etl_schedule,
        jobs,
        schedules,
        bad_jobs,
        etl_sensor,
        my_etl_sensor,
        res_id_has_no_nulls,
        fails_less_often,
        randomly_fails,
        valid_last_name,
    ]
