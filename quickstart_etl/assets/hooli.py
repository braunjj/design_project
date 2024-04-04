from dagster import (
    asset,
    FreshnessPolicy,
)
import random
import time

@asset(
    group_name="RAW_DATA", 
    compute_kind="sling", 
    code_version="4",
    freshness_policy = FreshnessPolicy(maximum_lag_minutes=60),
        owners=["gilfoyle@piedpiper.com", "team: ingest"],
    tags={"support_tier": "1", "consumer": "analytics"},
    )
def reservations():
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

import time


@asset(
    group_name="RAW_DATA", 
    compute_kind="stripe", 
    code_version="2",
    metadata={},
    freshness_policy = FreshnessPolicy(maximum_lag_minutes=60),
        owners=["gilfoyle@piedpiper.com", "team: ingest"],
    tags={"support_tier": "1", "consumer": "analytics"},
    )
def orders():
    """
    ## My asset
    A view of asset metrics used for consumption management reporting in the app

    ### Raw SQL:
    ```sql
    {{ config(snowflake_warehouse="L_WAREHOUSE") }}

    with deduped_internal_asset_materialization_events as (
        select * from {{ ref('reporting_deduped_internal_asset_materialization_metrics') }}
    ),

    metadata_asset_materialization_events as (
        select * from {{ ref('reporting_metadata_asset_materialization_metrics') }}
    )

    (
        select * from deduped_internal_asset_materialization_events
    )
    union
    (
        select * from metadata_asset_materialization_events
    )
    ```
    """
    time.sleep(1)
    pass

@asset(
    group_name="RAW_DATA", 
    compute_kind="stripe", 
    code_version="1",
    description="A table containing all users data",
    owners=["gilfoyle@piedpiper.com", "team: ingest"],
    tags={"support_tier": "1", "consumer": "analytics"},
    )
def users():
    time.sleep(1)
    pass

@asset(
    group_name="RAW_DATA", 
    compute_kind="sling", 
    code_version="1",
    owners=["gilfoyle@piedpiper.com", "team: ingest"],
    tags={"support_tier": "1", "consumer": "analytics"},
    metadata={
        "num_records": 10251,
        "rows_modified":854,
        "sync_time": "72.24",
        "sync_start": "Mar 31, 10:51 PM", 
        "total_elapsed_time": "86:24",
        "nulls": "0"
    },
    )
def locations():
    """
    This table contains one row for each location.

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
    time.sleep(1)
    pass


@asset(
    group_name="CLEANED", 
    compute_kind="dbt", 
    code_version="1",
    description="dbt model for: orders_cleaned",
        owners=["gilfoyle@piedpiper.com", "team: analytics"],
    tags={"support_tier": "1", "consumer": "finance"},
    )
def orders_cleaned(orders):
    time.sleep(1)
    pass

@asset(
    group_name="CLEANED", 
    compute_kind="dbt", 
    code_version="1",
    description="dbt model for: locations_cleaned",
    owners=["gilfoyle@piedpiper.com", "team: analytics"],
    tags={"support_tier": "1", "consumer": "finance"},
    )
def locations_cleaned(locations):
    time.sleep(1)
    pass


@asset(
    group_name="CLEANED", 
    compute_kind="dbt", 
    code_version="1",
    description="dbt model for: users_cleaned",
    owners=["gilfoyle@piedpiper.com", "team: analytics"],
    tags={"support_tier": "1", "consumer": "finance"},
    )
def users_cleaned(users):
    time.sleep(1)
    pass

@asset(
    group_name="CLEANED", 
    compute_kind="dbt", 
    code_version="1",
    description="dbt model for: reservations_cleaned",
    owners=["gilfoyle@piedpiper.com", "team: analytics"],
    tags={"support_tier": "1", "consumer": "finance"},
    )
def reservations_cleaned(reservations):
    time.sleep(1)
    pass


@asset(
    group_name="ANALYTICS", 
    compute_kind="dbt", 
    code_version="1",
    description="dbt model for: orders_augmented",
    owners=["gilfoyle@piedpiper.com", "team: analytics"],
    tags={"PII":"", "consumer": "sales ops", "consumer": "ml"},
    )
def orders_augmented(orders_cleaned, users_cleaned, locations_cleaned):
    time.sleep(1)
    pass

@asset(
    group_name="ANALYTICS", 
    compute_kind="dbt", 
    code_version="1",
    description="dbt model for: company_stats",
    owners=["gilfoyle@piedpiper.com", "team: analytics"],
    tags={"PII":"", "consumer": "sales ops", "consumer": "ml"},
    )
def company_stats(orders_augmented):
    time.sleep(1)
    pass

@asset(
    group_name="ANALYTICS", 
    compute_kind="dbt", 
    code_version="1",
    description="dbt model for: order_stats",
    owners=["dinesh@piedpiper.com", "team: analytics"],
    tags={"PII":"", "consumer": "sales ops", "consumer": "ml"},

    )
def order_stats(orders_augmented):
    time.sleep(1)
    pass

@asset(
    group_name="ANALYTICS", 
    compute_kind="dbt", 
    code_version="1",
    owners=["dinesh@piedpiper.com", "team: analytics"],
    tags={"PII":"", "consumer": "sales ops", "consumer": "ml",},
    )
def sku_stats(orders_augmented):
    time.sleep(1)
    pass

@asset(
    group_name="ANALYTICS", 
    compute_kind="dbt", 
    code_version="1",
    owners=["richard@piedpiper.com", "team: analytics"],
    tags={"PII":"", "consumer": "sales ops", "consumer": "ml"},
    )
def weekly_order_summary(order_stats):
    time.sleep(1)
    pass

@asset(
    group_name="ANALYTICS", 
    compute_kind="dbt", 
    code_version="1",
    description="dbt model for: company_perf",
    owners=["richard@piedpiper.com", "team: analytics"],
    tags={"support_tier": "1", "consumer": "sales ops", "consumer": "ml", "consumer": "marketing"},
    )
def company_perf(order_stats):
    time.sleep(1)
    pass


@asset(
    group_name="ANALYTICS", 
    compute_kind="scikit learn", 
    code_version="1",
    description="model parameters that best fit the weekly order summary data",
    owners=["richard@piedpiper.com", "team: analytics"],
    tags={"support_tier": "1", "consumer": "sales ops", "consumer": "ml", "consumer": "marketing"},
    )
def order_forecast_model(weekly_order_summary):
    time.sleep(1)
    pass


@asset(
    group_name="FORECASTING", 
    compute_kind="python", 
    code_version="1",
    owners=["richard@piedpiper.com", "team: ml"],
    tags={"support_tier": "2"},
    )
def predicted_orders(weekly_order_summary, order_forecast_model):
    time.sleep(1)
    pass

@asset(
    group_name="FORECASTING", 
    compute_kind="databricks", 
    code_version="1",
    owners=["richard@piedpiper.com", "team: ml"],
    tags={"support_tier": "2"},
    )
def big_orders(predicted_orders):
    time.sleep(1)
    pass

@asset(
    group_name="FORECASTING", 
    compute_kind="scikit learn",
    code_version="1",
    owners=["richard@piedpiper.com", "team: ml"],
    tags={"support_tier": "1"},
    )
def model_stats_by_month(weekly_order_summary, order_forecast_model):
    time.sleep(1)
    pass

@asset(
    group_name="FORECASTING", 
    compute_kind="python",
    code_version="1",
    owners=["dinesh@piedpiper.com", "team: ml"],
    tags={"support_tier": "1"},
    )
def small_orders(predicted_orders):
    time.sleep(1)
    pass

@asset(
    group_name="FORECASTING", 
    compute_kind="jupyter",
    code_version="1",
    owners=["dinesh@piedpiper.com", "team: ml"],
    tags={"support_tier": "1"},
    )
def model_nb(weekly_order_summary, order_forecast_model):
    time.sleep(1)
    pass


@asset(
    group_name="MARKETING", 
    compute_kind="Python",
    code_version="1",
    description="Computes avg order KPI, must be updated regularly for exec dashboard",
    owners=["dinesh@piedpiper.com", "team: analytics"],
    tags={"support_tier": "1"},
    )
def avg_orders(company_perf):
    time.sleep(1)
    pass

@asset(
    group_name="MARKETING", 
    compute_kind="Python",
    code_version="1",
    description="Computes min order",
    owners=["dinesh@piedpiper.com", "team: analytics"],
    tags={"PII":""},
    )
def min_order(company_perf):
    time.sleep(1)
    pass

@asset(
    group_name="MARKETING", 
    compute_kind="Hex",
    code_version="1",
    description="Creates a file for a BI tool based on the current quarters top product, represented as a dynamic partition",
    owners=["dinesh@piedpiper.com", "team: analytics"],
    tags={"PII":""},
    )
def key_product_deepdive(sku_stats):
    time.sleep(1)
    pass