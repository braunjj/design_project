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
    metadata={"owner": "josh@mycompany.com", "priority": "high", "team": "sales"},
    freshness_policy = FreshnessPolicy(maximum_lag_minutes=3),
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
    )
def users():
    time.sleep(1)
    pass

@asset(
    group_name="RAW_DATA", 
    compute_kind="sling", 
    code_version="1",
    )
def locations():
    time.sleep(1)
    pass


@asset(
    group_name="CLEANED", 
    compute_kind="dbt", 
    code_version="1",
    description="dbt model for: orders_cleaned",
    )
def orders_cleaned(orders):
    time.sleep(1)
    pass

@asset(
    group_name="CLEANED", 
    compute_kind="dbt", 
    code_version="1",
    description="dbt model for: locations_cleaned",
    )
def locations_cleaned(locations):
    time.sleep(1)
    pass


@asset(
    group_name="CLEANED", 
    compute_kind="dbt", 
    code_version="1",
    description="dbt model for: users_cleaned",
    )
def users_cleaned(users):
    time.sleep(1)
    pass

@asset(
    group_name="CLEANED", 
    compute_kind="dbt", 
    code_version="1",
    description="dbt model for: reservations_cleaned",
    tags={"priority": "high"},
    )
def reservations_cleaned(reservations):
    time.sleep(1)
    pass


@asset(
    group_name="ANALYTICS", 
    compute_kind="dbt", 
    code_version="1",
    description="dbt model for: orders_augmented",
    owners=["pete@hooli.com","team: Sales Ops"]
    )
def orders_augmented(orders_cleaned, users_cleaned, locations_cleaned):
    time.sleep(1)
    pass

@asset(
    group_name="ANALYTICS", 
    compute_kind="dbt", 
    code_version="1",
    description="dbt model for: company_stats",
    owners=["pete@hooli.com","team: Sales Ops"],
    tags={"priority": "critical", "consumer": "sales"},
    )
def company_stats(orders_augmented):
    time.sleep(1)
    pass

@asset(
    group_name="ANALYTICS", 
    compute_kind="dbt", 
    code_version="1",
    description="dbt model for: order_stats",
    owners=["pete@hooli.com","team: Sales Ops"]
    )
def order_stats(orders_augmented):
    time.sleep(1)
    pass

@asset(
    group_name="ANALYTICS", 
    compute_kind="dbt", 
    code_version="1",
    description="dbt model for: sku_stats",
    owners=["pete@hooli.com","team: Sales Ops"]
    )
def sku_stats(orders_augmented):
    time.sleep(1)
    pass

@asset(
    group_name="ANALYTICS", 
    compute_kind="dbt", 
    code_version="1",
    description="dbt model for: weekly_order_summary",
    owners=["pete@hooli.com","team: Sales Ops"]
    )
def weekly_order_summary(order_stats):
    time.sleep(1)
    raise Exception("Failed!")

@asset(
    group_name="ANALYTICS", 
    compute_kind="dbt", 
    code_version="1",
    description="dbt model for: company_perf",
    owners=["pete@hooli.com","team: Sales Ops"]
    )
def company_perf(order_stats):
    time.sleep(1)
    pass


@asset(
    group_name="ANALYTICS", 
    compute_kind="scikit learn", 
    code_version="1",
    description="model parameters that best fit the weekly order summary data",
    owners=["pete@hooli.com","team: Sales Ops"]
    )
def order_forecast_model(weekly_order_summary):
    time.sleep(1)
    pass


@asset(
    group_name="FORECASTING", 
    compute_kind="python", 
    code_version="1",
    description="predicted orders for the 30 days",
    owners=["pete@hooli.com","team: Sales Ops"]
    )
def predicted_orders(weekly_order_summary, order_forecast_model):
    time.sleep(1)
    pass

@asset(
    group_name="FORECASTING", 
    compute_kind="databricks", 
    code_version="1",
    description="days where predicted orders surpass 1000",
    owners=["pete@hooli.com","team: Sales Ops"]
    )
def big_orders(predicted_orders):
    time.sleep(1)
    pass

@asset(
    group_name="FORECASTING", 
    compute_kind="scikit learn",
    code_version="1",
    description="model errors by month",
    owners=["pete@hooli.com","team: Sales Ops"]
    )
def model_stats_by_month(weekly_order_summary, order_forecast_model):
    time.sleep(1)
    pass

@asset(
    group_name="FORECASTING", 
    compute_kind="kubernetes",
    code_version="1",
    )
def k8s_pod_asset(predicted_orders):
    time.sleep(1)
    pass

@asset(
    group_name="FORECASTING", 
    compute_kind="jupyter",
    code_version="1",
    description="Backed by a notebook",
    owners=["pete@hooli.com","team: Sales Ops"]
    )
def model_nb(weekly_order_summary, order_forecast_model):
    time.sleep(1)
    pass