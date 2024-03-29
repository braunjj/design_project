from dagster import OpExecutionContext, asset, FreshnessPolicy, graph, AutoMaterializePolicy, AutoMaterializeRule, graph_asset, op, AssetSpec 

import time

my_custom_policy = AutoMaterializePolicy.lazy().with_rules(
    AutoMaterializeRule.materialize_on_cron('*/5 * * * *', timezone='EST', all_partitions=False),
)
my_custom_lazy_policy = AutoMaterializePolicy.lazy().without_rules(
    AutoMaterializeRule.skip_on_parent_missing(),
    AutoMaterializeRule.skip_on_parent_outdated(),
    AutoMaterializeRule.skip_on_backfill_in_progress(),
)

my_lazy_policy = AutoMaterializePolicy.lazy()
my_eager_policy = AutoMaterializePolicy.eager()

@asset(
    group_name="RAW_DATA", 
    compute_kind="stripe", 
    code_version="2",
    metadata={"owner": "josh@mycompany.com", "priority": "high", "tags": ["urgent", "something else"]},
    freshness_policy = FreshnessPolicy(maximum_lag_minutes=3),
    auto_materialize_policy = my_custom_policy,
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
    owners=["pete@hooli.com","team: Sales Ops"]
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

@asset(group_name="GRAPH_ASSETS")
def upstream_asset():
    return 1

@op
def add_one(input_num):
    return input_num + 1

@op
def multiply_by_two(input_num):
    return input_num * 2

@op
def add_two(input_num):
    return input_num + 2

@graph_asset(
    group_name="GRAPH_ASSETS",
    metadata={"owner": "josh@dagsterlabs.com"}
)
def middle_asset(upstream_asset):
    return multiply_by_two(add_one(upstream_asset))


@asset(group_name="GRAPH_ASSETS")
def downstream_asset(middle_asset):
    return middle_asset + 7


@op
def return_one_b(context: OpExecutionContext) -> int:
    return 1


@op
def add_one_b(context: OpExecutionContext, number: int) -> int:
    return number + 1


@graph_asset(
    group_name="GRAPH_ASSETS",
    metadata={"owner": "josh@dagsterlabs.com"}
)
def custom_graph_assed(upstream_asset):
    return add_one_b(add_one_b(add_one_b(add_one(upstream_asset))))

raw_logs = AssetSpec("raw_logs", group_name="EXTERNAL")
processed_logs = AssetSpec("processed_logs", deps=[raw_logs], group_name="EXTERNAL")

@asset(
    deps=[processed_logs],
    compute_kind="python",
    description="External asset",
    group_name="EXTERNAL"
)
def aggregated_logs() -> None:
    pass