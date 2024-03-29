import base64
from io import BytesIO
from typing import List
import time
import random


from dagster import (
    asset_check, 
    BackfillPolicy, 
    AssetCheckSpec, 
    AssetCheckResult, 
    AssetCheckSeverity, 
    MetadataValue, 
    asset, 
    DailyPartitionsDefinition, 
    Output, 
    StaticPartitionsDefinition,
)

CONTINENTS = [
    "Africa",
    "Antarctica",
    "Asia",
    "Europe",
    "North America",
    "Oceania",
    "South America",
]

failure_probability_int = 0  # Adjust this probability as needed

@asset(group_name="bronze", compute_kind="sling", owners=["josh@hooli.com", "pete@hooli.com"])
def members_raw():
    """
    Randomly fails
    """
    # Generate a random number between 0 and 1
    random_number = random.random()
    
    # Define a probability threshold for failure
    failure_probability = failure_probability_int  # Adjust this probability as needed

    time.sleep(.5)
    if random_number < failure_probability:
        # Raise an exception to simulate a failure
        raise Exception("Randomly failed!")
    

@asset(group_name="silver", compute_kind="dbt", code_version="2", owners=["josh@hooli.com"])
def reservations_cleaned(members_raw):
    time.sleep(1)
    pass

@asset(group_name="silver", compute_kind="dbt", code_version="1", owners=["josh@hooli.com"])
def members_cleaned(reservations_raw, members_raw):
    time.sleep(1)
    pass

@asset(group_name="gold", owners=["nick@hooli.com"], compute_kind="python", code_version="1", check_specs=[AssetCheckSpec(name="orders_id_has_no_nulls", asset="reservations_prod")])
def reservations_prod(reservations_cleaned):
    """
    Production Reservations
    """
    time.sleep(2.5)
    yield Output(value=None)
    yield AssetCheckResult(
        passed=True,
    )

@asset(group_name="gold", compute_kind="python", code_version="1", check_specs=[AssetCheckSpec(name="orders_id_has_no_nulls", asset="members_prod")])
def members_prod(members_cleaned):
    """
    Production members table
    """
    time.sleep(1)
    yield Output(value=None)
    yield AssetCheckResult(
        passed=True,
    )

@asset(group_name="daily_etl", compute_kind="sling", owners=["team: Finance", "team: Customer Success"])
def transactions():
    """
    From the transactions table
    """
    time.sleep(2.5)
    pass

@asset(group_name="daily_etl", compute_kind="sling")
def subscriptions():
    """
    From the users table
    """
    time.sleep(3)
    pass

@asset(group_name="daily_etl", compute_kind="sling")
def accounts():
    # Generate a random number between 0 and 1
    random_number = random.random()
    
    # Define a probability threshold for failure
    failure_probability = failure_probability_int  # Adjust this probability as needed

    time.sleep(.5)
    if random_number < failure_probability:
        # Raise an exception to simulate a failure
        raise Exception("Randomly failed!")

@asset(
    group_name="customer_feedback", 
    compute_kind="slack", 
    partitions_def=DailyPartitionsDefinition(start_date="2022-07-01"),
    backfill_policy = BackfillPolicy.multi_run(max_partitions_per_run=10)
) 
def daily_slack_summary():
    """
    Customer sentiment analysis
    """
    time.sleep(2)
    num = random.randint(25, 100)
    return Output(num, 
        metadata={
            "num_rows": num,
            "total_notes": 18,
            "last_edited": "Dec 13, 2022",
            "url": MetadataValue.url("https://docs.google.com/spreadsheets/d/1Q2t34UOjA3CGKBOImcpr-GqXd0uGp7chSh6xOh1XNH4/edit?usp=sharing")
        }
    )
@asset(group_name="customer_feedback", compute_kind="fivetran", partitions_def=DailyPartitionsDefinition(start_date="2022-07-01"))
def raw_reviews():
    """
    Raw customer review data
    """
    time.sleep(1)
    pass

@asset(group_name="customer_feedback", compute_kind="python", partitions_def=DailyPartitionsDefinition(start_date="2022-07-01")) 
def customer_sentiment(raw_reviews, daily_slack_summary):
    """
    Customer sentiment analysis
    """
    time.sleep(2)
    num = random.randint(25, 100)
    return Output(num, 
        metadata={
            "num_rows": num,
            "total_notes": 18,
            "last_edited": "Dec 13, 2022",
            "url": MetadataValue.url("https://docs.google.com/spreadsheets/d/1Q2t34UOjA3CGKBOImcpr-GqXd0uGp7chSh6xOh1XNH4/edit?usp=sharing")
        }
    )

@asset(group_name="daily_etl", compute_kind="dbt")
def transactions_cleaned(transactions):
    time.sleep(2.5)
    pass

@asset(group_name="daily_etl", compute_kind="dbt")
def subscriptions_cleaned(subscriptions):
    time.sleep(3)
    pass

@asset(group_name="daily_etl", compute_kind="dbt", code_version="2")
def accounts_cleaned(accounts):
    time.sleep(3)
    pass

@asset(group_name="daily_etl", compute_kind="python")
def user_metrics(transactions_cleaned, accounts_cleaned, subscriptions_cleaned):
    """
    Do some transformations
    """
    time.sleep(3)
    pass

@asset(group_name="demographics", description="Population data on all countries", compute_kind="dbt")
def country_stats():
    time.sleep(2.5)
    pass

@asset(group_name="demographics", description="Regression of pop change against continent", compute_kind="dbt")
def change_model(country_stats):
    time.sleep(2.5)
    pass

@asset(group_name="demographics", description="Statistics for each continent", compute_kind="dbt")
def continent_stats(country_stats, change_model):
    time.sleep(2.5)
    pass


@asset(group_name="orders", compute_kind="fivetran", description="Raw order data")
def raw_orders():
    time.sleep(2.5)
    pass

@asset(group_name="orders", compute_kind="sling", description="Raw user data")
def raw_users():
    time.sleep(2.5)
    pass

@asset(group_name="orders", compute_kind="dbt", description="Filtered version of the raw data")
def orders_clean(raw_orders):
    time.sleep(2.5)
    pass

@asset(group_name="orders", compute_kind="dbt", description="Raw users data augmented with backend ...")
def users_clean(raw_users):
    time.sleep(2.5)
    pass

@asset(group_name="orders", compute_kind="dbt", description="Daily metrics for orders placed on this pla...")
def daily_orders_summary(orders_clean, users_clean):
    time.sleep(2.5)
    pass

@asset(group_name="machine_learning_really_long_name", compute_kind="python", description="Model parameters that best fit the observ...")
def orders_forecast_model(daily_orders_summary):
    time.sleep(2.5)
    pass


@asset(group_name="reporting", compute_kind="metabase", description="Model parameters that best fit the observ...")
def weekly_orders(daily_orders_summary, user_metrics, accounts_cleaned):
    time.sleep(1.1)
    pass

@asset(group_name="reporting", compute_kind="metabase", description="Model parameters that best fit the observ...")
def churn_risk(payments_cleaned, invoices_cleaned, events_cleaned, charges_cleaned, user_metrics, accounts_cleaned):
    time.sleep(1.8)
    pass

@asset(group_name="reporting", compute_kind="metabase", description="Model parameters that best fit the observ...")
def sales_pipelien(plans_cleaned, task_cleaned, accounts_cleaned, opportunities_cleaned):
    time.sleep(2)
    pass

@asset(group_name="reporting", compute_kind="metabase", description="Model parameters that best fit the observ...")
def cohort_insights(userrole_cleaned, user_metrics, task_cleaned, plans_cleaned, balance_transactions_cleaned):
    time.sleep(2.5)
    pass


@asset(group_name="tags", compute_kind="fivetran")
def tag_airflow():
    time.sleep(1)
    pass

@asset(group_name="tags", compute_kind="airtable")
def tag_airtable(tag_airflow):
    time.sleep(1)
    pass

@asset(group_name="tags", compute_kind="fivetran")
def tag_omni(tag_airtable):
    time.sleep(1)
    pass

@asset(group_name="tags", compute_kind="fivetran")
def tag_catboost():
    time.sleep(1)
    pass

@asset(group_name="tags", compute_kind="python")
def tag_rust(tag_catboost):
    time.sleep(1)
    pass

@asset(group_name="tags", compute_kind="fivetran")
def tag_py_lightning(tag_rust):
    time.sleep(1)
    pass

@asset(group_name="tags", compute_kind="fivetran")
def tag_delta_lake(tag_omni):
    time.sleep(1)
    pass

@asset(group_name="tags", compute_kind="stripe")
def tag_parquet(tag_py_lightning):
    time.sleep(1)
    pass

@asset(group_name="tags", compute_kind="slack")
def tag_slack(tag_parquet):
    time.sleep(1)
    pass

@asset(group_name="tags", compute_kind="slack")
def tag_xgboost():
    time.sleep(1)
    pass

@asset(group_name="tags", compute_kind="slack")
def tag_lightgbm(tag_xgboost):
    time.sleep(1)
    pass

@asset(group_name="tags", compute_kind="slack")
def tag_optuna(tag_lightgbm):
    time.sleep(1)
    pass

@asset(group_name="tags", compute_kind="airtable")
def tag_jax(tag_optuna):
    time.sleep(1)
    pass

@asset(group_name="tags", compute_kind="fivetran")
def tag_ray():
    time.sleep(1)
    pass

@asset(group_name="tags", compute_kind="fivetran")
def tag_snowpark(tag_ray):
    time.sleep(1)
    pass

@asset(group_name="tags", compute_kind="fivetran")
def tag_datadog(tag_snowpark):
    time.sleep(1)
    pass

@asset(group_name="tags", compute_kind="fivetran")
def tag_excel(tag_datadog):
    time.sleep(1)
    pass

@asset(group_name="tags", compute_kind="fivetran")
def tag_chalk(tag_excel):
    time.sleep(1)
    pass

@asset(group_name="tags", compute_kind="fivetran")
def tag_axioma():
    time.sleep(1)
    pass

@asset(group_name="tags", compute_kind="fivetran")
def tag_cube(tag_axioma):
    time.sleep(1)
    pass

@asset(group_name="tags", compute_kind="metabase")
def tag_metabase(tag_cube):
    time.sleep(1)
    pass

@asset(group_name="tags", compute_kind="metabase")
def tag_linear(tag_metabase):
    time.sleep(1)
    pass

@asset(group_name="tags", compute_kind="metabase")
def tag_notion(tag_linear):
    time.sleep(1)
    pass

@asset(group_name="tags", compute_kind="hackernews api")
def tag_hackernews():
    time.sleep(1)
    pass

@asset(group_name="tags", compute_kind="metabase")
def tag_tecton(tag_hackernews):
    time.sleep(1)
    pass

@asset(group_name="tags", compute_kind="metabase")
def tag_dask(tag_tecton):
    time.sleep(1)
    pass

@asset(group_name="tags", compute_kind="metabase")
def tag_dlt(tag_dask):
    time.sleep(1)
    pass

@asset(group_name="tags", compute_kind="metabase")
def tag_dlthub(tag_dlt):
    time.sleep(1)
    pass

@asset(group_name="tags", compute_kind="python")
def tag_huggingface(tag_delta_lake):
    time.sleep(1)
    pass
