import base64
from io import BytesIO
from typing import List
import time
import random


from dagster import asset_check, AssetCheckSpec, AssetCheckResult, AssetCheckSeverity, MetadataValue, asset, DailyPartitionsDefinition, Output, StaticPartitionsDefinition

    
@asset(group_name="daily_etl", compute_kind="dbt")
def opportunities_cleaned(opportunities_raw):
    """
    dbt asset
    """
    # Generate a random number between 0 and 1
    random_number = random.random()
    
    # Define a probability threshold for failure
    failure_probability = 0.2  # Adjust this probability as needed

    time.sleep(.5)
    if random_number < failure_probability:
        # Raise an exception to simulate a failure
        raise Exception("Randomly failed!")
    
@asset(group_name="daily_etl", compute_kind="dbt")
def task_cleaned(task):
    """
    dbt asset
    """
    # Generate a random number between 0 and 1
    random_number = random.random()
    
    # Define a probability threshold for failure
    failure_probability = 0.2  # Adjust this probability as needed

    time.sleep(.5)
    if random_number < failure_probability:
        # Raise an exception to simulate a failure
        raise Exception("Randomly failed!")
    

@asset(group_name="daily_etl", compute_kind="dbt")
def userrole_cleaned(userrole):
    """
    dbt asset
    """
    # Generate a random number between 0 and 1
    random_number = random.random()
    
    # Define a probability threshold for failure
    failure_probability = 0.2  # Adjust this probability as needed

    time.sleep(.5)
    if random_number < failure_probability:
        # Raise an exception to simulate a failure
        raise Exception("Randomly failed!")
    
@asset(group_name="daily_etl", compute_kind="dbt")
def charges_cleaned(charges):
    """
    dbt asset
    """
    # Generate a random number between 0 and 1
    random_number = random.random()
    
    # Define a probability threshold for failure
    failure_probability = 0.2  # Adjust this probability as needed

    time.sleep(.5)
    if random_number < failure_probability:
        # Raise an exception to simulate a failure
        raise Exception("Randomly failed!")
    
@asset(group_name="daily_etl", compute_kind="dbt")
def balance_transactions_cleaned(balance_transactions):
    """
    dbt asset
    """
    # Generate a random number between 0 and 1
    random_number = random.random()
    
    # Define a probability threshold for failure
    failure_probability = 0.2  # Adjust this probability as needed

    time.sleep(.5)
    if random_number < failure_probability:
        # Raise an exception to simulate a failure
        raise Exception("Randomly failed!")
    

@asset(group_name="daily_etl", compute_kind="dbt")
def coupons_cleaned(coupons):
    """
    dbt asset
    """
    # Generate a random number between 0 and 1
    random_number = random.random()
    
    # Define a probability threshold for failure
    failure_probability = 0.2  # Adjust this probability as needed

    time.sleep(.5)
    if random_number < failure_probability:
        # Raise an exception to simulate a failure
        raise Exception("Randomly failed!")
    

@asset(group_name="daily_etl", compute_kind="dbt")
def events_cleaned(events):
    """
    dbt asset
    """
    # Generate a random number between 0 and 1
    random_number = random.random()
    
    # Define a probability threshold for failure
    failure_probability = 0.2  # Adjust this probability as needed

    time.sleep(.5)
    if random_number < failure_probability:
        # Raise an exception to simulate a failure
        raise Exception("Randomly failed!")
    
@asset(group_name="daily_etl", compute_kind="dbt")
def invoices_cleaned(invoices):
    """
    dbt asset
    """
    # Generate a random number between 0 and 1
    random_number = random.random()
    
    # Define a probability threshold for failure
    failure_probability = 0.2  # Adjust this probability as needed

    time.sleep(.5)
    if random_number < failure_probability:
        # Raise an exception to simulate a failure
        raise Exception("Randomly failed!")

@asset(group_name="daily_etl", compute_kind="dbt")
def payments_cleaned(payments):
    """
    dbt asset
    """
    # Generate a random number between 0 and 1
    random_number = random.random()
    
    # Define a probability threshold for failure
    failure_probability = 0.2  # Adjust this probability as needed

    time.sleep(.5)
    if random_number < failure_probability:
        # Raise an exception to simulate a failure
        raise Exception("Randomly failed!")
    
@asset(group_name="daily_etl", compute_kind="dbt")
def plans_cleaned(plans):
    """
    dbt asset
    """
    # Generate a random number between 0 and 1
    random_number = random.random()
    
    # Define a probability threshold for failure
    failure_probability = 0.2  # Adjust this probability as needed

    time.sleep(.5)
    if random_number < failure_probability:
        # Raise an exception to simulate a failure
        raise Exception("Randomly failed!")
    
