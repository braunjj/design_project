import base64
from io import BytesIO
from typing import List
import time
import random


from dagster import asset_check, AssetCheckSpec, AssetCheckResult, AssetCheckSeverity, MetadataValue, asset, DailyPartitionsDefinition, Output, StaticPartitionsDefinition


@asset(group_name="daily_etl", compute_kind="sling")
def accounts_raw():
    """
    Sling asset
    """
    # Generate a random number between 0 and 1
    random_number = random.random()
    
    # Define a probability threshold for failure
    failure_probability = 1  # Adjust this probability as needed

    time.sleep(.5)
    if random_number < failure_probability:
        # Raise an exception to simulate a failure
        raise Exception("Randomly failed!")
    
@asset(group_name="daily_etl", compute_kind="sling")
def opportunities_raw():
    """
    Sling asset
    """
    # Generate a random number between 0 and 1
    random_number = random.random()
    
    # Define a probability threshold for failure
    failure_probability = 0.2  # Adjust this probability as needed

    time.sleep(.5)
    if random_number < failure_probability:
        # Raise an exception to simulate a failure
        raise Exception("Randomly failed!")
    
@asset(group_name="daily_etl", compute_kind="sling")
def task():
    """
    Sling asset
    """
    # Generate a random number between 0 and 1
    random_number = random.random()
    
    # Define a probability threshold for failure
    failure_probability = 0.2  # Adjust this probability as needed

    time.sleep(.5)
    if random_number < failure_probability:
        # Raise an exception to simulate a failure
        raise Exception("Randomly failed!")
    

@asset(group_name="daily_etl", compute_kind="sling")
def userrole():
    """
    Sling asset
    """
    # Generate a random number between 0 and 1
    random_number = random.random()
    
    # Define a probability threshold for failure
    failure_probability = 0.2  # Adjust this probability as needed

    time.sleep(.5)
    if random_number < failure_probability:
        # Raise an exception to simulate a failure
        raise Exception("Randomly failed!")
    
@asset(group_name="daily_etl", compute_kind="sling")
def charges():
    """
    Sling asset
    """
    # Generate a random number between 0 and 1
    random_number = random.random()
    
    # Define a probability threshold for failure
    failure_probability = 0.2  # Adjust this probability as needed

    time.sleep(.5)
    if random_number < failure_probability:
        # Raise an exception to simulate a failure
        raise Exception("Randomly failed!")
    
@asset(group_name="daily_etl", compute_kind="sling")
def balance_transactions():
    """
    Sling asset
    """
    # Generate a random number between 0 and 1
    random_number = random.random()
    
    # Define a probability threshold for failure
    failure_probability = 0.2  # Adjust this probability as needed

    time.sleep(.5)
    if random_number < failure_probability:
        # Raise an exception to simulate a failure
        raise Exception("Randomly failed!")
    

@asset(group_name="daily_etl", compute_kind="sling")
def coupons():
    """
    Sling asset
    """
    # Generate a random number between 0 and 1
    random_number = random.random()
    
    # Define a probability threshold for failure
    failure_probability = 0.2  # Adjust this probability as needed

    time.sleep(.5)
    if random_number < failure_probability:
        # Raise an exception to simulate a failure
        raise Exception("Randomly failed!")
    

@asset(group_name="daily_etl", compute_kind="sling")
def events():
    """
    Sling asset
    """
    # Generate a random number between 0 and 1
    random_number = random.random()
    
    # Define a probability threshold for failure
    failure_probability = 0.2  # Adjust this probability as needed

    time.sleep(.5)
    if random_number < failure_probability:
        # Raise an exception to simulate a failure
        raise Exception("Randomly failed!")
    
@asset(group_name="daily_etl", compute_kind="sling")
def invoices():
    """
    Sling asset
    """
    # Generate a random number between 0 and 1
    random_number = random.random()
    
    # Define a probability threshold for failure
    failure_probability = 0.2  # Adjust this probability as needed

    time.sleep(.5)
    if random_number < failure_probability:
        # Raise an exception to simulate a failure
        raise Exception("Randomly failed!")

@asset(group_name="daily_etl", compute_kind="sling")
def payments():
    """
    Sling asset
    """
    # Generate a random number between 0 and 1
    random_number = random.random()
    
    # Define a probability threshold for failure
    failure_probability = 0.2  # Adjust this probability as needed

    time.sleep(.5)
    if random_number < failure_probability:
        # Raise an exception to simulate a failure
        raise Exception("Randomly failed!")
    
@asset(group_name="daily_etl", compute_kind="sling")
def plans():
    """
    Sling asset
    """
    # Generate a random number between 0 and 1
    random_number = random.random()
    
    # Define a probability threshold for failure
    failure_probability = 0.2  # Adjust this probability as needed

    time.sleep(.5)
    if random_number < failure_probability:
        # Raise an exception to simulate a failure
        raise Exception("Randomly failed!")
    
