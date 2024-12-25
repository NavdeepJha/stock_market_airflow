from airflow.decorators import dag, task
from datetime import datetime
import random

 
@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    description='A simple DAG to generate and check random numbers',
    catchup=False
)
def random_number_checker():
 
    @task
    def generate_task():
        number = random.randint(1, 100)
        print(f"Generated random number: {number}")
        return number
        
    
    @task
    def check_task(value):
        number = value
        result = "even" if number % 2 == 0 else "odd"
        print(f"The number {number} is {result}.")
    
    check_task(generate_task())
 
random_number_checker()