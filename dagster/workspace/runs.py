from dagster import job, op

@op
def hello():
    print("Hello from Docker!")

@job
def my_first_job():
    hello()