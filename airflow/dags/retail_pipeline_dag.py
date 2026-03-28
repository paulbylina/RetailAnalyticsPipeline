from datetime import datetime
from pathlib import Path
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ROOT = "/opt/airflow/project"

with DAG(
    dag_id="retail_analytics_pipeline",
    start_date=datetime(2026, 3, 28),
    schedule="@daily",
    catchup=False,
    tags=["retail", "data-engineering"],
) as dag:
    # 1 - Generate Data
    generate_retail_data = BashOperator(
        task_id="generate_retail_data",
        bash_command=f"cd {PROJECT_ROOT} && python src/ingestion/generate_retail_data.py",
    )
    # 2 - Transform Data
    transform_retail_orders = BashOperator(
        task_id="transform_retail_orders",
        bash_command=f"cd {PROJECT_ROOT} && python src/etl/transform_retail_orders.py",
    )

    # 3 - Validate Data
    validate_retail_orders = BashOperator(
        task_id="validate_retail_orders",
        bash_command=f"cd {PROJECT_ROOT} && python src/etl/validate_retail_orders.py",
    )

    # 4 - Aggregate Data
    aggregate_retail_orders = BashOperator(
        task_id="aggregate_retail_orders",
        bash_command=f"cd {PROJECT_ROOT} && python src/etl/aggregate_retail_orders.py",
    )

    # 5 - Create fact orders table
    create_fact_orders_table = BashOperator(
        task_id="create_fact_orders_table",
        bash_command=f"cd {PROJECT_ROOT} && python src/etl/create_fact_orders_table.py",
    )

    # 6 - Create dim date table
    create_dim_date_table = BashOperator(
        task_id="create_dim_date_table",
        bash_command=f"cd {PROJECT_ROOT} && python src/etl/create_dim_date_table.py",
    )

    # 7 - Create dim customers table
    create_dim_customers_table = BashOperator(
        task_id="create_dim_customers_table",
        bash_command=f"cd {PROJECT_ROOT} && python src/etl/create_dim_customers_table.py",
    )
   
    # Dependency task order
    (
        generate_retail_data >>
        transform_retail_orders >>
        validate_retail_orders >>
        aggregate_retail_orders >>
        create_fact_orders_table >>
        create_dim_date_table >>
        create_dim_customers_table
    )