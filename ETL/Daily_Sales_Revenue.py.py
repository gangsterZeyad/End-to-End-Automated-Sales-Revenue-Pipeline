from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
import matplotlib.pyplot as plt

# PostgreSQL connection ID
PG_CONN_ID = 'postgres_conn'

# Parameters of Airflow
default_args = {
    'owner': 'kiwilytics',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# -----------------------------------------
# Extract Sales Data
# -----------------------------------------
def extract_sales_data():
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    conn = hook.get_conn()

    query = """
        SELECT 
            o.orderdate::date AS sale_date,
            od.productid,
            p.productname,
            od.quantity,
            p.price
        FROM orders o
        JOIN order_details od ON o.orderid = od.orderid
        JOIN products p ON od.productid = p.productid
    """

    df = pd.read_sql(query, conn)
    df.to_csv('/home/kiwilytics/airflow/dags/daily_sales_data.csv', index=False)

# -----------------------------------------
#  Calculate daily total revenue
# -----------------------------------------
def calc_daily_total_rev():
    df = pd.read_csv('/home/kiwilytics/airflow/dags/daily_sales_data.csv')

    df['total_revenue'] = df['quantity'] * df['price']

    rev_per_day = (
        df.groupby('sale_date')
          .agg(total_revenue=('total_revenue', 'sum'))
          .reset_index()
    )

    rev_per_day.to_csv('/home/kiwilytics/airflow/dags/revenue_per_day.csv', index=False)

# -----------------------------------------
#  Plot the revenue per day
# -----------------------------------------
def plt_daily_rev():
    df = pd.read_csv('/home/kiwilytics/airflow/dags/revenue_per_day.csv')
    df['sale_date'] = pd.to_datetime(df['sale_date'])

    plt.figure(figsize=(12, 6))
    plt.plot(df['sale_date'], df['total_revenue'], marker='o', linestyle='-')
    plt.title("Daily Total Sales Revenue")
    plt.xlabel("Date")
    plt.ylabel("Total Revenue")
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()

    output_path = '/home/kiwilytics/airflow_output/daily_revenue_plot.png'
    plt.savefig(output_path)
    print(f"Revenue chart saved to {output_path}")
    

# -----------------------------------------
#  DAG Definition
# -----------------------------------------
with DAG(
    dag_id='Daily_Sales_Revenue',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False,
    description='Compute and visualize daily revenue using Pandas and Matplotlib in Airflow',
) as dag:

    extract_data = PythonOperator(
        task_id='extract_sales_data',
        python_callable=extract_sales_data
    )

    daily_total_rev = PythonOperator(
        task_id='calc_daily_total_rev',
        python_callable=calc_daily_total_rev
    )

    plot_rev = PythonOperator(
        task_id='plot_daily_revenue',
        python_callable=plt_daily_rev
    )

    # Task pipeline
    extract_data >> daily_total_rev >> plot_rev
