# ----------------------
# Create Tables
# ----------------------

create-fact-orders-table:
	@python src/etl/create_fact_orders_table.py

create-dim-date-table:
	@python src/etl/create_dim_date_table.py

create-dim-customers-table:
	@python src/etl/create_dim_customers_table.py

# ----------------------
# Launch full pipeline
# ----------------------	

full-pipeline:
	@python src/ingestion/generate_retail_data.py
	@python src/etl/transform_retail_orders.py
	@python src/etl/validate_retail_orders.py
	@python src/etl/aggregate_retail_orders.py
	@python src/etl/create_fact_orders_table.py
	@python src/etl/create_dim_date_table.py
	@python src/etl/create_dim_customers_table.py

# ----------------------
# Show all tables in database
# ----------------------	

show-all-tables:
	@python -c "import duckdb; \
	pd.set_option('display.max_columns', None); pd.set_option('display.width', 1000); \
	con = duckdb.connect('data/warehouse/retail.duckdb'); \
	print(con.execute('SHOW TABLES').fetchdf())"

# ----------------------
# Install dependencies
# ----------------------

install:
	@pip install -r requirements.txt

# ----------------------
# Show table as pandas DataFrame
# ----------------------

show-data-fact-orders:
	@python -c "import duckdb, pandas as pd; \
	pd.set_option('display.max_columns', None); pd.set_option('display.width', 1000); \
	table = 'fact_orders'; \
	con = duckdb.connect('data/warehouse/retail.duckdb'); \
	print(con.execute(f'SELECT * FROM {table} ORDER BY order_date').fetchdf())"

show-data-dim-date:
	@python -c "import duckdb, pandas as pd; \
	pd.set_option('display.max_columns', None); pd.set_option('display.width', 1000); \
	table = 'dim_date'; \
	con = duckdb.connect('data/warehouse/retail.duckdb'); \
	print(con.execute(f'SELECT * FROM {table} ORDER BY full_date').fetchdf())"

show-data-dim-customers:
	@python -c "import duckdb, pandas as pd; \
	pd.set_option('display.max_columns', None); pd.set_option('display.width', 1000); \
	table = 'dim_customers'; \
	con = duckdb.connect('data/warehouse/retail.duckdb'); \
	print(con.execute(f'SELECT * FROM {table} ORDER BY customer_id').fetchdf())"

# ----------------------
# Table info/Datatypes
# ----------------------

show-fact-orders-datatypes:
	@python -c "import duckdb; \
	pd.set_option('display.max_columns', None); pd.set_option('display.width', 1000); \
	con = duckdb.connect('data/warehouse/retail.duckdb'); \
	table = 'fact_orders'; \
	print(f'\n\t\t\t{table}'.upper()); \
	print(con.execute(f'DESCRIBE {table}').fetchdf().to_string(index=False))"

show-dim-customers-datatypes:
	@python -c "import duckdb; \
	pd.set_option('display.max_columns', None); pd.set_option('display.width', 1000); \
	con = duckdb.connect('data/warehouse/retail.duckdb'); \
	table = 'dim_customers'; \
	print(f'\n\t\t\t{table}'.upper()); \
	print(con.execute(f'DESCRIBE {table}').fetchdf().to_string(index=False))"

show-dim-date-datatypes:
	@python -c "import duckdb; \
	pd.set_option('display.max_columns', None); pd.set_option('display.width', 1000); \
	con = duckdb.connect('data/warehouse/retail.duckdb'); \
	table = 'dim_date'; \
	print(f'\n\t\t\t{table}'.upper()); \
	print(con.execute(f'DESCRIBE {table}').fetchdf().to_string(index=False))"

# ----------------------
# Analytics Queries
# ----------------------

kpi-summary-df:
	@python -c "import duckdb, pathlib, pandas as pd; \
	pd.set_option('display.max_columns', None); pd.set_option('display.width', 1000); \
	sql = pathlib.Path('sql/analytics/kpi_summary.sql').read_text(); \
	con = duckdb.connect('data/warehouse/retail.duckdb'); \
	df = con.execute(sql).fetchdf(); \
	print(df)"

revenue-by-region-df:
	@python -c "import duckdb, pathlib, pandas as pd; \
	pd.set_option('display.max_columns', None); pd.set_option('display.width', 1000); \
	sql = pathlib.Path('sql/analytics/revenue_by_region.sql').read_text(); \
	con = duckdb.connect('data/warehouse/retail.duckdb'); \
	df = con.execute(sql).fetchdf(); \
	print(df)"

top-categories-df:
	@python -c "import duckdb, pathlib, pandas as pd; \
	pd.set_option('display.max_columns', None); pd.set_option('display.width', 1000); \
	sql = pathlib.Path('sql/analytics/top_categories.sql').read_text(); \
	con = duckdb.connect('data/warehouse/retail.duckdb'); \
	df = con.execute(sql).fetchdf(); \
	print(df)"

orders-by-status:
	@python -c "import duckdb, pathlib, pandas as pd; \
	pd.set_option('display.max_columns', None); pd.set_option('display.width', 1000); \
	sql = pathlib.Path('sql/analytics/orders_by_status.sql').read_text(); \
	con = duckdb.connect('data/warehouse/retail.duckdb'); \
	df = con.execute(sql).fetchdf(); \
	print(df)"

daily-reveue-trend:
	@python -c "import duckdb, pathlib, pandas as pd; \
	sql_file = 'daily_revenue_trend.sql'; \
	pd.set_option('display.max_columns', None); pd.set_option('display.width', 1000); \
	sql = pathlib.Path(f'sql/analytics/{sql_file}').read_text(); \
	con = duckdb.connect('data/warehouse/retail.duckdb'); \
	df = con.execute(sql).fetchdf(); \
	print(df)"

revenue-by-customer-segment:
	@python -c "import duckdb, pathlib, pandas as pd; \
	sql_file = 'revenue_by_customer_segment' + '.sql'; \
	pd.set_option('display.max_columns', None); pd.set_option('display.width', 1000); \
	sql = pathlib.Path(f'sql/analytics/{sql_file}').read_text(); \
	con = duckdb.connect('data/warehouse/retail.duckdb'); \
	df = con.execute(sql).fetchdf(); \
	print(df)"

revenue-by-weekday:
	@python -c "import duckdb, pathlib, pandas as pd; \
	sql_file = 'revenue_by_weekday' + '.sql'; \
	pd.set_option('display.max_columns', None); pd.set_option('display.width', 1000); \
	sql = pathlib.Path(f'sql/analytics/{sql_file}').read_text(); \
	con = duckdb.connect('data/warehouse/retail.duckdb'); \
	df = con.execute(sql).fetchdf(); \
	print(df)"

# ----------------------
# Docker
# ----------------------

docker-run:
	@docker run -p 8501:8501 retail-analytics-pipeline

# ----------------------
# Airflow
# ----------------------

airflow-up:
	@docker compose -f docker-compose.airflow.yml up

airflow-down:
	@docker compose -f docker-compose.airflow.yml down

# ----------------------
# Streamlit
# ----------------------

streamlit:
	@streamlit run src/dashboard/app.py

# ----------------------
# Help
# ----------------------

help:
	@echo "Available targets:"
	@echo ""
	@echo "Setup"
	@echo "  install                      Install dependencies"
	@echo ""
	@echo "Warehouse"
	@echo "  create-fact-orders-table     Build fact_orders table"
	@echo "  create-dim-date-table        Build dim_date table"
	@echo "  create-dim-customers-table   Build dim_customers table"
	@echo "  show-all-tables              Show all DuckDB tables"
	@echo "  show-data-fact-orders        Show fact_orders data"
	@echo "  show-data-dim-date           Show dim_date data"
	@echo "  show-data-dim-customers      Show dim_customers data"
	@echo "  full-pipeline                Run the full retail data pipeline"
	@echo ""
	@echo "Analytics"
	@echo "  kpi-summary-df               Run KPI summary query"
	@echo "  revenue-by-region-df         Run revenue by region query"
	@echo "  top-categories-df            Run top categories query"
	@echo "  orders-by-status             Run orders by status query"
	@echo "  daily-revenue-trend          Run daily revenue trend query"
	@echo "  revenue-by-customer-segment  Run revenue by customer segment query"
	@echo "  revenue-by-weekday           Run revenue by weekday query"
	@echo ""
	@echo "Apps"
	@echo "  streamlit                    Launch Streamlit dashboard"
	@echo "  docker-run                   Run dashboard with Docker Compose"
	@echo "  airflow-up                   Start Airflow"
	@echo "  airflow-down                 Stop Airflow"
