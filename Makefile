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
# Show all tables in database
# ----------------------	

show-all-tables:
	@python -c "import duckdb; \
	pd.set_option('display.max_columns', None); pd.set_option('display.width', 1000); \
	con = duckdb.connect('data/warehouse/retail.duckdb'); \
	print(con.execute('SHOW TABLES').fetchdf())"

# ----------------------
# Dashboard
# ----------------------

streamlit:
	@streamlit run src/dashboard/app.py

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

docker-build:
	@docker build -t retail-analytics-pipeline .

docker-run:
	@docker run -p 8501:8501 retail-analytics-pipeline