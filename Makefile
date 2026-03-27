create-fact-orders-table:
	python src/etl/create_fact_orders_table.py

create-dim-date-table:
	python src/etl/create_dim_date_table.py

create-dim-customers-table:
	python src/etl/create_dim_customers_table.py

database:
	duckdb retail.duckdb

install:
	pip install -r requirements.txt

make show-tables:
	python -c "import duckdb; con = duckdb.connect('data/warehouse/retail.duckdb'); print(con.execute('SHOW TABLES').fetchdf())"

show-all-columns:
	python -c "import duckdb; \
	con = duckdb.connect('data/warehouse/retail.duckdb'); \
	print(con.execute('DESCRIBE fact_orders').fetchdf().to_string(index=False))"

kpi-summary-df:
	python -c "import duckdb, pathlib, pandas as pd; \
	pd.set_option('display.max_columns', None); pd.set_option('display.width', 1000); \
	sql = pathlib.Path('sql/analytics/kpi_summary.sql').read_text(); \
	con = duckdb.connect('data/warehouse/retail.duckdb'); \
	df = con.execute(sql).fetchdf(); \
	print(df)"

revenue-by-region-df:
	python -c "import duckdb, pathlib, pandas as pd; \
	pd.set_option('display.max_columns', None); pd.set_option('display.width', 1000); \
	sql = pathlib.Path('sql/analytics/revenue_by_region.sql').read_text(); \
	con = duckdb.connect('data/warehouse/retail.duckdb'); \
	df = con.execute(sql).fetchdf(); \
	print(df)"

top-categories-df:
	python -c "import duckdb, pathlib, pandas as pd; \
	pd.set_option('display.max_columns', None); pd.set_option('display.width', 1000); \
	sql = pathlib.Path('sql/analytics/top_categories.sql').read_text(); \
	con = duckdb.connect('data/warehouse/retail.duckdb'); \
	df = con.execute(sql).fetchdf(); \
	print(df)"

orders-by-status:
	python -c "import duckdb, pathlib, pandas as pd; \
	pd.set_option('display.max_columns', None); pd.set_option('display.width', 1000); \
	sql = pathlib.Path('sql/analytics/orders_by_status.sql').read_text(); \
	con = duckdb.connect('data/warehouse/retail.duckdb'); \
	df = con.execute(sql).fetchdf(); \
	print(df)"

daily-revue-trend:
	python -c "import duckdb, pathlib, pandas as pd; \
	sql_file = 'daily_revenue_trend.sql'; \
	pd.set_option('display.max_columns', None); pd.set_option('display.width', 1000); \
	sql = pathlib.Path(f'sql/analytics/{sql_file}').read_text(); \
	con = duckdb.connect('data/warehouse/retail.duckdb'); \
	df = con.execute(sql).fetchdf(); \
	print(df)"

revenue-by-customer-segment:
	python -c "import duckdb, pathlib, pandas as pd; \
	sql_file = 'revenue_by_customer_segment' + '.sql'; \
	pd.set_option('display.max_columns', None); pd.set_option('display.width', 1000); \
	sql = pathlib.Path(f'sql/analytics/{sql_file}').read_text(); \
	con = duckdb.connect('data/warehouse/retail.duckdb'); \
	df = con.execute(sql).fetchdf(); \
	print(df)"

revenue-by-weekday:
	python -c "import duckdb, pathlib, pandas as pd; \
	sql_file = 'revenue_by_weekday' + '.sql'; \
	pd.set_option('display.max_columns', None); pd.set_option('display.width', 1000); \
	sql = pathlib.Path(f'sql/analytics/{sql_file}').read_text(); \
	con = duckdb.connect('data/warehouse/retail.duckdb'); \
	df = con.execute(sql).fetchdf(); \
	print(df)"