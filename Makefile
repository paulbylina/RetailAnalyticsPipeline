load-fact-orders:
	python src/etl/load_fact_orders_to_duckdb.py

create-dim-date:
	python src/etl/create_dim_date.py

database:
	duckdb retail.duckdb

install:
	pip install -r requirements.txt

make show-tables:
	python -c "import duckdb; con = duckdb.connect('data/warehouse/retail.duckdb'); print(con.execute('SHOW TABLES').fetchdf())"

run-kpi-summary-df:
	python -c "import duckdb, pathlib, pandas as pd; \
	pd.set_option('display.max_columns', None); \
	pd.set_option('display.width', 1000); \
	sql = pathlib.Path('sql/analytics/kpi_summary.sql').read_text(); \
	con = duckdb.connect('data/warehouse/retail.duckdb'); \
	df = con.execute(sql).fetchdf(); \
	print(df.to_string(index=False))"

run-kpi-summary:
	python -c "import duckdb, pathlib; \
	sql = pathlib.Path('sql/analytics/kpi_summary.sql').read_text(); \
	con = duckdb.connect('data/warehouse/retail.duckdb'); \
	result = con.execute(sql); print([col[0] for col in result.description]); \
	print(result.fetchall())"