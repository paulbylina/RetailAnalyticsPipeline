load-fact-orders:
	python src/etl/load_fact_orders_to_duckdb.py

create-dim-date:
	python src/etl/create_dim_date.py

database:
	duckdb retail.duckdb

requirements:
	pip install -r requirements.txt

make show-tables:
	python -c "import duckdb; con = duckdb.connect('data/warehouse/retail.duckdb'); print(con.execute('SHOW TABLES').fetchall())"