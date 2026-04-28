import pytest
from aileron_meta_collector.parsers.sql_parser import extract_tables


class TestSelectStatements:
    def test_simple_select(self):
        inputs, outputs = extract_tables("SELECT * FROM orders")
        assert inputs == ["orders"]
        assert outputs == []

    def test_select_with_join(self):
        sql = "SELECT o.id, u.name FROM orders o JOIN users u ON o.user_id = u.id"
        inputs, outputs = extract_tables(sql)
        assert "orders" in inputs
        assert "users" in inputs
        assert outputs == []

    def test_select_with_schema(self):
        inputs, outputs = extract_tables("SELECT * FROM public.orders")
        assert "public.orders" in inputs
        assert outputs == []

    def test_select_with_subquery(self):
        sql = "SELECT * FROM (SELECT id FROM orders WHERE status = 'active') AS sub"
        inputs, outputs = extract_tables(sql)
        assert outputs == []


class TestWriteStatements:
    def test_insert_into(self):
        sql = "INSERT INTO processed_orders SELECT * FROM orders WHERE status = 'done'"
        inputs, outputs = extract_tables(sql)
        assert "processed_orders" in outputs
        assert "orders" in inputs

    def test_update(self):
        sql = "UPDATE users SET status = 'active' WHERE id = 1"
        inputs, outputs = extract_tables(sql)
        assert "users" in outputs

    def test_create_table_as_select(self):
        sql = "CREATE TABLE summary AS SELECT user_id, COUNT(*) FROM orders GROUP BY user_id"
        inputs, outputs = extract_tables(sql)
        assert "summary" in outputs
        assert "orders" in inputs


class TestUnloadStatements:
    def test_unload_basic(self):
        sql = """
        UNLOAD (SELECT * FROM orders WHERE status = 'done')
        TO 's3://processed-bucket/output/orders/'
        WITH (format = 'PARQUET')
        """
        inputs, outputs = extract_tables(sql)
        assert "orders" in inputs
        assert len(outputs) == 1
        assert outputs[0] == "__s3__processed-bucket/output/orders"

    def test_unload_with_join(self):
        sql = """
        UNLOAD (SELECT o.id, u.name FROM orders o JOIN users u ON o.user_id = u.id)
        TO 's3://bucket/result/'
        WITH (format = 'TEXTFILE')
        """
        inputs, outputs = extract_tables(sql)
        assert "orders" in inputs
        assert "users" in inputs
        assert "__s3__bucket/result" in outputs

    def test_unload_with_schema(self):
        sql = """
        UNLOAD (SELECT * FROM prod.transactions)
        TO 's3://data-lake/transactions/'
        WITH (format = 'ORC')
        """
        inputs, outputs = extract_tables(sql)
        assert "prod.transactions" in inputs
        assert "__s3__data-lake/transactions" in outputs


class TestCreateViewStatements:
    def test_create_view_simple(self):
        sql = "CREATE VIEW order_view AS SELECT * FROM orders"
        inputs, outputs = extract_tables(sql)
        assert "order_view" in outputs
        assert "orders" in inputs

    def test_create_or_replace_view(self):
        sql = "CREATE OR REPLACE VIEW sales_db.daily_summary AS SELECT order_date, SUM(amount) FROM sales_db.orders GROUP BY order_date"
        inputs, outputs = extract_tables(sql)
        assert "sales_db.daily_summary" in outputs
        assert "sales_db.orders" in inputs

    def test_create_view_with_schema(self):
        sql = "CREATE VIEW analytics.report AS SELECT * FROM orders"
        inputs, outputs = extract_tables(sql)
        assert "analytics.report" in outputs
        assert "orders" in inputs

    def test_create_temp_view(self):
        sql = "CREATE TEMP VIEW tmp_v AS SELECT a.id, b.name FROM tableA a JOIN tableB b ON a.id = b.id"
        inputs, outputs = extract_tables(sql)
        assert "tmp_v" in outputs
        assert "tablea" in inputs
        assert "tableb" in inputs

    def test_create_or_replace_view_multi_join(self):
        sql = "CREATE OR REPLACE VIEW analytics.report AS SELECT o.id, c.name FROM orders o LEFT JOIN customers c ON o.cid = c.id"
        inputs, outputs = extract_tables(sql)
        assert "analytics.report" in outputs
        assert "orders" in inputs
        assert "customers" in inputs


class TestEdgeCases:
    def test_empty_sql(self):
        inputs, outputs = extract_tables("")
        assert inputs == []
        assert outputs == []

    def test_information_schema_excluded(self):
        sql = "SELECT * FROM information_schema.tables"
        inputs, outputs = extract_tables(sql)
        assert inputs == []

    def test_multiple_joins(self):
        sql = """
        SELECT o.id, u.name, p.title
        FROM orders o
        JOIN users u ON o.user_id = u.id
        LEFT JOIN products p ON o.product_id = p.id
        """
        inputs, outputs = extract_tables(sql)
        assert "orders" in inputs
        assert "users" in inputs
        assert "products" in inputs
        assert outputs == []
