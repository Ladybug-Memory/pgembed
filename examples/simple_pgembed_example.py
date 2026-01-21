import tempfile
import pgembed
import sqlalchemy as sa
from sqlalchemy_utils import database_exists, create_database

with tempfile.TemporaryDirectory() as tmpdir:
    with pgembed.get_server(tmpdir) as pg:
        database_name = 'testdb'
        uri = pg.get_uri(database_name)

        if not database_exists(uri):
            create_database(uri)

        engine = sa.create_engine(uri, isolation_level='AUTOCOMMIT')
        conn = engine.connect()

        with conn.begin():
            conn.execute(sa.text("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    email VARCHAR(100) NOT NULL
                )
            """))

            conn.execute(sa.text("""
                INSERT INTO users (name, email) VALUES
                    ('Alice', 'alice@example.com'),
                    ('Bob', 'bob@example.com'),
                    ('Charlie', 'charlie@example.com'),
                    ('Diana', 'diana@example.com'),
                    ('Eve', 'eve@example.com')
            """))

        result = conn.execute(sa.text("SELECT id, name, email FROM users ORDER BY id"))
        rows = result.fetchall()

        print("Users:")
        for row in rows:
            print(f"  {row.id}: {row.name} ({row.email})")

        conn.close()
