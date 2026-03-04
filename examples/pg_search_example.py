import tempfile
import pgembed
import sqlalchemy as sa
from sqlalchemy_utils import database_exists, create_database


def test_pg_search():
    with tempfile.TemporaryDirectory() as tmpdir:
        with pgembed.get_server(tmpdir) as pg:
            database_name = "testdb"
            uri = pg.get_uri(database_name)

            if not database_exists(uri):
                create_database(uri)

            engine = sa.create_engine(uri, isolation_level="AUTOCOMMIT")
            conn = engine.connect()

            with conn.begin():
                conn.execute(
                    sa.text("""
                    DROP TABLE IF EXISTS items;
                    CREATE TABLE items (
                        id SERIAL PRIMARY KEY,
                        title TEXT,
                        description TEXT,
                        content_search tsvector
                    );
                """)
                )

                data = [
                    ("Python Search", "A guide to searching with Python and Postgres"),
                    ("Postgres Tutorial", "Learn full text search in Postgres"),
                    ("Database Optimization", "Tips for better database performance"),
                ]
                for title, desc in data:
                    conn.execute(
                        sa.text("""
                        INSERT INTO items (title, description, content_search)
                        VALUES (:title, :desc, to_tsvector('english', :title || ' ' || :desc))
                    """),
                        {"title": title, "desc": desc},
                    )

                conn.execute(
                    sa.text(
                        "CREATE INDEX items_search_idx ON items USING GIN(content_search);"
                    )
                )

                search_query = "Python & Postgres"
                print(f"Searching for: '{search_query}'\n")

                result = conn.execute(
                    sa.text("""
                    SELECT title, description
                    FROM items
                    WHERE content_search @@ to_tsquery('english', :query)
                    ORDER BY ts_rank(content_search, to_tsquery('english', :query)) DESC;
                """),
                    {"query": search_query},
                )

                results = result.fetchall()
                for row in results:
                    print(f"Result: {row[0]} - {row[1]}")

            conn.close()


if __name__ == "__main__":
    test_pg_search()
