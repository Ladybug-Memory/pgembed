import tempfile
import random
import pgembed
import sqlalchemy as sa
from sqlalchemy_utils import database_exists, create_database

DIM = 3

with tempfile.TemporaryDirectory() as tmpdir:
    with pgembed.get_server(tmpdir) as pg:
        database_name = 'testdb'
        uri = pg.get_uri(database_name)

        if not database_exists(uri):
            create_database(uri)

        engine = sa.create_engine(uri, isolation_level='AUTOCOMMIT')
        conn = engine.connect()

        with conn.begin():
            conn.execute(sa.text("CREATE EXTENSION IF NOT EXISTS vector"))
            conn.execute(sa.text("""
                CREATE TABLE IF NOT EXISTS embeddings (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100),
                    vector VECTOR(:dim)
                )
            """), {"dim": DIM})

        with conn.begin():
            for i in range(100):
                vector = [random.uniform(-1, 1) for _ in range(DIM)]
                conn.execute(
                    sa.text("INSERT INTO embeddings (name, vector) VALUES (:name, :vector)"),
                    {"name": f"item_{i}", "vector": vector}
                )

        query_vector = [0.5, 0.5, 0.5]
        query_str = "[" + ",".join(str(v) for v in query_vector) + "]"

        result = conn.execute(
            sa.text(f"""
                SELECT id, name, vector, vector <-> '{query_str}' AS distance
                FROM embeddings
                ORDER BY distance
                LIMIT 5
            """)
        )

        print("Nearest neighbors by L2 distance:")
        for row in result:
            print(f"  {row.name}: distance={row.distance:.4f} vector={row.vector}")

        result2 = conn.execute(
            sa.text(f"""
                SELECT id, name, vector, vector <=> '{query_str}' AS similarity
                FROM embeddings
                ORDER BY similarity DESC
                LIMIT 5
            """)
        )

        print("\nNearest neighbors by cosine similarity:")
        for row in result2:
            print(f"  {row.name}: similarity={row.similarity:.4f}")

        conn.close()
