import tempfile
import pgembed
import sqlalchemy as sa
from sqlalchemy_utils import database_exists, create_database

SAMPLE_DOCUMENTS = [
    ("Introduction to PostgreSQL", "PostgreSQL is a powerful open-source relational database management system."),
    ("Full-Text Search Basics", "Full-text search allows you to search for words and phrases in text documents efficiently."),
    ("Python and Databases", "Python has excellent database integration through libraries like SQLAlchemy and psycopg2."),
    ("Vector Similarity Search", "pgvector enables similarity search for machine learning embeddings."),
    ("Docker Containerization", "Docker containers provide isolated environments for running database servers."),
    ("Text Analysis Techniques", "Natural language processing techniques help analyze and understand text content."),
    ("Database Indexing", "Proper indexing improves query performance significantly for large datasets."),
    ("Web Development with Flask", "Flask is a lightweight Python web framework for building web applications."),
]

with tempfile.TemporaryDirectory() as tmpdir:
    with pgembed.get_server(tmpdir) as pg:
        database_name = 'testdb'
        uri = pg.get_uri(database_name)

        if not database_exists(uri):
            create_database(uri)

        engine = sa.create_engine(uri, isolation_level='AUTOCOMMIT')
        conn = engine.connect()

        with conn.begin():
            conn.execute(sa.text("CREATE EXTENSION IF NOT EXISTS pg_textsearch"))

        with conn.begin():
            conn.execute(sa.text("""
                CREATE TABLE IF NOT EXISTS documents (
                    id SERIAL PRIMARY KEY,
                    title VARCHAR(200),
                    content TEXT
                )
            """))

        with conn.begin():
            for title, content in SAMPLE_DOCUMENTS:
                conn.execute(
                    sa.text("INSERT INTO documents (title, content) VALUES (:title, :content)"),
                    {"title": title, "content": content}
                )

        with conn.begin():
            conn.execute(sa.text("""
                CREATE INDEX IF NOT EXISTS idx_documents_bm25
                ON documents USING bm25(content) WITH (text_config='english')
            """))

        print("=== pg_textsearch (BM25) Demo ===\n")
        print(f"Inserted {len(SAMPLE_DOCUMENTS)} documents\n")

        print("--- Search for 'web' ---")
        result = conn.execute(
            sa.text("""
                SELECT id, title, content <@> to_bm25query('web', 'idx_documents_bm25') AS score
                FROM documents
                ORDER BY score DESC
            """)
        )
        for row in result:
            print(f"  {row.title} (score={row.score:.4f})")

        print("\n--- Search for 'database' ---")
        result = conn.execute(
            sa.text("""
                SELECT id, title, content <@> to_bm25query('database', 'idx_documents_bm25') AS score
                FROM documents
                ORDER BY score DESC
            """)
        )
        for row in result:
            print(f"  {row.title} (score={row.score:.4f})")

        print("\n--- Search for 'python flask' ---")
        result = conn.execute(
            sa.text("""
                SELECT id, title, content <@> to_bm25query('python flask', 'idx_documents_bm25') AS score
                FROM documents
                ORDER BY score DESC
            """)
        )
        for row in result:
            print(f"  {row.title} (score={row.score:.4f})")

        conn.close()
