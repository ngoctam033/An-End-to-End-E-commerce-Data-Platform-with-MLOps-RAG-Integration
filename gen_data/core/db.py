import psycopg2
from db_config import postgres_config

class Database:
    def __init__(self, config=postgres_config):
        self.config = config
        self.conn = None

    def connect(self):
        """Creates and returns a connection to the PostgreSQL database."""
        try:
            if not self.conn or self.conn.closed:
                self.conn = psycopg2.connect(**self.config)
            return self.conn
        except Exception as e:
            print(f"Error connecting to database: {e}")
            return None

    def close(self):
        """Closes the database connection."""
        if self.conn and not self.conn.closed:
            self.conn.close()

# Export a default instance for convenience
db = Database()
