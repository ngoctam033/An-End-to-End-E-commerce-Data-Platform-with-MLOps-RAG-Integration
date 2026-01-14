import psycopg2
from db_config import postgres_config
from logger import logger

class Database:
    def __init__(self, config=postgres_config):
        self.config = config
        self.conn = self.connect()

    def connect(self):
        """Creates and returns a connection to the PostgreSQL database."""
        try:
            conn = psycopg2.connect(**self.config)
            logger.info(f"Connected to database {self.config['database']} at {self.config['host']}:{self.config['port']}")
            return conn
        except Exception as e:
            logger.error(f"Error connecting to database: {e}")
            return None

    def close(self):
        """Closes the database connection."""
        if self.conn and not self.conn.closed:
            self.conn.close()

# Export a default instance for convenience
db = Database()
