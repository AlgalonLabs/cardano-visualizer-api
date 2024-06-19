import logging
import os

from dotenv import load_dotenv
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, AuthError
from sqlalchemy import create_engine

from app.db.models.base import Base

# Load environment variables from .env file
load_dotenv()


def connect_neo4j():
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "<your_password>")

    driver = GraphDatabase.driver(uri, auth=(user, password))

    try:
        driver.verify_connectivity()
        logging.info(f"Connected to Neo4j at {uri}")
    except ServiceUnavailable as e:
        logging.error(f"Failed to connect to Neo4j: {e}")
        raise
    except AuthError as e:
        logging.error(f"Authentication error: {e}")
        raise

    return driver


def connect_postgres():
    dbname = os.getenv("POSTGRES_DB", "cexplorer")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST", "localhost")

    try:
        engine = create_engine(
            f'postgresql+psycopg2://{user}:{password}@{host}/{dbname}'
        )
        Base.metadata.create_all(engine)
        logging.info(f"Connected to db {dbname} on host {host}")
        return engine
    except Exception as e:
        logging.error(f"Failed to connect to db {dbname} on host {host}: {e}")
        raise


def init_db():
    dbname = os.getenv("POSTGRES_DB", "cexplorer")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST", "localhost")

    engine = create_engine(
        f'postgresql+psycopg2://{user}:{password}@{host}/{dbname}'
    )
    Base.metadata.create_all(engine)
    print("Database schema created successfully.")
