import logging
import os

import psycopg2
from dotenv import load_dotenv
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, AuthError

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
    password = os.getenv("POSTGRES_PASSWORD", "v8hlDV0yMAHHlIurYupj")
    host = os.getenv("POSTGRES_HOST", "localhost")

    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host
        )
        logging.info(f"Connected to db {dbname} on host {host}")
        return conn
    except psycopg2.Error as e:
        logging.error(f"Failed to connect to db {dbname} on host {host}: {e}")
        raise
