import logging
from datetime import datetime

import neo4j

from app.db.connections import connect_neo4j


def clear_neo4j_database():
    logging.info("Performing a clean-up of the graph database")
    driver = connect_neo4j()
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
    driver.close()


def serialize_value(value):
    if isinstance(value, neo4j.time.DateTime):
        return value.iso_format()
    return value


def serialize_node(node, exclude_keys=None):
    if node is None:
        return None
    if exclude_keys is None:
        exclude_keys = []
    return {key: serialize_value(value) for key, value in dict(node).items() if key not in exclude_keys}


def parse_timestamp(ts: str) -> str:
    return datetime.strptime(ts, '%Y-%m-%dT%H:%M:%S').isoformat()
