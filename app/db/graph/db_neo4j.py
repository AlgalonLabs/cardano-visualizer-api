import binascii
import logging
from datetime import datetime

from neo4j.time import DateTime

from app.db.connections import connect_neo4j


def clear_neo4j_database():
    logging.info("Performing a clean-up of the graph database")
    driver = connect_neo4j()
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
    driver.close()


def serialize_node(node, exclude_keys=None):
    if exclude_keys is None:
        exclude_keys = []

    if hasattr(node, 'items'):
        return {key: serialize_value(value) for key, value in node.items() if key not in exclude_keys}
    elif isinstance(node, dict):
        return {key: serialize_value(value) for key, value in node.items() if key not in exclude_keys}
    else:
        return str(node)


def serialize_value(value):
    if isinstance(value, DateTime):
        return value.isoformat()
    elif isinstance(value, (list, set)):
        return [serialize_value(v) for v in value]
    elif isinstance(value, dict):
        return {k: serialize_value(v) for k, v in value.items()}
    elif hasattr(value, 'items'):  # Check if it's a node-like object
        return serialize_node(value)
    elif isinstance(value, bytes):
        try:
            return binascii.hexlify(value).decode('ascii')
        except UnicodeDecodeError:
            return binascii.hexlify(value).decode('unicode_escape')
    else:
        return value


def parse_timestamp(ts: str) -> str:
    return datetime.strptime(ts, '%Y-%m-%dT%H:%M:%S').isoformat()
