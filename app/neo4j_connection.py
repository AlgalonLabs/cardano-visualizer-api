from neo4j import GraphDatabase


def connect_neo4j():
    uri = "bolt://localhost:7687"
    return GraphDatabase.driver(uri, auth=("matt", "Topspin3)"))


neo4j_driver = connect_neo4j()
