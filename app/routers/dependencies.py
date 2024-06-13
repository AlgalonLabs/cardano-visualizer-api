from app.db.connections import connect_neo4j


def get_neo4j_driver():
    driver = connect_neo4j()
    try:
        yield driver
    finally:
        driver.close()
