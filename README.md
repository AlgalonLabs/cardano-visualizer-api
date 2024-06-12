# Visualizer API

This guide will help you set up a Neo4j database using the Neo4j Desktop application, connect it with the cardano-db-sync
database, and integrate both with a Python application/back-fill script.

## Prerequisites

- Neo4j Desktop application
- Cardano DB Sync PostgreSQL database
- Python 3.12
- Pip

## Step 1: Setting Up Neo4j

### Download and Install Neo4j Desktop

1. Download the Neo4j Desktop application from the [official website](https://neo4j.com/download/).
2. Install the application following the instructions for your operating system.

### Create a New Database

1. Open Neo4j Desktop.
2. Click on "New" under the "Local DBMS" section.
3. Give your database a name and set a password (remember this password for later).
4. Click "Create" to initialize the database.
5. Once created, start the database by clicking "Open".

### Configure Connection Details

Ensure your Neo4j database is running and note down the connection details:

- **URI**: `bolt://localhost:7687`
- **Username**: `neo4j`
- **Password**: `<your_password>` (the password you set during database creation)

## Step 2: Setting up Cardano DB Sync

### Follow Cardano DB Sync Docker Guide

1. Follow the instructions found [here](https://github.com/IntersectMBO/cardano-db-sync/blob/master/doc/docker.md).

### Configure Connection Details

Ensure your `cardano-db-sync-postgres` instance is running, and note down the connection details:

- **Database Name**: `cexplorer`
- **User**: `postgres`
- **Password**: See `cardano-db-sync/config/secrets/postgres_password` file.
- **Host**: `localhost`

## Step 3: Setting Up Python Environment

### Install Python Dependencies

1. Create a virtual environment (optional but recommended):

```bash
python -m venv venv
source venv/bin/activate
```

2. Install required Python packages:

```bash
pip install -r requirements.txt
```

### Connect to Databases

#### Create .env file

Create a `.env` file in the root directory of the project and add the following environment variables:

```env
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=<your_password>
POSTGRES_DB=cexplorer
POSTGRES_USER=postgres
POSTGRES_PASSWORD=<your_password>
POSTGRES_HOST=localhost
```

## Step 4: Populating Neo4j

1. Ensure both Neo4j and Postgres databases are running.
2. Run your Python application. If you want to **build the graph**, you can use the following command:

```bash
python extract_transactions_to_graph_store.py
```

You may also configure the timerange for which you want to extract transactions by modifying the `start_time` and `end_time` variables in the script.

## Additional Information

- [Neo4j Cypher Query Language](https://neo4j.com/developer/cypher/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Neo4j Python Driver Documentation](https://neo4j.com/docs/api/python-driver/current/)
- [Psycopg2 Documentation](https://www.psycopg.org/docs/)

## Troubleshooting

- Ensure the Neo4j and PostgreSQL services are running and accessible.
- Verify the connection details (URI, username, password, host) are correct.
- Check for any firewall or network issues that might be blocking the connections.

If you encounter any issues, refer to the respective documentation or seek help from the community forums.