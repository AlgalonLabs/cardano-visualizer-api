from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.db.connections import connect_neo4j
from app.routers import graph, dashboard, details

app = FastAPI()

origins = [
    "http://localhost:3000",
]

neo4_driver = connect_neo4j()

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(graph.router)
app.include_router(dashboard.router)
app.include_router(details.router)
