from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routers import graph

app = FastAPI()

# Add CORS middleware to allow requests from the frontend
origins = [
    "http://localhost:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(graph.router)