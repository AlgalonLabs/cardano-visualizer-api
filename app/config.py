# File: app/config.py

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    cmc_api_key: str = Field(..., env="CMC_API_KEY")
    neo4j_uri: str = Field(..., env="NEO4J_URI")
    neo4j_user: str = Field(..., env="NEO4J_USER")
    neo4j_password: str = Field(..., env="NEO4J_PASSWORD")
    postgres_db: str = Field(..., env="POSTGRES_DB")
    postgres_user: str = Field(..., env="POSTGRES_USER")
    postgres_password: str = Field(..., env="POSTGRES_PASSWORD")
    postgres_host: str = Field(..., env="POSTGRES_HOST")

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", frozen=True)

    def __hash__(self):
        return hash((type(self),) + tuple(self.__dict__.values()))


def get_settings() -> Settings:
    return Settings()
