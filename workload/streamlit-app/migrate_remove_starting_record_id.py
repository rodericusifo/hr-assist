"""
Migration script to remove starting_record_id column from ingestion_runs table.
Run this once to update your database schema.
"""
import asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from backend.resume_ingestion.internal.pkg.config.config_database_sql import (
    database_sql_connection_string,
)
from backend.resume_ingestion.internal.pkg.constant.dialect import Dialect


async def migrate() -> None:
    connection_string = database_sql_connection_string(Dialect.POSTGRES)
    engine = create_async_engine(
        connection_string.replace("postgresql://", "postgresql+asyncpg://", 1)
    )
    
    async with engine.begin() as conn:
        print("Dropping starting_record_id column from ingestion_runs table...")
        await conn.execute(
            text("ALTER TABLE ingestion_runs DROP COLUMN IF EXISTS starting_record_id")
        )
        print("✓ Migration complete")
    
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(migrate())
