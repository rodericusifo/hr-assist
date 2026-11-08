import streamlit as st

from backend.resume_ingestion.internal.pkg.constant.dialect import Dialect


def database_sql_connection_string(dialect: Dialect) -> str:
    match dialect:
        case Dialect.POSTGRES:
            secret_key = f"PRIMARY_{Dialect.POSTGRES}_DATABASE_SQL_URL"
        case _:
            raise ValueError(f"Unsupported database SQL dialect: {dialect}")

    connection_string = st.secrets.get(secret_key)
    if not connection_string:
        raise RuntimeError(
            f"Configure {secret_key} "
            "in .streamlit/secrets.toml"
        )
    return str(connection_string)
