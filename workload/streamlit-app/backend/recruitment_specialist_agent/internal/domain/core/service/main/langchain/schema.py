from dataclasses import dataclass
from sqlalchemy.engine import Engine
from langchain_core.vectorstores import VectorStore

@dataclass
class MainAgentContext:
    sql_engine: Engine | None = None
    vectorstore: VectorStore | None = None
