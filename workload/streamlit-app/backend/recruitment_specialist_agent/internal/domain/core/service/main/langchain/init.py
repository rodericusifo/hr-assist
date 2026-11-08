from typing import Generator
import json

from langchain.agents import create_agent
from langchain_openai import ChatOpenAI
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits import SQLDatabaseToolkit
from langsmith import Client
from langchain_openai import OpenAIEmbeddings
from langchain_qdrant import QdrantVectorStore
from langgraph.checkpoint.memory import InMemorySaver
from qdrant_client import QdrantClient
from sqlalchemy import create_engine
import streamlit as st

from backend.recruitment_specialist_agent.internal.domain.core.service.main.langchain.tool.search_resume_vector import search_resume_vector
from backend.recruitment_specialist_agent.internal.pkg.config.config_database_sql import (
    database_sql_connection_string,
)
from backend.recruitment_specialist_agent.internal.pkg.constant.dialect import Dialect
from .schema import MainAgentContext

langsmith_client = Client()

class LangchainMainAgentService:
    def __init__(self):
        self.llm_chat = ChatOpenAI(
            model="qwen/qwen3.7-flash",
            api_key=st.secrets["OPENROUTER_API_KEY"],
            base_url=st.secrets["OPENROUTER_API_BASE"],
            temperature=0.0,
        )
        self.llm_embbeder = OpenAIEmbeddings(
            model="openai/text-embedding-3-small",
            api_key=st.secrets["OPENROUTER_API_KEY"],
            base_url=st.secrets["OPENROUTER_API_BASE"],
        )
        self.sql_engine = create_engine(
            database_sql_connection_string(Dialect.POSTGRES),
            pool_pre_ping=True,
        )
        self.vectorstore = QdrantVectorStore(
            client=QdrantClient(
                url=st.secrets["PRIMARY_QDRANT_DATABASE_VECTOR_URL"],
                api_key=st.secrets["PRIMARY_QDRANT_DATABASE_SQL_API_KEY"] if "PRIMARY_QDRANT_DATABASE_SQL_API_KEY" in st.secrets else None,
                prefer_grpc=True,
            ),
            collection_name="resumes",
            embedding=self.llm_embbeder,
        )
        self.tools = [
            search_resume_vector,
        ] + SQLDatabaseToolkit(
            db=SQLDatabase(self.sql_engine, include_tables=["resumes"]),
            llm=self.llm_chat
        ).get_tools()
        self.agent = create_agent(
            model=self.llm_chat,
            tools=self.tools,
            system_prompt=langsmith_client.pull_prompt("recruitment-specialist-agent").format(),
            checkpointer=InMemorySaver(),
        )

    def run(self, thread_id: str, user_prompt: str) -> Generator[dict, None, None]:
        try:
            config = {
                "configurable": {
                    "thread_id": thread_id,
                }
            }
            context = MainAgentContext(
                sql_engine=self.sql_engine,
                vectorstore=self.vectorstore,
            )
            stream = self.agent.stream_events(
                {"messages": [{"role": "user", "content": user_prompt}]},
                config=config,
                context=context,
                version="v3",
            )

            msg_n = tool_n = 0
            for kind, item in stream.interleave("messages", "tool_calls"):
                if kind == "messages":
                    msg_n += 1
                    mid = f"msg_{msg_n}"
                    yield self._ev("message_start", id=mid)

                    reasoning = ""
                    for delta in item.reasoning:
                        reasoning += str(delta)
                        yield self._ev("reasoning_delta", id=mid, delta=str(delta))
                    if reasoning:
                        yield self._ev("reasoning_end", id=mid, text=reasoning)

                    text = ""
                    for delta in item.text:
                        text += str(delta)
                        yield self._ev("text_delta", id=mid, delta=str(delta))
                    if text:
                        yield self._ev("text_end", id=mid, text=text)

                    yield self._ev("message_end", id=mid)

                elif kind == "tool_calls":
                    tool_n += 1
                    tid = f"tool_{tool_n}"
                    yield self._ev("tool_start", id=tid, name=item.tool_name,
                            input= self._jsonable(item.input))
                    for delta in item.output_deltas:
                        yield self._ev("tool_output_delta", id=tid, delta=str(delta))
                    error = item.error
                    yield self._ev(
                        "tool_end", id=tid, name=item.tool_name,
                        output=None if error else self._jsonable(item.output),
                        error=str(error) if error else None,
                    )
        except Exception as e:  # noqa: BLE001 - surface to the consumer as an event
            yield self._ev("error", type=type(e).__name__, message=str(e))
        yield self._ev("done")

    @staticmethod
    def _ev(event: str, **data) -> dict:
        return {"event": event, "data": data}

    @staticmethod
    def _jsonable(obj):
        obj = getattr(obj, "content", obj)
        try:
            json.dumps(obj)
            return obj
        except TypeError:
            return str(obj)
