from langchain_core.embeddings import Embeddings
from pydantic import BaseModel, ConfigDict


class LLM(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    embedder: Embeddings
