from langchain_core.language_models.chat_models import BaseChatModel
from pydantic import BaseModel, ConfigDict

class LLM(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    parser: BaseChatModel
