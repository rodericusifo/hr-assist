
from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .init import (
        LangchainMainAgentService,
    )

from .schema import MainAgentContext
from typing import Generator
import json

def run(self: LangchainMainAgentService, thread_id: str, user_prompt: str) -> Generator[dict, None, None]:
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
                yield _ev("message_start", id=mid)

                reasoning = ""
                for delta in item.reasoning:
                    reasoning += str(delta)
                    yield _ev("reasoning_delta", id=mid, delta=str(delta))
                if reasoning:
                    yield _ev("reasoning_end", id=mid, text=reasoning)

                text = ""
                for delta in item.text:
                    text += str(delta)
                    yield _ev("text_delta", id=mid, delta=str(delta))
                if text:
                    yield _ev("text_end", id=mid, text=text)

                usage = getattr(item.output, "usage_metadata", None)
                yield _ev("message_end", id=mid, usage=dict(usage) if usage else None)

            elif kind == "tool_calls":
                tool_n += 1
                tid = f"tool_{tool_n}"
                yield _ev("tool_start", id=tid, name=item.tool_name,
                        input=_jsonable(item.input))
                for delta in item.output_deltas:
                    yield _ev("tool_output_delta", id=tid, delta=str(delta))
                error = item.error
                yield _ev(
                    "tool_end", id=tid, name=item.tool_name,
                    output=None if error else _jsonable(item.output),
                    error=str(error) if error else None,
                )
    except Exception as e:  # noqa: BLE001 - surface to the consumer as an event
        yield _ev("error", type=type(e).__name__, message=str(e))
    yield _ev("done")

def _ev(event: str, **data) -> dict:
    return {"event": event, "data": data}

def _jsonable(obj):
    obj = getattr(obj, "content", obj)
    try:
        json.dumps(obj)
        return obj
    except TypeError:
        return str(obj)