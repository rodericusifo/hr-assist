import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import json
import os
import uuid

import streamlit as st

from backend.recruitment_specialist_agent.internal.registry.init import DomainRegistry

REQUIRED_SECRETS = (
    "OPENROUTER_API_KEY",
    "OPENROUTER_API_BASE",
    "PRIMARY_POSTGRES_DATABASE_SQL_URL",
    "PRIMARY_QDRANT_DATABASE_VECTOR_URL",
    "LANGSMITH_API_KEY",
    "LANGSMITH_TRACING",
    "LANGSMITH_PROJECT",
)

def initialize_langsmith() -> None:
    os.environ["LANGSMITH_API_KEY"] = st.secrets[
        "LANGSMITH_API_KEY"
    ]
    os.environ["LANGSMITH_TRACING"] = st.secrets[
        "LANGSMITH_TRACING"
    ]
    os.environ["LANGSMITH_PROJECT"] = st.secrets[
        "LANGSMITH_PROJECT"
    ]

initialize_langsmith()

missing_secrets = [key for key in REQUIRED_SECRETS if key not in st.secrets]
if missing_secrets:
    st.error(
        f"Missing secrets: {', '.join(missing_secrets)}. "
        "Add them to .streamlit/secrets.toml."
    )
    st.stop()

def _usage_text(u: dict) -> str:
    out_details = u.get("output_token_details") or {}
    in_details = u.get("input_token_details") or {}
    parts = [
        f"in {u.get('input_tokens', 0):,}",
        f"out {u.get('output_tokens', 0):,}",
    ]
    if out_details.get("reasoning"):
        parts.append(f"reasoning {out_details['reasoning']:,}")
    if in_details.get("cache_read"):
        parts.append(f"cache read {in_details['cache_read']:,}")
    parts.append(f"total {u.get('total_tokens', 0):,}")
    return "🪙 " + " · ".join(parts)


def _sum_usage(usages: list[dict]) -> dict:
    total: dict = {}
    for u in usages:
        for k in ("input_tokens", "output_tokens", "total_tokens"):
            total[k] = total.get(k, 0) + (u.get(k) or 0)
        for group in ("input_token_details", "output_token_details"):
            for k, v in (u.get(group) or {}).items():
                if isinstance(v, int):
                    total.setdefault(group, {})
                    total[group][k] = total[group].get(k, 0) + v
    return total

def _fmt(obj) -> str:
    if obj is None:
        return ""
    if isinstance(obj, str):
        return obj
    return json.dumps(obj, indent=2, ensure_ascii=False, default=str)

@st.cache_resource(show_spinner=False)
def get_agent():
    return DomainRegistry().get("service.main.agent")

agent = get_agent()
st.set_page_config(page_title="Recruitment Specialist Agent", page_icon="🤖", layout="centered")
st.title("🤖 Recruitment Specialist Agent")
# session_state survives reruns (each message) but is wiped on browser reload,
# so every page load starts a fresh thread + empty history.
st.session_state.setdefault("thread_id", str(uuid.uuid4()))
# history: list of {"role": "user"|"assistant", "blocks": [ {type, ...}, ... ]}
st.session_state.setdefault("history", [])

# ---------- static rendering (history) ----------
def render_block(block: dict):
    kind = block["type"]
    if kind == "reasoning":
        with st.expander("🧠 Reasoning", expanded=False):
            st.markdown(block["text"])
    elif kind == "text":
        st.markdown(block["text"])
    elif kind == "tool":
        state = "error" if block.get("error") else "complete"
        with st.status(f"🔧 {block['name']}", state=state, expanded=False):
            st.caption("Input")
            st.code(_fmt(block["input"]), language="json")
            st.caption("Error" if block.get("error") else "Output")
            st.code(block.get("error") or _fmt(block.get("output")))
    elif kind == "error":
        st.error(block["text"])

for turn in st.session_state.history:
    with st.chat_message(turn["role"]):
        for b in turn["blocks"]:
            render_block(b)

# ---------- live rendering (events) ----------
def render_events(events) -> list[dict]:
    """Draw events as they arrive; return blocks to store in history."""
    blocks: list[dict] = []
    ui: dict = {}     # live widgets keyed by event id
    buf: dict = {}    # accumulated text keyed by (id, channel)
    usages: list[dict] = []

    # Events after which the agent goes quiet (model is thinking / about to
    # call the next step). While waiting for the next event, show a spinner.
    idle_after = {None, "message_start", "message_end", "tool_end"}
    it = iter(events)
    last = None

    while True:
        if last in idle_after:
            with st.spinner("Thinking…", show_time=True):
                ev = next(it, None)   # blocks here while the agent works
        else:
            ev = next(it, None)
        if ev is None:
            break
        name, d = ev["event"], ev["data"]
        last = name

        if name == "reasoning_delta":
            key = (d["id"], "reasoning")
            buf[key] = buf.get(key, "") + d["delta"]
            if key not in ui:
                box = st.status("🧠 Thinking…", expanded=True)
                ui[key] = (box, box.empty())
            ui[key][1].markdown(buf[key] + "▌")

        elif name == "reasoning_end":
            blocks.append({"type": "reasoning", "text": d["text"]})
            key = (d["id"], "reasoning")
            if key in ui:
                box, ph = ui[key]
                ph.markdown(d["text"])
                box.update(label="🧠 Reasoning", state="complete", expanded=False)

        elif name == "text_delta":
            key = (d["id"], "text")
            buf[key] = buf.get(key, "") + d["delta"]
            if key not in ui:
                ui[key] = st.empty()
            ui[key].markdown(buf[key] + "▌")

        elif name == "text_end":
            blocks.append({"type": "text", "text": d["text"]})
            key = (d["id"], "text")
            if key in ui:
                ui[key].markdown(d["text"])

        elif name == "tool_start":
            box = st.status(f"🔧 Running `{d['name']}`…", expanded=True)
            box.caption("Input")
            box.code(_fmt(d["input"]), language="json")
            box.caption("Output")
            ui[d["id"]] = (box, box.empty())
            blocks.append({"type": "tool", "id": d["id"], "name": d["name"], "input": d["input"]})

        elif name == "tool_output_delta":
            key = (d["id"], "out")
            buf[key] = buf.get(key, "") + d["delta"]
            ui[d["id"]][1].code(buf[key])

        elif name == "tool_end":
            box, ph = ui[d["id"]]
            ph.code(d["error"] or _fmt(d["output"]))
            box.update(
                label=f"🔧 {d['name']}",
                state="error" if d["error"] else "complete",
                expanded=False,
            )
            for b in blocks:
                if b.get("id") == d["id"]:
                    b.update(output=d["output"], error=d["error"])

        elif name == "message_end":
            if d.get("usage"):
                usages.append(d["usage"])

        elif name == "error":
            msg = f"⚠️ {d['type']}: {d['message']}"
            st.error(msg)
            blocks.append({"type": "error", "text": msg})

    # One line per turn, summed over all model calls (incl. after tool calls).
    if usages:
        total = _sum_usage(usages)
        st.caption(_usage_text(total))
        blocks.append({"type": "usage", "usage": total})

    return blocks


if prompt := st.chat_input("Ask something…"):
    st.session_state.history.append(
        {"role": "user", "blocks": [{"type": "text", "text": prompt}]}
    )
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        blocks = render_events(
            agent.run(thread_id=st.session_state.thread_id, user_prompt=prompt)
        )
    st.session_state.history.append({"role": "assistant", "blocks": blocks})