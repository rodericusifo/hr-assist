from langgraph.graph.state import CompiledStateGraph
from langgraph.errors import NodeError
from langgraph.types import Command, RetryPolicy
from langgraph.graph import END, START, StateGraph
from langgraph.checkpoint.base import BaseCheckpointSaver

from .schema import (
    IngestResumeRecordsToKnowledgeBaseWorkflowContext,
    IngestResumeRecordsToKnowledgeBaseWorkflowState,
)
from .extract import extract
from .transform_clean import transform_clean
from .transform_enrich import transform_enrich
from .transform_chunk import transform_chunk
from .load_to_sql_db import load_to_sql_db
from .load_to_vector_db import load_to_vector_db
from backend.resume_ingestion.internal.pkg.interface.resume import (
    ISQLResumeRepository,
    IVectorResumeRepository,
)

class IngestResumeRecordsToKnowledgeBaseWorkflow:
    extract = extract
    transform_clean = transform_clean
    transform_enrich = transform_enrich
    transform_chunk = transform_chunk
    load_to_sql_db = load_to_sql_db
    load_to_vector_db = load_to_vector_db

    def __init__(
        self,
        vector_resume_repository: IVectorResumeRepository | None = None,
        sql_resume_repository: ISQLResumeRepository | None = None,
    ):
        self._vector_resume_repository = vector_resume_repository
        self._sql_resume_repository = sql_resume_repository
        graph = StateGraph(
            state_schema=IngestResumeRecordsToKnowledgeBaseWorkflowState,
            context_schema=IngestResumeRecordsToKnowledgeBaseWorkflowContext
        )

        graph.set_node_defaults(
            retry_policy=RetryPolicy(max_attempts=3),
            error_handler=self.default_error_handler,
        )
        graph.add_node("extract", self.extract)
        graph.add_node("transform_clean", self.transform_clean)
        graph.add_node("transform_enrich", self.transform_enrich)
        graph.add_node("transform_chunk", self.transform_chunk)
        graph.add_node("load_to_vector_db", self.load_to_vector_db)
        graph.add_node("load_to_sql_db", self.load_to_sql_db)
        graph.add_node("finalize", self.finalize)

        graph.add_edge(START, "extract")
        graph.add_edge("extract", "transform_clean")
        graph.add_edge("transform_clean", "transform_enrich")
        graph.add_edge("transform_enrich", "transform_chunk")
        graph.add_edge("transform_chunk", "load_to_vector_db")
        graph.add_edge("transform_chunk", "load_to_sql_db")
        graph.add_edge(["load_to_vector_db", "load_to_sql_db"], END)
        graph.add_edge("finalize", END)

        self._graph = graph

    def compile(self, checkpointer: BaseCheckpointSaver | None = None) -> CompiledStateGraph:
        return self._graph.compile(checkpointer=checkpointer)

    @staticmethod
    def default_error_handler(
        state: IngestResumeRecordsToKnowledgeBaseWorkflowState,
        error: NodeError,
    ) -> Command:
        return Command(
            update={
                "error": {
                    "node": error.node,
                    "message": str(error.error),
                }
            },
            goto="finalize",
        )

    def finalize(state: IngestResumeRecordsToKnowledgeBaseWorkflowState) -> IngestResumeRecordsToKnowledgeBaseWorkflowState:
        # Finalize the workflow, perform any necessary cleanup or final actions
        return state
