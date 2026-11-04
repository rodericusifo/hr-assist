from langgraph.graph.state import CompiledStateGraph
from langgraph.errors import NodeError
from langgraph.types import Command, RetryPolicy
from langgraph.graph import END, START, StateGraph
from langgraph.checkpoint.base import BaseCheckpointSaver

from backend.resume_ingestion.internal.domain.core.service.resume.langgraph.workflow.ingest_resume_records_to_knowledge_base.schema import (
    IngestResumeRecordsToKnowledgeBaseWorkflowContext,
    IngestResumeRecordsToKnowledgeBaseWorkflowState,
)

from backend.resume_ingestion.internal.domain.core.service.resume.langgraph.workflow.ingest_resume_records_to_knowledge_base.extract import (
    ExtractMixin,
)
from backend.resume_ingestion.internal.domain.core.service.resume.langgraph.workflow.ingest_resume_records_to_knowledge_base.transform import (
    TransformMixin,
)
from backend.resume_ingestion.internal.domain.core.service.resume.langgraph.workflow.ingest_resume_records_to_knowledge_base.load import (
    LoadMixin,
)
from backend.resume_ingestion.internal.pkg.interface.resume import (
    IResumeRepository,
)


class IngestResumeRecordsToKnowledgeBaseWorkflow(
    ExtractMixin,
    TransformMixin,
    LoadMixin,
):
    def __init__(
        self,
        vector_database_resume_repository: IResumeRepository | None = None,
        sql_database_resume_repository: IResumeRepository | None = None,
    ):
        self._vector_database_resume_repository = vector_database_resume_repository
        self._sql_database_resume_repository = sql_database_resume_repository
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
