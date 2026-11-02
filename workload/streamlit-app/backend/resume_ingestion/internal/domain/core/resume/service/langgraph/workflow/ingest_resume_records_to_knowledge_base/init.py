from langgraph.graph.state import CompiledStateGraph, TimeoutPolicy
from langgraph.errors import NodeError
from langgraph.types import Command, RetryPolicy
from langgraph.graph import END, START, StateGraph
from langgraph.checkpoint.base import BaseCheckpointSaver

from backend.resume_ingestion.internal.domain.core.resume.service.langgraph.workflow.ingest_resume_records_to_knowledge_base.schema import (
    IngestResumeRecordsToKnowledgeBaseWorkflowContext,
    IngestResumeRecordsToKnowledgeBaseWorkflowState,
)

from backend.resume_ingestion.internal.domain.core.resume.service.langgraph.workflow.ingest_resume_records_to_knowledge_base.extract import (
    ExtractMixin,
)
from backend.resume_ingestion.internal.domain.core.resume.service.langgraph.workflow.ingest_resume_records_to_knowledge_base.transform import (
    TransformMixin,
)


class IngestResumeRecordsToKnowledgeBaseWorkflow(ExtractMixin, TransformMixin):
    def __init__(self):
        graph = StateGraph(
            state_schema=IngestResumeRecordsToKnowledgeBaseWorkflowState,
            context_schema=IngestResumeRecordsToKnowledgeBaseWorkflowContext
        )

        graph.set_node_defaults(
            retry_policy=RetryPolicy(max_attempts=3),
            error_handler=self.default_error_handler,
            timeout=TimeoutPolicy(run_timeout=30),
        )
        graph.add_node("extract", self.extract)
        graph.add_node("transform_clean", self.transform_clean)
        graph.add_node("transform_enrich", self.transform_enrich)
        graph.add_node("transform_chunk", self.transform_chunk)
        graph.add_node("finalize", self.finalize)

        graph.add_edge(START, "extract")
        graph.add_edge("extract", "transform_clean")
        graph.add_edge("transform_clean", "transform_enrich")
        graph.add_edge("transform_enrich", "transform_chunk")
        graph.add_edge("transform_chunk", END)

        self._graph = graph

    def compile(self, checkpointer: BaseCheckpointSaver | None = None) -> CompiledStateGraph:
        return self._graph.compile(checkpointer=checkpointer)

    def default_error_handler(state: IngestResumeRecordsToKnowledgeBaseWorkflowState, error: NodeError) -> Command:
        # Handle errors and update the state accordingly
        return Command(
            update={
                "error": {
                    "node": error.node,
                    "message": str(error),
                }
            },
            goto="finalize",
        )

    def finalize(state: IngestResumeRecordsToKnowledgeBaseWorkflowState) -> IngestResumeRecordsToKnowledgeBaseWorkflowState:
        # Finalize the workflow, perform any necessary cleanup or final actions
        return state
