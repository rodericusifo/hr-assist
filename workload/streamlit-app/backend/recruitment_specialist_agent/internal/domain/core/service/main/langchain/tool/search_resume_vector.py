from langchain.tools import tool, ToolRuntime

from backend.recruitment_specialist_agent.internal.domain.core.service.main.langchain.schema import MainAgentContext

@tool
def search_resume_vector(runtime: ToolRuntime[MainAgentContext], query: str, top_k: int = 3) -> str:
    """
    Performs semantic similarity search on resume chunks using the Vector Store.
    Returns textual excerpts, verbatim evidence, skills list, and resume_id
    matched from Qdrant metadata.
    """
    try:
        if runtime.context.vectorstore is None:
            return "Error: Vector store instance is not initialized."

        # Search vector store based on semantic query
        docs = runtime.context.vectorstore.similarity_search(query, k=top_k)

        if not docs:
            return "No resumes matched the semantic search criteria."

        formatted_results = []
        for i, doc in enumerate(docs, 1):
            meta = doc.metadata or {}
            # Extract resume_id (corresponds to resumes.id in PostgreSQL schema)
            resume_id = meta.get("resume_id") or meta.get("id") or "Unknown"
            category = meta.get("category", "N/A")
            skills_list = ", ".join(meta.get("skills", []))

            formatted_results.append(
                f"--- Vector Match #{i} ---\n"
                f"Resume ID: {resume_id}\n"
                f"Category: {category}\n"
                f"Extracted Skills: {skills_list}\n"
                f"Verbatim Excerpt / Evidence: {doc.page_content}\n"
            )

        return "\n".join(formatted_results)
    except Exception as err:
        return f"Vector Search Error: {str(err)}"