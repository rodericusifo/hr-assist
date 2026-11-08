from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .init import DomainRegistry

from backend.recruitment_specialist_agent.internal.domain.core.service.main.langchain.init import LangchainMainAgentService

def registerServices(self: DomainRegistry) -> None:
    self._components["service.main.agent"] = LangchainMainAgentService()