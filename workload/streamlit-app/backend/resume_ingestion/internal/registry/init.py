from backend.resume_ingestion.internal.registry.services import ServicesMixin
from backend.resume_ingestion.internal.registry.repositories import RepositoriesMixin

class DomainRegistry(ServicesMixin, RepositoriesMixin):
    def __init__(self):
        self._components = {}

        self.registerRepositories()
        self.registerServices()

    def get(self, name: str) -> type:
        if name not in self._components:
            raise ValueError(f"Domain class with name '{name}' is not registered.")
        return self._components[name]

    def list_registered(self) -> list[str]:
        return list(self._components.keys())