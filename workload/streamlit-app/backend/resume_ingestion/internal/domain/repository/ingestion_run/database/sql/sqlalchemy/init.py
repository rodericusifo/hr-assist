from sqlalchemy.ext.asyncio import AsyncEngine

from .set_status import set_status
from .set_progress import set_progress
from .get_or_create import get_or_create

class SqlAlchemyIngestionRunRepository:
    set_status = set_status
    set_progress = set_progress
    get_or_create = get_or_create

    def __init__(self, engine: AsyncEngine):
        self._engine = engine
