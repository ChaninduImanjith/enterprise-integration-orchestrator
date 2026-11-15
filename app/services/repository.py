import asyncio
import json
import logging
import sqlite3
from datetime import datetime, timezone
from typing import List, Optional

from app.core.config import settings
from app.models.schemas import SagaContext, SagaStatus

logger = logging.getLogger(__name__)

class SagaRepository:
    """
    Asynchronous persistent repository for Saga state machine contexts.
    Utilizes SQLite with WAL (Write-Ahead Logging) for durability and safe concurrency.
    """

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or settings.DATABASE_PATH
        self._init_lock = asyncio.Lock()
        self._initialized = False

    def _get_connection(self) -> sqlite3.Connection:
        """Creates a configured connection with row dictionary access."""
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        return conn

    def _sync_init_db(self) -> None:
        """Synchronously initializes schema and performance indexes."""
        with self._get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS saga_instances (
                    saga_id TEXT PRIMARY KEY,
                    order_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    current_step TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    error_message TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_saga_order_id ON saga_instances(order_id);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_saga_status ON saga_instances(status);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_saga_created_at ON saga_instances(created_at);")
            conn.commit()
        logger.info(f"Saga repository initialized successfully at {self.db_path}")

    async def init_db(self) -> None:
        """Asynchronously initializes database schema."""
        async with self._init_lock:
            if not self._initialized:
                await asyncio.to_thread(self._sync_init_db)
                self._initialized = True

    def _sync_save_saga(self, context: SagaContext) -> None:
        """Synchronously upserts a SagaContext record."""
        payload_json = json.dumps(context.model_dump(mode="json"))
        now_iso = datetime.now(timezone.utc).isoformat()
        
        with self._get_connection() as conn:
            conn.execute("""
                INSERT INTO saga_instances (
                    saga_id, order_id, status, current_step, payload, error_message, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(saga_id) DO UPDATE SET
                    status = excluded.status,
                    current_step = excluded.current_step,
                    payload = excluded.payload,
                    error_message = excluded.error_message,
                    updated_at = excluded.updated_at;
            """, (
                context.saga_id,
                context.order.order_id,
                context.status.value,
                context.current_step,
                payload_json,
                context.error_message,
                context.created_at.isoformat(),
                now_iso
            ))
            conn.commit()

    async def save_saga(self, context: SagaContext) -> None:
        """Asynchronously persists or updates a saga context."""
        await asyncio.to_thread(self._sync_save_saga, context)

    def _sync_get_saga(self, saga_id: str) -> Optional[SagaContext]:
        """Synchronously retrieves a saga context by saga_id."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT payload FROM saga_instances WHERE saga_id = ?", (saga_id,))
            row = cursor.fetchone()
            if not row:
                return None
            return SagaContext.model_validate_json(row["payload"])

    async def get_saga(self, saga_id: str) -> Optional[SagaContext]:
        """Asynchronously retrieves a saga context by saga_id."""
        return await asyncio.to_thread(self._sync_get_saga, saga_id)

    def _sync_list_sagas(
        self,
        status: Optional[str] = None,
        limit: int = 50,
        offset: int = 0
    ) -> List[SagaContext]:
        """Synchronously retrieves paginated sagas with optional status filter."""
        query = "SELECT payload FROM saga_instances"
        params: List[object] = []

        if status:
            query += " WHERE status = ?"
            params.append(status.upper())

        query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            rows = cursor.fetchall()
            return [SagaContext.model_validate_json(row["payload"]) for row in rows]

    async def list_sagas(
        self,
        status: Optional[str] = None,
        limit: int = 50,
        offset: int = 0
    ) -> List[SagaContext]:
        """Asynchronously retrieves paginated saga instances."""
        return await asyncio.to_thread(self._sync_list_sagas, status, limit, offset)

saga_repository = SagaRepository()

