import logging
from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query, status

from app.models.schemas import SagaContext, SagaStatus
from app.services.repository import saga_repository

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/sagas", tags=["Saga Status & Audit"])

@router.get(
    "/{saga_id}",
    response_model=SagaContext,
    status_code=status.HTTP_200_OK,
    summary="Query live Saga state by Saga ID"
)
async def get_saga_by_id(saga_id: str):
    """
    Retrieves the complete state of a distributed Saga transaction by its unique ID.
    Includes inventory reservation tokens, payment transactions, shipping tracking, and compensation details.
    """
    saga = await saga_repository.get_saga(saga_id)
    if not saga:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Saga transaction '{saga_id}' was not found in the state store."
        )
    return saga

@router.get(
    "",
    response_model=List[SagaContext],
    status_code=status.HTTP_200_OK,
    summary="List recent Saga transactions with optional status filter and pagination"
)
async def list_sagas(
    saga_status: Optional[SagaStatus] = Query(None, alias="status", description="Filter by Saga status"),
    limit: int = Query(50, ge=1, le=100, description="Max number of records to return"),
    offset: int = Query(0, ge=0, description="Pagination offset")
):
    """
    Returns an audited history of Saga transactions executed by the orchestrator.
    Supports filtering by status (e.g., COMPLETED, COMPENSATED, FAILED, IN_PROGRESS).
    """
    status_filter = saga_status.value if saga_status else None
    return await saga_repository.list_sagas(status=status_filter, limit=limit, offset=offset)
