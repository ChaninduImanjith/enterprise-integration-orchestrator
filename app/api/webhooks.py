import logging
from typing import Any, Dict
from fastapi import APIRouter, HTTPException, status
from app.core.config import settings
from app.core.rabbitmq import rabbitmq_client
from app.models.schemas import OrderPayload, SagaContext, SagaStatus, WebhookResponse
from app.services.ai_mapper import ai_mapper_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/webhooks", tags=["Webhooks & Ingestion"])

async def _publish_saga_event(order: OrderPayload) -> SagaContext:
    """Helper to initialize Saga state context and publish event to RabbitMQ."""
    saga_context = SagaContext(
        order=order,
        status=SagaStatus.STARTED,
        current_step="ORDER_RECEIVED"
    )

    logger.info(f"Initiating Saga {saga_context.saga_id} for order {order.order_id}")

    if not rabbitmq_client.channel or rabbitmq_client.channel.is_closed:
        logger.error("RabbitMQ broker connection is unavailable.")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Message broker is currently unavailable. Workflow could not be queued."
        )

    try:
        await rabbitmq_client.publish(
            routing_key=settings.ORDER_SAGA_ROUTING_KEY,
            message=saga_context.model_dump(mode="json"),
            correlation_id=saga_context.saga_id
        )
    except Exception as exc:
        logger.exception(f"Failed to publish saga event to RabbitMQ: {exc}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to publish saga event to messaging queue: {str(exc)}"
        )
        
    return saga_context

@router.post(
    "/order",
    response_model=WebhookResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Ingest strictly-typed order and initiate asynchronous Saga workflow"
)
async def receive_order_webhook(order: OrderPayload):
    """
    Accepts an incoming standard enterprise order payload, initializes a new Saga state context,
    and publishes the initiation event to the RabbitMQ workflow exchange.
    """
    saga_context = await _publish_saga_event(order)

    return WebhookResponse(
        success=True,
        saga_id=saga_context.saga_id,
        order_id=order.order_id,
        message="Order accepted successfully. Saga orchestration workflow initiated."
    )

@router.post(
    "/legacy-order",
    response_model=WebhookResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Ingest unstructured/legacy JSON and map to target schema via AI Schema Mapper"
)
async def receive_legacy_order_webhook(payload: Dict[str, Any]):
    """
    Accepts arbitrary, heterogeneous, or legacy enterprise payloads (e.g., from old ERP/CRM systems),
    uses Meta Llama 3 to transform it into canonical OrderPayload schema, and launches the Saga workflow.
    """
    logger.info(f"Received legacy payload with keys: {list(payload.keys())}")
    try:
        mapped_order = await ai_mapper_service.map_legacy_payload_to_order(payload)
        logger.info(f"Successfully mapped legacy payload to Order ID: {mapped_order.order_id}")
    except Exception as err:
        logger.exception(f"Failed to map legacy schema: {err}")
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"AI Schema Mapper could not map payload to canonical order model: {str(err)}"
        )

    saga_context = await _publish_saga_event(mapped_order)

    return WebhookResponse(
        success=True,
        saga_id=saga_context.saga_id,
        order_id=mapped_order.order_id,
        message="Legacy order mapped successfully via AI engine. Saga orchestration workflow initiated."
    )
