import logging
from fastapi import APIRouter, HTTPException, status
from app.core.config import settings
from app.core.rabbitmq import rabbitmq_client
from app.models.schemas import OrderPayload, SagaContext, SagaStatus, WebhookResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/webhooks", tags=["Webhooks & Ingestion"])

@router.post(
    "/order",
    response_model=WebhookResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Ingest order and initiate asynchronous Saga workflow"
)
async def receive_order_webhook(order: OrderPayload):
    """
    Accepts an incoming enterprise order payload, initializes a new Saga state context,
    and publishes the initiation event to the RabbitMQ workflow exchange.
    """
    # 1. Initialize Saga Context
    saga_context = SagaContext(
        order=order,
        status=SagaStatus.STARTED,
        current_step="ORDER_RECEIVED"
    )

    logger.info(f"Initiating Saga {saga_context.saga_id} for order {order.order_id}")

    # 2. Check broker connection
    if not rabbitmq_client.channel or rabbitmq_client.channel.is_closed:
        logger.error("RabbitMQ broker connection is unavailable.")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Message broker is currently unavailable. Workflow could not be queued."
        )

    # 3. Publish initial saga event
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

    return WebhookResponse(
        success=True,
        saga_id=saga_context.saga_id,
        order_id=order.order_id,
        message="Order accepted successfully. Saga orchestration workflow initiated."
    )

