import asyncio
import json
import logging
import random
from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4
import aio_pika

from app.core.config import settings
from app.core.rabbitmq import rabbitmq_client
from app.models.schemas import (
    InventoryStatus,
    PaymentStatus,
    SagaContext,
    SagaStatus,
    ShippingStatus,
    StepStatus,
)

logger = logging.getLogger(__name__)

class PaymentProcessingError(Exception):
    """Raised when payment transaction fails."""
    pass

class SagaOrchestrator:
    """
    Saga Orchestrator implementing a 3-step forward-recovery / backward-compensation state machine:
      Step 1: Reserve Inventory
      Step 2: Process Payment (Simulates 30% failure rate)
      Step 3: Dispatch Shipping
      Compensation: If Step 2 fails, roll back Step 1 (release inventory).
    """

    def __init__(self, failure_rate: float = 0.30):
        self.failure_rate = failure_rate
        self._consumer_task: Optional[asyncio.Task] = None
        self._is_running: bool = False

    # -------------------------------------------------------------------------
    # STEP 1: RESERVE INVENTORY & COMPENSATION
    # -------------------------------------------------------------------------
    async def step_reserve_inventory(self, context: SagaContext) -> InventoryStatus:
        logger.info(f"[{context.saga_id}] [STEP 1/3] Initiating inventory reservation for Order: {context.order.order_id}")
        await asyncio.sleep(0.5)  # Simulate async service latency
        
        reserved_ids = [item.item_id for item in context.order.items]
        reservation_id = f"RES-{uuid4().hex[:8].upper()}"
        
        status = InventoryStatus(
            status=StepStatus.SUCCESS,
            reservation_id=reservation_id,
            items_reserved=reserved_ids,
            detail=f"Successfully reserved {len(reserved_ids)} items."
        )
        context.inventory = status
        context.current_step = "INVENTORY_RESERVED"
        context.updated_at = datetime.now(timezone.utc)
        logger.info(f"[{context.saga_id}] [STEP 1/3] Inventory reserved: {reservation_id}")
        return status

    async def compensate_inventory(self, context: SagaContext) -> None:
        """Compensating Transaction: Release reserved inventory units."""
        logger.warning(f"[{context.saga_id}] [COMPENSATION] Executing inventory release for Order: {context.order.order_id}")
        await asyncio.sleep(0.3)  # Simulate async rollback call to inventory service
        
        if context.inventory and context.inventory.reservation_id:
            logger.info(f"[{context.saga_id}] [COMPENSATION] Releasing reservation ID: {context.inventory.reservation_id}")
            context.inventory.status = StepStatus.COMPENSATED
            context.inventory.detail = f"Released reservation {context.inventory.reservation_id} due to upstream failure."
        else:
            context.inventory = InventoryStatus(
                status=StepStatus.COMPENSATED,
                detail="Inventory reservation cancelled."
            )
        context.updated_at = datetime.now(timezone.utc)
        logger.info(f"[{context.saga_id}] [COMPENSATION] Inventory rollback complete.")

    # -------------------------------------------------------------------------
    # STEP 2: PROCESS PAYMENT & COMPENSATION
    # -------------------------------------------------------------------------
    async def step_process_payment(self, context: SagaContext) -> PaymentStatus:
        logger.info(f"[{context.saga_id}] [STEP 2/3] Processing payment of ${context.order.total_amount:.2f} for Order: {context.order.order_id}")
        await asyncio.sleep(0.5)  # Simulate gateway interaction
        
        # Simulate 30% failure rate
        if random.random() < self.failure_rate:
            error_msg = f"Payment declined: Insufficient funds or card processor timeout for order {context.order.order_id}"
            logger.error(f"[{context.saga_id}] [STEP 2/3] {error_msg}")
            context.payment = PaymentStatus(
                status=StepStatus.FAILED,
                amount=context.order.total_amount,
                currency=context.order.currency,
                failure_reason=error_msg
            )
            context.current_step = "PAYMENT_FAILED"
            context.updated_at = datetime.now(timezone.utc)
            raise PaymentProcessingError(error_msg)

        txn_id = f"TXN-{uuid4().hex[:10].upper()}"
        payment_status = PaymentStatus(
            status=StepStatus.SUCCESS,
            transaction_id=txn_id,
            amount=context.order.total_amount,
            currency=context.order.currency,
            failure_reason=None
        )
        context.payment = payment_status
        context.current_step = "PAYMENT_PROCESSED"
        context.updated_at = datetime.now(timezone.utc)
        logger.info(f"[{context.saga_id}] [STEP 2/3] Payment succeeded. Transaction: {txn_id}")
        return payment_status

    async def compensate_payment(self, context: SagaContext) -> None:
        """Compensating Transaction: Refund payment if subsequent steps fail."""
        if context.payment and context.payment.status == StepStatus.SUCCESS:
            logger.warning(f"[{context.saga_id}] [COMPENSATION] Refunding transaction {context.payment.transaction_id}")
            await asyncio.sleep(0.3)
            context.payment.status = StepStatus.COMPENSATED
            context.payment.failure_reason = "Transaction refunded during Saga compensation."
            context.updated_at = datetime.now(timezone.utc)

    # -------------------------------------------------------------------------
    # STEP 3: DISPATCH SHIPPING
    # -------------------------------------------------------------------------
    async def step_dispatch_shipping(self, context: SagaContext) -> ShippingStatus:
        logger.info(f"[{context.saga_id}] [STEP 3/3] Dispatching shipping for Order: {context.order.order_id}")
        await asyncio.sleep(0.5)  # Simulate logistics dispatch API
        
        tracking_no = f"TRK-{uuid4().hex[:12].upper()}"
        shipping_status = ShippingStatus(
            status=StepStatus.SUCCESS,
            tracking_number=tracking_no,
            carrier="FedEx Express",
            detail=f"Dispatched {len(context.order.items)} package(s) to customer {context.order.customer_id}."
        )
        context.shipping = shipping_status
        context.current_step = "SHIPPING_DISPATCHED"
        context.updated_at = datetime.now(timezone.utc)
        logger.info(f"[{context.saga_id}] [STEP 3/3] Shipping dispatched. Tracking: {tracking_no}")
        return shipping_status

    # -------------------------------------------------------------------------
    # SAGA STATE MACHINE EXECUTION
    # -------------------------------------------------------------------------
    async def execute_saga(self, context: SagaContext) -> SagaContext:
        """
        Executes the 3-step saga. Triggers automatic compensating transactions on failure.
        """
        logger.info(f"[{context.saga_id}] Starting Saga Execution for Order {context.order.order_id}")
        context.status = SagaStatus.IN_PROGRESS

        # Step 1: Reserve Inventory
        try:
            await self.step_reserve_inventory(context)
        except Exception as exc:
            logger.exception(f"[{context.saga_id}] Step 1 (Inventory) failed: {exc}")
            context.status = SagaStatus.FAILED
            context.error_message = f"Inventory step failed: {str(exc)}"
            return context

        # Step 2: Process Payment
        try:
            await self.step_process_payment(context)
        except PaymentProcessingError as exc:
            logger.warning(f"[{context.saga_id}] Step 2 (Payment) failed. Commencing compensation...")
            context.status = SagaStatus.COMPENSATING
            context.error_message = str(exc)
            
            # Rollback Step 1
            await self.compensate_inventory(context)
            
            context.status = SagaStatus.COMPENSATED
            logger.info(f"[{context.saga_id}] Saga successfully compensated and rolled back.")
            return context
        except Exception as exc:
            logger.exception(f"[{context.saga_id}] Unexpected error in Step 2: {exc}")
            context.status = SagaStatus.COMPENSATING
            context.error_message = f"Unexpected payment error: {str(exc)}"
            await self.compensate_inventory(context)
            context.status = SagaStatus.COMPENSATED
            return context

        # Step 3: Dispatch Shipping
        try:
            await self.step_dispatch_shipping(context)
        except Exception as exc:
            logger.exception(f"[{context.saga_id}] Step 3 (Shipping) failed. Commencing multi-step rollback: {exc}")
            context.status = SagaStatus.COMPENSATING
            context.error_message = f"Shipping step failed: {str(exc)}"
            
            # Rollback Step 2 & Step 1
            await self.compensate_payment(context)
            await self.compensate_inventory(context)
            
            context.status = SagaStatus.COMPENSATED
            return context

        # All Steps Completed Successfully
        context.status = SagaStatus.COMPLETED
        context.current_step = "SAGA_COMPLETED"
        context.updated_at = datetime.now(timezone.utc)
        logger.info(f"[{context.saga_id}] Saga completed successfully for Order {context.order.order_id}")
        return context

    # -------------------------------------------------------------------------
    # RABBITMQ CONSUMER WORKER
    # -------------------------------------------------------------------------
    async def start_consumer(self):
        """
        Starts listening to the RabbitMQ order saga queue and processes messages.
        """
        if not rabbitmq_client.channel or rabbitmq_client.channel.is_closed:
            logger.warning("RabbitMQ channel not ready. Consumer will defer startup.")
            return

        self._is_running = True
        queue = await rabbitmq_client.channel.get_queue(settings.ORDER_SAGA_QUEUE)
        logger.info(f"Saga Orchestrator consumer listening on queue '{settings.ORDER_SAGA_QUEUE}'...")

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                if not self._is_running:
                    break
                await self._process_incoming_message(message)

    async def _process_incoming_message(self, message: aio_pika.IncomingMessage):
        """
        Processes a single incoming aio_pika message, validates schema, and invokes Saga.
        """
        async with message.process(requeue=False, reject_on_redelivered=True):
            try:
                payload = json.loads(message.body.decode("utf-8"))
                logger.info(f"Received message from queue. Correlation ID: {message.correlation_id}")
                
                saga_context = SagaContext.model_validate(payload)
                await self.execute_saga(saga_context)
                
            except json.JSONDecodeError as err:
                logger.error(f"Malformed JSON in message. Rejecting to DLQ: {err}")
                # message.process with requeue=False will automatically route to DLQ on exception
                raise
            except Exception as err:
                logger.exception(f"Unhandled error while processing saga message: {err}")
                raise

    def stop_consumer(self):
        """Signals the consumer loop to stop."""
        self._is_running = False
        if self._consumer_task and not self._consumer_task.done():
            self._consumer_task.cancel()
        logger.info("Saga Orchestrator consumer stopped.")

orchestrator_service = SagaOrchestrator()
