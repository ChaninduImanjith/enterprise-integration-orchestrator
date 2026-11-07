from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import uuid4
from pydantic import BaseModel, Field

class StepStatus(str, Enum):
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    COMPENSATED = "COMPENSATED"

class SagaStatus(str, Enum):
    STARTED = "STARTED"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETING = "COMPLETING"
    COMPLETED = "COMPLETED"
    COMPENSATING = "COMPENSATING"
    COMPENSATED = "COMPENSATED"
    FAILED = "FAILED"

class OrderItem(BaseModel):
    item_id: str = Field(..., description="Unique product/item identifier")
    name: str = Field(..., description="Name of the product")
    quantity: int = Field(..., gt=0, description="Quantity of units")
    unit_price: float = Field(..., ge=0.0, description="Price per unit")

class OrderPayload(BaseModel):
    order_id: str = Field(default_factory=lambda: f"ORD-{uuid4().hex[:8].upper()}", description="Unique order reference")
    customer_id: str = Field(..., description="Customer identifier")
    customer_email: str = Field(..., description="Customer email address for notifications")
    currency: str = Field(default="USD", description="Currency code (e.g. USD, EUR)")
    total_amount: float = Field(..., gt=0.0, description="Total order amount")
    items: List[OrderItem] = Field(..., min_length=1, description="List of items in the order")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Arbitrary enterprise metadata")

class InventoryStatus(BaseModel):
    step_name: str = "INVENTORY"
    status: StepStatus
    reservation_id: Optional[str] = None
    items_reserved: List[str] = Field(default_factory=list)
    detail: Optional[str] = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class PaymentStatus(BaseModel):
    step_name: str = "PAYMENT"
    status: StepStatus
    transaction_id: Optional[str] = None
    amount: float
    currency: str = "USD"
    failure_reason: Optional[str] = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class ShippingStatus(BaseModel):
    step_name: str = "SHIPPING"
    status: StepStatus
    tracking_number: Optional[str] = None
    carrier: Optional[str] = None
    detail: Optional[str] = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class SagaContext(BaseModel):
    saga_id: str = Field(default_factory=lambda: f"SAGA-{uuid4().hex[:12].upper()}")
    order: OrderPayload
    current_step: str = "INITIALIZED"
    status: SagaStatus = SagaStatus.STARTED
    inventory: Optional[InventoryStatus] = None
    payment: Optional[PaymentStatus] = None
    shipping: Optional[ShippingStatus] = None
    error_message: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class WorkflowEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    event_type: str = Field(..., description="Event action identifier, e.g. saga.order.start")
    saga_id: str = Field(..., description="Associated Saga ID")
    payload: Dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class WebhookResponse(BaseModel):
    success: bool
    saga_id: str
    order_id: str
    message: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

