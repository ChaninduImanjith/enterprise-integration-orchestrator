from datetime import datetime, timezone
from typing import Any, Dict, Optional
from uuid import uuid4
from pydantic import BaseModel, Field

class IntegrationMessage(BaseModel):
    """
    Standard message envelope for enterprise integration events.
    Follows CloudEvents/Enterprise Integration Patterns convention.
    """
    id: str = Field(default_factory=lambda: str(uuid4()), description="Unique message/event ID")
    event_type: str = Field(..., description="Type of event, e.g. order.created, user.synced")
    source: str = Field(..., description="System origin/producer of the message")
    target: Optional[str] = Field(None, description="Intended consumer or target system")
    correlation_id: str = Field(default_factory=lambda: str(uuid4()), description="Traceability / correlation ID across microservices")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="UTC timestamp of event creation")
    payload: Dict[str, Any] = Field(default_factory=dict, description="Event body / data payload")
    headers: Dict[str, str] = Field(default_factory=dict, description="Custom message headers and metadata")

class PublishMessageRequest(BaseModel):
    event_type: str = Field(..., example="inventory.item.updated")
    source: str = Field(..., example="crm-service")
    target: Optional[str] = Field(None, example="erp-service")
    routing_key: Optional[str] = Field(None, description="Optional custom AMQP routing key. Defaults to event_type.")
    payload: Dict[str, Any] = Field(default_factory=dict, example={"item_id": 1024, "stock": 50})
    headers: Optional[Dict[str, str]] = Field(default_factory=dict)

class PublishMessageResponse(BaseModel):
    success: bool
    message_id: str
    correlation_id: str
    routing_key: str
    exchange: str
    detail: Optional[str] = None

