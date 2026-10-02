from pydantic import BaseModel, Field
from uuid import UUID
from typing import Optional
from enum import Enum


class OrderStatus(str, Enum):
    PENDING = "PENDING"
    CONFIRMED = "CONFIRMED"
    REJECTED = "REJECTED"


class OrderCreate(BaseModel):
    event_id: UUID
    user_id: UUID
    quantity: int = Field(default=1, ge=1)


class OrderResponse(BaseModel):
    order_id: UUID
    status: OrderStatus

    class Config:
        from_attributes = True


class OrderStatusResponse(BaseModel):
    order_id: UUID
    event_id: UUID
    user_id: UUID
    quantity: int
    status: OrderStatus
    reason: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    class Config:
        from_attributes = True