import uuid
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.schemas import OrderCreate, OrderResponse, OrderStatusResponse, OrderStatus
from app.db import get_db
from app.kafka_producer import kafka_producer
from app.models import Order

router = APIRouter(prefix="/orders", tags=["orders"])


@router.post("", response_model=OrderResponse, status_code=status.HTTP_202_ACCEPTED)
async def create_order(order_data: OrderCreate):
    order_id = uuid.uuid4()
    
    message = {
        "order_id": str(order_id),
        "event_id": str(order_data.event_id),
        "user_id": str(order_data.user_id),
        "quantity": order_data.quantity,
    }
    
    kafka_producer.send_order_request(str(order_data.event_id), message)
    
    return OrderResponse(order_id=order_id, status=OrderStatus.PENDING)


@router.get("/{order_id}", response_model=OrderStatusResponse)
async def get_order_status(order_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Order).where(Order.order_id == order_id))
    order = result.scalar_one_or_none()
    
    if not order:
        return OrderStatusResponse(
            order_id=order_id,
            event_id=uuid.UUID("00000000-0000-0000-0000-000000000000"),
            user_id=uuid.UUID("00000000-0000-0000-0000-000000000000"),
            quantity=0,
            status=OrderStatus.PENDING,
        )
    
    return OrderStatusResponse(
        order_id=order.order_id,
        event_id=order.event_id,
        user_id=order.user_id,
        quantity=order.quantity,
        status=order.status,
        reason=order.reason,
        created_at=order.created_at.isoformat() if order.created_at else None,
        updated_at=order.updated_at.isoformat() if order.updated_at else None,
    )