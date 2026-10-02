import uuid
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from worker.models import Event, Order, OrderStatus


async def process_order(db: AsyncSession, order_data: dict) -> bool:
    order_id = uuid.UUID(order_data["order_id"])
    event_id = uuid.UUID(order_data["event_id"])
    user_id = uuid.UUID(order_data["user_id"])
    quantity = order_data["quantity"]

    result = await db.execute(
        select(Order).where(Order.order_id == order_id)
    )
    existing_order = result.scalar_one_or_none()

    if existing_order:
        if existing_order.status in (OrderStatus.CONFIRMED, OrderStatus.REJECTED):
            return True
        order = existing_order
    else:
        order = Order(
            order_id=order_id,
            event_id=event_id,
            user_id=user_id,
            quantity=quantity,
            status=OrderStatus.PENDING,
        )
        db.add(order)

    update_result = await db.execute(
        update(Event)
        .where(Event.id == event_id)
        .where(Event.available_tickets >= quantity)
        .values(available_tickets=Event.available_tickets - quantity)
    )

    if update_result.rowcount == 0:
        order.status = OrderStatus.REJECTED
        order.reason = "sold_out"
    else:
        order.status = OrderStatus.CONFIRMED

    await db.commit()
    return True