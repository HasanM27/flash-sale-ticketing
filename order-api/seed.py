import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from app.models import Event

DATABASE_URL = "postgresql+asyncpg://admin:password@localhost:5432/flash_sale"

async def seed_data():
    engine = create_async_engine(DATABASE_URL, echo=True)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session() as session:
        # Create one test event with exactly 10 tickets
        test_event = Event(
            name="Taylor Swift - VIP Front Row",
            total_tickets=10,
            available_tickets=10
        )
        session.add(test_event)
        await session.commit()
        print(f"\n--- SUCCESS ---")
        print(f"Seeded Event: '{test_event.name}'")
        print(f"Save this Event ID for testing: {test_event.id}\n")

if __name__ == "__main__":
    asyncio.run(seed_data())