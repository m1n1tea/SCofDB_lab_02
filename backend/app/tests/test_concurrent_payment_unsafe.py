"""
Тест для демонстрации ПРОБЛЕМЫ race condition.

Этот тест должен ПРОХОДИТЬ, подтверждая, что при использовании
pay_order_unsafe() возникает двойная оплата.
"""

import asyncio
import pytest
import uuid
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

from app.application.payment_service import PaymentService


# TODO: Настроить подключение к тестовой БД
DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/marketplace"


@pytest.fixture
async def db_engine():
    engine = create_async_engine(DATABASE_URL, echo=True)
    yield engine
    await engine.dispose()

@pytest.fixture
async def db_session(db_engine):
    async with AsyncSession(db_engine) as session:
        yield session


@pytest.fixture
async def test_order(db_session):
    """
    Создать тестовый заказ со статусом 'created'.
    
    TODO: Реализовать фикстуру:
    1. Создать тестового пользователя
    2. Создать тестовый заказ со статусом 'created'
    3. Записать начальный статус в историю
    4. Вернуть order_id
    5. После теста - очистить данные
    """
    user_id = uuid.uuid4()
    await db_session.execute(
        text("""
            INSERT INTO users (id, email, name)
            VALUES (:id, :email, :name)
        """),
        {"id": user_id, "email": f"test_order_{uuid.uuid4()}@example.com", "name": "Test User"}
    )

    # Создаём заказ
    order_id = uuid.uuid4()
    status = 'created'
    await db_session.execute(
        text("""
            INSERT INTO orders (id, user_id, status, total_amount)
            VALUES (:id, :user_id, :status, 100.00)
        """),
        {"id": order_id, "user_id": user_id, "status": status}
    )

    # Записываем начальный статус в историю
    await db_session.execute(
        text("""
            INSERT INTO order_status_history (id, order_id, status, changed_at)
            VALUES (gen_random_uuid(), :order_id, :status, NOW())
        """),
        {"order_id": order_id, "status": status}
    )

    await db_session.commit()

    yield order_id

    # Очистка: удаляем заказ (каскадно удалит историю), затем пользователя
    await db_session.execute(
        text("DELETE FROM order_status_history WHERE order_id = :order_id"),
        {"order_id": order_id}
    )
    await db_session.execute(
        text("DELETE FROM orders WHERE id = :order_id"),
        {"order_id": order_id}
    )
    await db_session.execute(
        text("DELETE FROM users WHERE id = :user_id"),
        {"user_id": user_id}
    )
    await db_session.commit()


@pytest.mark.asyncio
async def test_concurrent_payment_unsafe_demonstrates_race_condition(db_session, test_order):
    """
    Тест демонстрирует проблему race condition при использовании pay_order_unsafe().
    
    ОЖИДАЕМЫЙ РЕЗУЛЬТАТ: Тест ПРОХОДИТ, подтверждая, что заказ был оплачен дважды.
    Это показывает, что метод pay_order_unsafe() НЕ защищен от конкурентных запросов.
    
    TODO: Реализовать тест следующим образом:
    
    1. Создать два экземпляра PaymentService с РАЗНЫМИ сессиями
       (это имитирует два независимых HTTP-запроса)
       
    2. Запустить два параллельных вызова pay_order_unsafe():
       
       async def payment_attempt_1():
           service1 = PaymentService(session1)
           return await service1.pay_order_unsafe(order_id)
           
       async def payment_attempt_2():
           service2 = PaymentService(session2)
           return await service2.pay_order_unsafe(order_id)
           
       results = await asyncio.gather(
           payment_attempt_1(),
           payment_attempt_2(),
           return_exceptions=True
       )
       
    3. Проверить историю оплат:
       
       service = PaymentService(session)
       history = await service.get_payment_history(order_id)
       
       # ОЖИДАЕМ ДВЕ ЗАПИСИ 'paid' - это и есть проблема!
       assert len(history) == 2, "Ожидалось 2 записи об оплате (RACE CONDITION!)"
       
    4. Вывести информацию о проблеме:
       
       print(f"⚠️ RACE CONDITION DETECTED!")
       print(f"Order {order_id} was paid TWICE:")
       for record in history:
           print(f"  - {record['changed_at']}: status = {record['status']}")
    """

    order_id = test_order

    engine = create_async_engine(DATABASE_URL)

    async def payment_attempt_1():
        async with AsyncSession(engine) as session1:
            service = PaymentService(session1)
            return await service.pay_order_unsafe(order_id)

    async def payment_attempt_2():
        async with AsyncSession(engine) as session2:
            service = PaymentService(session2)
            return await service.pay_order_unsafe(order_id)

    results = await asyncio.gather(
        payment_attempt_1(),
        payment_attempt_2(),
        return_exceptions=True
    )

    for i, res in enumerate(results, 1):
        # Проверяем, что это не исключение
        assert not isinstance(res, Exception), f"Attempt {i} failed with exception: {res}"
        # Проверяем, что это словарь (или содержит нужные ключи)
        assert isinstance(res, dict), f"Expected dict, got {type(res)}"
        assert res.get("status") == "paid", f"Unexpected status: {res}"

    service = PaymentService(db_session)
    history = await service.get_payment_history(order_id)

    assert len(history) == 2, "Ожидалось 2 записи об оплате (RACE CONDITION!)"
    assert all(r['status'] == 'paid' for r in history), "All records must have status 'paid'"


    print(f"\n⚠️  RACE CONDITION DETECTED!")
    print(f"Order {order_id} payment history records:")
    for record in history:
        print(f"  - {record['changed_at']}: status = {record['status']}")
    await engine.dispose()


if __name__ == "__main__":
    """
    Запуск теста:
    
    cd backend
    export PYTHONPATH=$(pwd)
    pytest app/tests/test_concurrent_payment_unsafe.py -v -s
    
    ОЖИДАЕМЫЙ РЕЗУЛЬТАТ:
    ✅ test_concurrent_payment_unsafe_demonstrates_race_condition PASSED
    
    Вывод должен показывать:
    ⚠️ RACE CONDITION DETECTED!
    Order XXX was paid TWICE:
      - 2024-XX-XX: status = paid
      - 2024-XX-XX: status = paid
    """
    pytest.main([__file__, "-v", "-s"])
