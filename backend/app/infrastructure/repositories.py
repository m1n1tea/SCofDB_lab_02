"""Реализация репозиториев с использованием SQLAlchemy."""

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Optional, List

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.user import User
from app.domain.order import Order, OrderItem, OrderStatus, OrderStatusChange


class UserRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def save(self, user: User) -> None:
        stmt = text("""
            INSERT INTO users (id, email, name, created_at)
            VALUES (:id, :email, :name, :created_at)
            ON CONFLICT (email) DO UPDATE SET
                name = EXCLUDED.name
        """)
        await self.session.execute(stmt, {
            "id": user.id,
            "email": user.email,
            "name": user.name,
            "created_at": user.created_at
        })
        await self.session.flush()

    async def find_by_id(self, user_id: uuid.UUID) -> Optional[User]:
        stmt = text("SELECT id, email, name, created_at FROM users WHERE id = :id")
        result = await self.session.execute(stmt, {"id": user_id})
        row = result.first()
        if row:
            return User(
                id=row[0],
                email=row[1],
                name=row[2],
                created_at=row[3]
            )
        return None

    async def find_by_email(self, email: str) -> Optional[User]:
        stmt = text("SELECT id, email, name, created_at FROM users WHERE email = :email")
        result = await self.session.execute(stmt, {"email": email})
        row = result.first()
        if row:
            return User(
                id=row[0],
                email=row[1],
                name=row[2],
                created_at=row[3]
            )
        return None

    async def find_all(self) -> List[User]:
        stmt = text("SELECT id, email, name, created_at FROM users")
        result = await self.session.execute(stmt)
        rows = result.all()
        return [
            User(id=r[0], email=r[1], name=r[2], created_at=r[3])
            for r in rows
        ]


class OrderRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def save(self, order: Order) -> None:
        stmt_order = text("""
            INSERT INTO orders (id, user_id, status, total_amount, created_at)
            VALUES (:id, :user_id, :status, :total_amount, :created_at)
            ON CONFLICT (id) DO UPDATE SET
                user_id = EXCLUDED.user_id,
                status = EXCLUDED.status,
                total_amount = EXCLUDED.total_amount
        """)
        await self.session.execute(stmt_order, {
            "id": order.id,
            "user_id": order.user_id,
            "status": order.status.value,
            "total_amount": order.total_amount,
            "created_at": order.created_at
        })
        await self.session.execute(
            text("DELETE FROM order_items WHERE order_id = :order_id"),
            {"order_id": order.id}
        )

        if order.items:
            items_data = [
                {
                    "id": item.id,
                    "order_id": order.id,
                    "product_name": item.product_name,
                    "price": item.price,
                    "quantity": item.quantity
                }
                for item in order.items
            ]
            stmt_items = text("""
                INSERT INTO order_items (id, order_id, product_name, price, quantity)
                VALUES (:id, :order_id, :product_name, :price, :quantity)
            """)
            for data in items_data:
                await self.session.execute(stmt_items, data)

        await self.session.execute(
            text("DELETE FROM order_status_history WHERE order_id = :order_id"),
            {"order_id": order.id}
        )

        if order.status_history:
            history_data = [
                {
                    "id": hist.id,
                    "order_id": order.id,
                    "status": hist.status,
                    "changed_at": hist.changed_at
                }
                for hist in order.status_history
            ]
            stmt_history = text("""
                INSERT INTO order_status_history (id, order_id, status, changed_at)
                VALUES (:id, :order_id, :status, :changed_at)
            """)
            for data in history_data:
                await self.session.execute(stmt_history, data)

        await self.session.flush()

    async def find_by_id(self, order_id: uuid.UUID) -> Optional[Order]:
        stmt_order = text("""
            SELECT id, user_id, status, total_amount, created_at
            FROM orders
            WHERE id = :id
        """)
        result = await self.session.execute(stmt_order, {"id": order_id})
        order_row = result.first()
        if not order_row:
            return None

        stmt_items = text("""
            SELECT id, product_name, price, quantity
            FROM order_items
            WHERE order_id = :order_id
        """)
        result_items = await self.session.execute(stmt_items, {"order_id": order_id})
        items_rows = result_items.all()
        items = [
            OrderItem(
                id=row[0],
                product_name=row[1],
                price=row[2],
                quantity=row[3],
                order_id=order_id
            )
            for row in items_rows
        ]

        stmt_history = text("""
            SELECT id, status, changed_at
            FROM order_status_history
            WHERE order_id = :order_id
            ORDER BY changed_at
        """)
        result_history = await self.session.execute(stmt_history, {"order_id": order_id})
        history_rows = result_history.all()
        history = [
            OrderStatusChange(
                id=row[0],
                status=OrderStatus(row[1]),
                changed_at=row[2],
                order_id=order_id
            )
            for row in history_rows
        ]

        order = object.__new__(Order)
        order.id = order_row[0]
        order.user_id = order_row[1]
        order.status = OrderStatus(order_row[2])
        order.total_amount = order_row[3]
        order.created_at = order_row[4]
        order.items = items
        order.status_history = history
        return order

    async def find_by_user(self, user_id: uuid.UUID) -> List[Order]:
        stmt_orders = text("""
            SELECT id, user_id, status, total_amount, created_at
            FROM orders
            WHERE user_id = :user_id
        """)
        result = await self.session.execute(stmt_orders, {"user_id": user_id})
        order_rows = result.all()
        if not order_rows:
            return []

        order_ids = [row[0] for row in order_rows]

        stmt_items = text("""
            SELECT order_id, id, product_name, price, quantity
            FROM order_items
            WHERE order_id = ANY(:order_ids)
        """)
        result_items = await self.session.execute(stmt_items, {"order_ids": order_ids})
        items_rows = result_items.all()
        items_by_order = {}
        for row in items_rows:
            items_by_order.setdefault(row[0], []).append(
                OrderItem(
                    id=row[1],
                    product_name=row[2],
                    price=row[3],
                    quantity=row[4],
                    order_id = row[0]
                )
            )

        stmt_history = text("""
            SELECT order_id, id, status, changed_at
            FROM order_status_history
            WHERE order_id = ANY(:order_ids)
            ORDER BY changed_at
        """)
        result_history = await self.session.execute(stmt_history, {"order_ids": order_ids})
        history_rows = result_history.all()
        history_by_order = {}
        for row in history_rows:
            history_by_order.setdefault(row[0], []).append(
                OrderStatusChange(
                    id=row[1],
                    status=OrderStatus(row[2]),
                    changed_at=row[3],
                    order_id = row[0]
                )
            )

        orders = []
        for row in order_rows:
            order_id = row[0]
            order = object.__new__(Order)
            order.id = order_id
            order.user_id = row[1]
            order.status = OrderStatus(row[2])
            order.total_amount = row[3]
            order.created_at = row[4]
            order.items = items_by_order.get(order_id, [])
            order.status_history = history_by_order.get(order_id, [])
            orders.append(order)

        return orders

    async def find_all(self) -> List[Order]:
        stmt_orders = text("SELECT id, user_id, status, total_amount, created_at FROM orders")
        result = await self.session.execute(stmt_orders)
        order_rows = result.all()
        if not order_rows:
            return []

        order_ids = [row[0] for row in order_rows]

        stmt_items = text("""
            SELECT order_id, id, product_name, price, quantity
            FROM order_items
            WHERE order_id = ANY(:order_ids)
        """)
        result_items = await self.session.execute(stmt_items, {"order_ids": order_ids})
        items_rows = result_items.all()
        items_by_order = {}
        for row in items_rows:
            items_by_order.setdefault(row[0], []).append(
                OrderItem(
                    id=row[1],
                    product_name=row[2],
                    price=row[3],
                    quantity=row[4],
                    order_id = row[0]
                )
            )

        stmt_history = text("""
            SELECT order_id, id, status, changed_at
            FROM order_status_history
            WHERE order_id = ANY(:order_ids)
            ORDER BY changed_at
        """)
        result_history = await self.session.execute(stmt_history, {"order_ids": order_ids})
        history_rows = result_history.all()
        history_by_order = {}
        for row in history_rows:
            history_by_order.setdefault(row[0], []).append(
                OrderStatusChange(
                    id=row[1],
                    status=OrderStatus(row[2]),
                    changed_at=row[3],
                    order_id = row[0]
                )
            )

        orders = []
        for row in order_rows:
            order_id = row[0]
            order = object.__new__(Order)
            order.id = order_id
            order.user_id = row[1]
            order.status = OrderStatus(row[2])
            order.total_amount = row[3]
            order.created_at = row[4]
            order.items = items_by_order.get(order_id, [])
            order.status_history = history_by_order.get(order_id, [])
            orders.append(order)

        return orders