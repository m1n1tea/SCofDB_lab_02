"""Доменные сущности заказа."""

import uuid
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import List
from dataclasses import dataclass, field

from .exceptions import (
    OrderAlreadyPaidError,
    OrderCancelledError,
    InvalidQuantityError,
    InvalidPriceError,
    InvalidAmountError,
)


class OrderStatus(str, Enum):
    CREATED = "created"
    PAID = "paid"
    CANCELLED = "cancelled"
    SHIPPED = "shipped"
    COMPLETED = "completed"


@dataclass
class OrderItem:
    product_name: str
    price: Decimal
    quantity: int
    order_id: uuid.UUID | None = None
    id: uuid.UUID = field(default_factory=uuid.uuid4)

    def __post_init__(self):
        if self.quantity <= 0:
            raise InvalidQuantityError("Количество должно быть положительным")
        if self.price < 0:
            raise InvalidPriceError("Цена не может быть отрицательной")

    @property
    def subtotal(self) -> Decimal:
        return self.price * self.quantity


@dataclass
class OrderStatusChange:
    order_id: uuid.UUID
    status: OrderStatus
    changed_at: datetime = field(default_factory=datetime.now)
    id: uuid.UUID = field(default_factory=uuid.uuid4)

@dataclass
class Order:
    user_id: uuid.UUID
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    status: OrderStatus = OrderStatus.CREATED
    total_amount: Decimal = Decimal('0')
    created_at: datetime = field(default_factory=datetime.now)
    items: List[OrderItem] = field(default_factory=list)
    status_history: List[OrderStatusChange] = field(default_factory=list)

    def _change_status(self, new_status: OrderStatus):
        self.status = new_status
        self.status_history.append(
            OrderStatusChange(order_id=self.id, status=new_status)
        )

    def __post_init__(self):
        self._change_status(self.status)

    def add_item(self, product_name: str, price: Decimal, quantity: int) -> OrderItem:
        if self.status == OrderStatus.CANCELLED:
            raise OrderCancelledError(self.id)
        if self.status != OrderStatus.CREATED:
            raise ValueError(f"Заказ {self.id} уже в обработке, нельзя добавлять новые товары.")

        item = OrderItem(
            product_name=product_name,
            price=price,
            quantity=quantity,
            order_id=self.id
        )
        self.items.append(item)
        self.total_amount += item.subtotal
        if self.total_amount < 0:
            raise InvalidAmountError(self.total_amount)
        return item

    def pay(self) -> None:
        for status_change in self.status_history:
            if status_change.status == OrderStatus.PAID:
                raise OrderAlreadyPaidError(self.id)
        if self.status == OrderStatus.CANCELLED:
            raise OrderCancelledError(self.id)

        self._change_status(OrderStatus.PAID)

    def cancel(self) -> None:
        if self.status == OrderStatus.CANCELLED:
            raise OrderCancelledError(self.id)
        
        for status_change in self.status_history:
            if status_change.status == OrderStatus.PAID:
                raise OrderAlreadyPaidError(self.id)
        
        self._change_status(OrderStatus.CANCELLED)

    def ship(self) -> None:
        if self.status != OrderStatus.PAID:
            raise ValueError(f"Отправить можно только оплаченный заказ. Заказ {self.id}")
        self._change_status(OrderStatus.SHIPPED)

    def complete(self) -> None:
        if self.status != OrderStatus.SHIPPED:
            raise ValueError(f"Завершить можно только отправленный заказ. Заказ {self.id}")
        self._change_status(OrderStatus.COMPLETED)