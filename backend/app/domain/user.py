"""Доменная сущность пользователя."""
import re
import uuid
from datetime import datetime
from dataclasses import dataclass, field
from .exceptions import InvalidEmailError


@dataclass
class User:
    email: str
    name: str | None = None
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    created_at: datetime = field(default_factory=datetime.now)

    def __post_init__(self) -> None:
        pattern = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9.-]+$"
        if not re.match(pattern, self.email):
            raise InvalidEmailError(f"Неверный формат email: {self.email}")