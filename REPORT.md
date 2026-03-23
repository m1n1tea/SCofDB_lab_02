# Отчёт по лабораторной работе №2
## Управление конкурентными транзакциями в маркетплейсе

**Студент:** _[Макуров Михаил БПМ-22-ПО-3]_  
**Дата:** _[23.03.2026]_

---

## Раздел 1: Описание проблемы

### Что такое Race Condition?

_TODO: Объясните, что такое race condition в контексте баз данных._

Race condition (состояние гонки) — это ситуация, когда...

**Пример из жизни:**
- ...

### Почему READ COMMITTED не защищает от двойной оплаты?

_TODO: Объясните механизм работы READ COMMITTED и почему он не предотвращает race condition._

На уровне изоляции READ COMMITTED:
1. Каждая транзакция видит...
2. Когда две транзакции одновременно...
3. В результате...

**Демонстрация проблемы:**

```
Время | Сессия 1                    | Сессия 2
------|----------------------------|---------------------------
t1    | BEGIN                      |
t2    | SELECT status (created)    |
t3    |                            | BEGIN
t4    |                            | SELECT status (created)
t5    | pg_sleep(3)                |
t6    |                            | pg_sleep(3)
t7    | UPDATE status = paid       |
t8    |                            | UPDATE status = paid (ждет)
t9    | COMMIT                     |
t10   |                            | UPDATE выполняется...
t11   |                            | COMMIT
```

_TODO: Объясните, что происходит на каждом этапе._

### Примеры из реальной жизни

_TODO: Приведите реальные примеры, когда может возникнуть такая проблема._

1. **Двойной клик на кнопку "Оплатить"**
   - Пользователь нажимает кнопку дважды
   - Два HTTP-запроса приходят почти одновременно
   - Оба запроса начинают обработку параллельно

2. **Микросервисная архитектура**
   - ...

3. **Сбой сети**
   - ...

---

## Раздел 2: Уровни изоляции в PostgreSQL

### READ UNCOMMITTED

**Описание:**
_TODO: Опишите этот уровень изоляции._

**Предотвращает:**
- _TODO: Список аномалий, которые предотвращаются._

**Не предотвращает:**
- Dirty reads
- Non-repeatable reads
- Phantom reads

**Когда использовать:**
_TODO: В каких случаях этот уровень подходит._

**Особенность в PostgreSQL:**
В PostgreSQL READ UNCOMMITTED работает как READ COMMITTED из-за архитектуры MVCC.

---

### READ COMMITTED (по умолчанию)

**Описание:**
_TODO: Опишите этот уровень изоляции._

Каждый SELECT в транзакции видит...

**Предотвращает:**
- ✅ Dirty reads (чтение незакоммиченных данных)

**Не предотвращает:**
- ❌ Non-repeatable reads (повторное чтение дает другой результат)
- ❌ Phantom reads (новые строки появляются в результате запроса)

**Пример non-repeatable read:**
```sql
-- Сессия 1
BEGIN;
SELECT balance FROM accounts WHERE id = 1; -- Результат: 1000
-- Сессия 2 изменяет balance и делает COMMIT
SELECT balance FROM accounts WHERE id = 1; -- Результат: 500 (!)
COMMIT;
```

**Когда использовать:**
_TODO: В каких случаях этот уровень подходит._

---

### REPEATABLE READ

**Описание:**
_TODO: Опишите этот уровень изоляции._

Транзакция видит snapshot данных на момент начала...

**Предотвращает:**
- ✅ Dirty reads
- ✅ Non-repeatable reads
- ✅ Phantom reads (в PostgreSQL благодаря MVCC)

**Не предотвращает:**
- ❌ Write skew (специфичная аномалия)
- ❌ Некоторые сериализационные аномалии

**Особенность в PostgreSQL:**
В отличие от стандарта SQL, PostgreSQL на REPEATABLE READ также предотвращает phantom reads.

**Когда использовать:**
_TODO: В каких случаях этот уровень подходит._

---

### SERIALIZABLE

**Описание:**
_TODO: Опишите этот уровень изоляции._

Самый строгий уровень, гарантирующий...

**Предотвращает:**
- ✅ Все аномалии чтения
- ✅ Сериализационные аномалии
- ✅ Write skew

**Недостатки:**
- ❌ Может откатывать транзакции при конфликтах (serialization failure)
- ❌ Снижение производительности
- ❌ Требует retry logic в приложении

**Пример serialization failure:**
```sql
-- Сессия 1
BEGIN ISOLATION LEVEL SERIALIZABLE;
SELECT SUM(balance) FROM accounts; -- 5000
UPDATE accounts SET balance = balance + 100 WHERE id = 1;

-- Сессия 2 (параллельно)
BEGIN ISOLATION LEVEL SERIALIZABLE;
SELECT SUM(balance) FROM accounts; -- 5000
UPDATE accounts SET balance = balance + 200 WHERE id = 2;

-- При COMMIT одна из транзакций получит ошибку:
-- ERROR: could not serialize access due to read/write dependencies
```

**Когда использовать:**
_TODO: В каких случаях этот уровень подходит._

---

### Сравнительная таблица

| Уровень изоляции  | Dirty Read | Non-Repeatable Read | Phantom Read | Performance | Use Case |
|-------------------|------------|---------------------|--------------|-------------|----------|
| READ UNCOMMITTED  | ❌          | ❌                   | ❌            | Высокая     | Аналитика (неточная) |
| READ COMMITTED    | ✅          | ❌                   | ❌            | Высокая     | Обычные операции |
| REPEATABLE READ   | ✅          | ✅                   | ✅*           | Средняя     | Критичные операции |
| SERIALIZABLE      | ✅          | ✅                   | ✅            | Низкая      | Финансовые транзакции |

_*В PostgreSQL REPEATABLE READ также предотвращает phantom reads благодаря MVCC._

---

## Раздел 3: Решение проблемы

### Почему REPEATABLE READ решает проблему?

_TODO: Объясните, как REPEATABLE READ помогает решить проблему конкурентных оплат._

REPEATABLE READ использует snapshot isolation:
1. Транзакция видит...
2. Это означает, что...
3. Однако, без блокировок...

### Зачем нужен FOR UPDATE?

_TODO: Объясните роль FOR UPDATE в решении проблемы._

`FOR UPDATE` создает эксклюзивную блокировку на уровне строки (row-level lock):

**Без FOR UPDATE:**
```sql
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT status FROM orders WHERE id = '...';
-- Другая транзакция может прочитать ту же строку
UPDATE orders SET status = 'paid' WHERE id = '...';
COMMIT;
```

**С FOR UPDATE:**
```sql
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT status FROM orders WHERE id = '...' FOR UPDATE;
-- Другая транзакция ЖДЕТ освобождения блокировки
UPDATE orders SET status = 'paid' WHERE id = '...';
COMMIT;
```

**Типы блокировок:**
- `FOR UPDATE` — эксклюзивная блокировка (никто не может изменить или заблокировать)
- `FOR SHARE` — разделяемая блокировка (другие могут читать с FOR SHARE, но не могут изменять)
- `FOR NO KEY UPDATE` — как FOR UPDATE, но разрешает concurrent FOR KEY SHARE
- `FOR KEY SHARE` — слабая блокировка (предотвращает DELETE и UPDATE ключевых полей)

### Что произойдет без FOR UPDATE на REPEATABLE READ?

_TODO: Опишите сценарий, когда REPEATABLE READ без FOR UPDATE не работает._

Даже на REPEATABLE READ возможна аномалия:

```sql
-- Сессия 1
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT status FROM orders WHERE id = '...'; -- created
-- Задержка...
UPDATE orders SET status = 'paid' WHERE id = '...' AND status = 'created';
COMMIT;

-- Сессия 2 (параллельно)
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT status FROM orders WHERE id = '...'; -- created (snapshot!)
-- Задержка...
UPDATE orders SET status = 'paid' WHERE id = '...' AND status = 'created';
-- ЧТО ПРОИЗОЙДЕТ?
```

_TODO: Объясните, что произойдет в этом случае._

### Разница между FOR UPDATE и FOR SHARE

| Характеристика | FOR UPDATE | FOR SHARE |
|----------------|------------|-----------|
| Тип блокировки | Эксклюзивная | Разделяемая |
| Блокирует чтение с FOR UPDATE | ✅ | ✅ |
| Блокирует чтение с FOR SHARE | ✅ | ❌ |
| Блокирует UPDATE | ✅ | ✅ |
| Блокирует DELETE | ✅ | ✅ |
| Use case | Перед изменением | Защита от изменений |

**Пример использования FOR SHARE:**
```sql
-- Проверка наличия товара перед созданием заказа
SELECT quantity FROM products WHERE id = '...' FOR SHARE;
-- Товар не может быть удален или изменен до COMMIT
```

---

## Раздел 4: Рекомендации для продакшена

### Какой ISOLATION LEVEL использовать для продакшена маркетплейса?

_TODO: Дайте обоснованную рекомендацию по выбору уровня изоляции для продакшена._

**Рекомендация:** Для продакшена маркетплейса рекомендуется использовать **READ COMMITTED как default уровень** с явным использованием **REPEATABLE READ + FOR UPDATE для критичных операций**.

#### Обоснование:

**1. Производительность**
- READ COMMITTED имеет минимальный overhead
- REPEATABLE READ используется только там, где необходимо
- Избегаем глобального использования SERIALIZABLE

_TODO: Расширьте обоснование._

**2. Безопасность данных**
- Критичные операции (оплата, изменение баланса) защищены через REPEATABLE READ + FOR UPDATE
- Некритичные операции (просмотр каталога) работают на READ COMMITTED
- Четкое разделение зон ответственности

_TODO: Расширьте обоснование._

**3. Риски deadlock**
- FOR UPDATE может привести к deadlock при неправильном порядке блокировок
- Решение: всегда блокировать ресурсы в одном порядке
- Пример: сначала order, потом order_items, потом payment

_TODO: Расширьте обоснование._

**4. Простота разработки**
- READ COMMITTED легко понять и отлаживать
- REPEATABLE READ + FOR UPDATE требует explicit intent
- Код становится самодокументируемым

_TODO: Расширьте обоснование._

**5. Масштабируемость**
- READ COMMITTED хорошо масштабируется
- Локальное использование блокировок не создает bottleneck
- Connection pooling работает эффективно

_TODO: Расширьте обоснование._

### Альтернативные подходы

#### Подход 1: Использовать SERIALIZABLE везде

**Плюсы:**
- Максимальная корректность
- Не нужно думать о блокировках
- PostgreSQL автоматически обнаруживает конфликты

**Минусы:**
- Значительное снижение производительности (20-50%)
- Требует retry logic во всех операциях
- Высокий процент rollback при нагрузке
- Сложная отладка serialization failures

**Вывод:** Не рекомендуется для high-load систем.

#### Подход 2: Optimistic Locking (версионирование)

**Реализация:**
```sql
ALTER TABLE orders ADD COLUMN version INTEGER DEFAULT 1;

UPDATE orders 
SET status = 'paid', version = version + 1
WHERE id = '...' AND status = 'created' AND version = 1;

-- Проверить ROW_COUNT, если 0 — конфликт
```

**Плюсы:**
- Нет блокировок на чтение
- Хорошая производительность
- Масштабируется горизонтально

**Минусы:**
- Требует изменения схемы БД
- Требует retry logic в приложении
- Может быть много конфликтов при высокой нагрузке

**Вывод:** Хороший подход для распределенных систем.

#### Подход 3: Advisory Locks

**Реализация:**
```sql
BEGIN;
SELECT pg_advisory_xact_lock(hashtext('order_' || order_id));
-- Критическая секция
COMMIT; -- Блокировка автоматически снимается
```

**Плюсы:**
- Гибкий контроль блокировок
- Можно блокировать логические ресурсы
- Работает на любом уровне изоляции

**Минусы:**
- Легко забыть снять блокировку
- Сложнее отлаживать
- Требует дисциплины от разработчиков

**Вывод:** Полезно для специфичных случаев, но не как основной подход.

### Итоговая рекомендация

Для продакшена маркетплейса используйте **гибридный подход**:

1. **Default isolation level:** `READ COMMITTED`
   - Для обычных операций (чтение каталога, просмотр истории)
   - 95% операций

2. **REPEATABLE READ + FOR UPDATE:** для критичных операций
   - Оплата заказа
   - Изменение баланса
   - Резервирование товара
   - 5% операций

3. **Дополнительно:** Optimistic locking для операций с высокой конкурентностью
   - Обновление счетчиков
   - Изменение рейтингов

**Пример кода:**
```python
async def pay_order(order_id: UUID):
    async with db.transaction(isolation='repeatable_read'):
        # Заблокировать заказ
        order = await db.fetch_one(
            "SELECT * FROM orders WHERE id = $1 FOR UPDATE",
            order_id
        )
        
        if order['status'] != 'created':
            raise OrderAlreadyPaidError()
        
        # Обновить статус
        await db.execute(
            "UPDATE orders SET status = 'paid' WHERE id = $1",
            order_id
        )
        
        # Записать в историю
        await db.execute(
            "INSERT INTO order_status_history (order_id, status) VALUES ($1, 'paid')",
            order_id
        )
```

---

## Заключение

_TODO: Напишите краткое заключение о проделанной работе._

В ходе лабораторной работы было изучено:
1. ...
2. ...
3. ...

Основные выводы:
- ...
- ...

---

## Приложение: Результаты тестирования

### Тест 1: Демонстрация проблемы (READ COMMITTED)

_TODO: Вставьте скриншоты или вывод команд, демонстрирующие двойную оплату._

```
# История статусов после теста
 id | order_id | status | changed_at
----|----------|--------|------------
 .. | ...      | created | 2024-...
 .. | ...      | paid    | 2024-... <-- Сессия 1
 .. | ...      | paid    | 2024-... <-- Сессия 2 (ПРОБЛЕМА!)
```

### Тест 2: Решение проблемы (REPEATABLE READ + FOR UPDATE)

_TODO: Вставьте скриншоты или вывод команд, демонстрирующие корректную работу._

```
# История статусов после теста
 id | order_id | status | changed_at
----|----------|--------|------------
 .. | ...      | created | 2024-...
 .. | ...      | paid    | 2024-... <-- Только одна запись (КОРРЕКТНО!)
```

---
