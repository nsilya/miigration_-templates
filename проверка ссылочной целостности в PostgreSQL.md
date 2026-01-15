Инструмент для проверки ссылочной целостности** в PostgreSQL, который:

- ✅ Не требует сложных зависимостей (только Python + `psycopg2`),
- ✅ Генерирует отчёт в консоли и/или файле,
- ✅ Легко расширяем (добавляешь пару строк в YAML — и новая проверка готова).

---

## Шаг 1. Файл конфигурации: `fk_rules.yaml`

Создай файл `fk_rules.yaml` рядом с будущим скриптом:

```yaml
Создай файл `fk_rules.yaml`:

```yaml
# fk_rules.yaml — правила для Northwind (PostgreSQL)
rules:
  # Заказы → клиенты
  - child_table: orders
    child_col: customer_id
    parent_table: customers
    parent_col: customer_id

  # Заказы → сотрудники
  - child_table: orders
    child_col: employee_id
    parent_table: employees
    parent_col: employee_id

  # Заказы → перевозчики
  - child_table: orders
    child_col: ship_via
    parent_table: shippers
    parent_col: shipper_id

  # Позиции заказа → заказы
  - child_table: order_details
    child_col: order_id
    parent_table: orders
    parent_col: order_id

  # Позиции заказа → товары
  - child_table: order_details
    child_col: product_id
    parent_table: products
    parent_col: product_id

  # Товары → категории
  - child_table: products
    child_col: category_id
    parent_table: categories
    parent_col: category_id

  # Товары → поставщики
  - child_table: products
    child_col: supplier_id
    parent_table: suppliers
    parent_col: supplier_id

  # Сотрудники → менеджеры (самоссылка, NULL допустим)
  - child_table: employees
    child_col: reports_to
    parent_table: employees
    parent_col: employee_id
```
```

>  Это всё, что нужно менять при добавлении новой проверки.

---

##  Шаг 2. Скрипт: `check_fk_integrity.py`

Создай файл `check_fk_integrity.py`:

```python
#!/usr/bin/env python3
"""
Проверка ссылочной целостности по правилам из YAML.
Требования: pip install psycopg2 pyyaml
"""

import yaml
import psycopg2
import sys
from typing import List, Dict

# --- Настройки подключения (измени под себя) ---
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'your_db',      # ← замени
    'user': 'your_user',        # ← замени
    'password': 'your_pass'     # ← или используй .pgpass / env
}

def load_rules(yaml_path: str) -> List[Dict]:
    with open(yaml_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)['rules']

def build_query(rule: Dict) -> str:
    ct = rule['child_table']
    cc = rule['child_col']
    pt = rule['parent_table']
    pc = rule['parent_col']
    
    return f"""
    WITH missing_refs AS (
        SELECT DISTINCT od.{cc} AS val
        FROM {ct} od
        LEFT JOIN {pt} p ON od.{cc} = p.{pc}
        WHERE od.{cc} IS NOT NULL AND p.{pc} IS NULL
    )
    SELECT 
        COUNT(*) AS broken,
        ARRAY(
            SELECT val FROM missing_refs ORDER BY val LIMIT 10
        ) AS sample_ids
    FROM missing_refs;
    """

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    rules = load_rules('fk_rules.yaml')
    violations = []

    print("🔍 Проверка ссылочной целостности...\n")

    for rule in rules:
        desc = f"{rule['child_table']}.{rule['child_col']} → {rule['parent_table']}.{rule['parent_col']}"
        print(f"Проверяю: {desc}")
        
        cursor.execute(build_query(rule))
        broken, sample = cursor.fetchone()
        
        if broken > 0:
            violations.append((desc, broken, sample))
            print(f"   НАРУШЕНО: {broken} записей")
            print(f"     Примеры: {sample}\n")
        else:
            print("   OK\n")

    conn.close()

    if violations:
        print("\n Найдены нарушения ссылочной целостности:")
        for desc, cnt, _ in violations:
            print(f" - {desc}: {cnt} проблемных записей")
        sys.exit(1)  # полезно для CI/CD или cron
    else:
        print("✅ Все проверки пройдены. Целостность соблюдена.")

if __name__ == '__main__':
    main()
```

---

##  Как запустить

1. Установи зависимости (один раз):
   ```bash
   pip install psycopg2 pyyaml
   ```

2. Настрой подключение в `DB_CONFIG` (или лучше — вынеси в `.env` или `.pgpass`, но для старта сойдёт и так).

3. Запусти:
   ```bash
   python3 check_fk_integrity.py
   ```

---

##  Преимущества такого подхода

| Плюс | Почему это важно именно тебе |
|------|------------------------------|
| **Минимум ручной работы** | Ты не пишешь SQL каждый раз — только описываешь связь |
| **Повторяемость** | Можно запускать после каждой миграции |
| **Отчёт для команды** | Видно, что проверено и где проблемы |
| **Часть портфолио** | GitHub-репозиторий с `fk_checker/` — сразу видно инженерное мышление |
| **Безопасность** | Учёт `NULL`, нет ложных срабатываний |

---

##  Что дальше?

- Хочешь — добавлю поддержку **составных ключей** (например, `(tenant_id, product_id)`).
- Могу сделать **HTML-отчёт** или **JSON-вывод** для интеграции в CI.
- Или оберну это в **Airflow DAG**, если ты уже используешь оркестрацию.

Но даже в текущем виде — это **боевой инструмент**, который решает реальную проблему в миграциях.

Готов адаптировать под твою схему (Northwind? Сбер? Свой проект?). Просто пришли список таблиц и колонок — и я сгенерирую `fk_rules.yaml` за тебя.


адаптируем автоматизацию под **Northwind**, классическую учебную базу данных(в том числе с PostgreSQL и LeetCode).

---

##  Схема Northwind: основные внешние связи

В типичной реализации Northwind (например, [этой](https://github.com/pthom/northwind_psql)) есть следующие таблицы и FK-связи:

| Дочерняя таблица | Колонка | Родительская таблица | Колонка |
|------------------|--------|----------------------|--------|
| `orders`         | `customer_id` | `customers`       | `customer_id` |
| `orders`         | `employee_id` | `employees`       | `employee_id` |
| `orders`         | `ship_via`    | `shippers`        | `shipper_id` |
| `order_details`  | `order_id`    | `orders`          | `order_id` |
| `order_details`  | `product_id`  | `products`        | `product_id` |
| `products`       | `category_id` | `categories`      | `category_id` |
| `products`       | `supplier_id` | `suppliers`       | `supplier_id` |
| `employees`      | `reports_to`  | `employees`       | `employee_id` *(самоссылка)* |

>  Обрати внимание:  
> - В `orders` колонка называется `customer_id`, а в `customers` — тоже `customer_id` (не `id`).  
> - В `shippers` первичный ключ — `shipper_id`, но в `orders` ссылка через `ship_via`.  
> - `reports_to` — nullable self-reference (допустимо, что `NULL` = CEO).

---


> 🔒 Все правила корректны даже если FK не объявлены в БД — мы проверяем **фактическую целостность**.

---

## ▶️ Как использовать

1. Убедись, что у тебя запущен PostgreSQL с Northwind (например, из докера или дампа).
2. Обнови `DB_CONFIG` в скрипте `check_fk_integrity.py`:
   ```python
   DB_CONFIG = {
       'host': 'localhost',
       'port': 5432,
       'database': 'northwind',   # ← имя твоей БД
       'user': 'postgres',        # ← или твой юзер
       'password': 'your_pass'
   }
   ```
3. Запусти:
   ```bash
   python3 check_fk_integrity.py
   ```

---

## 🧪 Ожидаемый результат (на чистом Northwind)

Если данные не повреждены — вывод будет:

```
✅ Все проверки пройдены. Целостность соблюдена.
```

Если же ты **мигрировал данные** и, например, забыл загрузить `shippers`, то получишь:

```
Проверяю: orders.ship_via → shippers.shipper_id
  ❌ НАРУШЕНО: 830 записей
     Примеры: [1, 2, 3]
```

---

---

