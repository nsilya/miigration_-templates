
## Инструмент для проверки ссылочной целостности в PostgreSQL

Этот инструмент позволяет проверить соответствие данных в связанных таблицах даже в отсутствие объявленных `FOREIGN KEY`. Он предназначен для использования при миграции, загрузке данных или аудите. Требует только Python и две библиотеки.

### Требования
- ОС: Ubuntu (или любая Linux-система с Python 3.7+)
- Пакеты: `psycopg2`, `pyyaml`

Установка:
```bash
pip install psycopg2 pyyaml
```

---

### Шаг 1. Конфигурация: `fk_rules.yaml`

Создайте файл `fk_rules.yaml` в рабочей директории. Каждое правило описывает связь между дочерней и родительской таблицами:

```yaml
rules:
  - child_table: order_details
    child_col: product_id
    parent_table: products
    parent_col: product_id

  - child_table: orders
    child_col: customer_id
    parent_table: customers
    parent_col: id

  - child_table: orders
    child_col: employee_id
    parent_table: employees
    parent_col: employee_id
```

> Добавление новой проверки требует только добавления нового блока в этот файл.

---

### Шаг 2. Скрипт: `check_fk_integrity.py`

Создайте файл `check_fk_integrity.py`:

```python
#!/usr/bin/env python3
"""
Проверка ссылочной целостности по правилам из YAML.
Зависимости: psycopg2, pyyaml
"""

import sys
import yaml
import psycopg2
from typing import List, Dict

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'northwind',
    'user': 'postgres',
    'password': 'postgres'
}

def load_rules(path: str) -> List[Dict]:
    with open(path, 'r', encoding='utf-8') as f:
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
            ARRAY(SELECT val FROM missing_refs ORDER BY val LIMIT 10) AS sample_ids
        FROM missing_refs;
    """

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    rules = load_rules('fk_rules.yaml')
    violations = []

    print("Проверка ссылочной целостности...\n")

    for rule in rules:
        desc = f"{rule['child_table']}.{rule['child_col']} → {rule['parent_table']}.{rule['parent_col']}"
        print(f"Проверяю: {desc}")

        cursor.execute(build_query(rule))
        broken, sample = cursor.fetchone()

        if broken > 0:
            violations.append((desc, broken, sample))
            print(f"  НАРУШЕНО: {broken} записей")
            print(f"    Примеры: {sample}\n")
        else:
            print("  OK\n")

    conn.close()

    if violations:
        print("\nНайдены нарушения ссылочной целостности:")
        for desc, cnt, _ in violations:
            print(f" - {desc}: {cnt} проблемных записей")
        sys.exit(1)
    else:
        print("Все проверки пройдены. Целостность соблюдена.")

if __name__ == '__main__':
    main()
```

---

### Запуск

1. Убедитесь, что PostgreSQL запущен (например, через Docker: `docker-compose up` в репозитории `northwind_psql`).
2. При необходимости обновите `DB_CONFIG` под вашу БД.
3. Выполните:
   ```bash
   python3 check_fk_integrity.py
   ```

---

### Особенности

- Поддерживает только одноколоночные связи.
- Игнорирует `NULL` в дочерней колонке.
- Возвращает до 10 примеров нарушенных значений.
- При наличии нарушений завершается с кодом `1` — подходит для cron или CI.

---

### Возможные доработки

- Поддержка составных ключей.
- Вывод в JSON/CSV.
- Интеграция с Airflow.
- Чтение параметров подключения из `.env` или `.pgpass`.
 
