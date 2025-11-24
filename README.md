# Лабораторна робота №2: Проєктування розподілених часових схем даних в Apache Cassandra

## Енергетичні системи моніторингу

### Мета роботи

Освоїти принципи проєктування розподілених, денормалізованих схем даних у Apache Cassandra. Навчитися створювати keyspace, таблиці з різними типами primary key. Виконати масовий запис IoT-даних, аналітичні запити та оцінити продуктивність. Проаналізувати ефективність створеної структури для реальних енергетичних систем моніторингу.

### Фокус дослідження

Обчислення середнього та максимального значення параметра `power_output`. Вивід короткого звіту у вигляді: "Середня потужність = … кВт, Максимальна = … кВт". Формування висновку щодо того, який об'єкт показує найвищі значення.

## Схема бази даних

### Опис таблиць

| № | Тип даних | Назва таблиці | Primary Key | Призначення |
|---|-----------|---------------|-------------|-------------|
| 1 | Оперативні дані | `realtime_readings_by_station` | `(station_id, timestamp DESC)` | Останні показання окремої станції |
| 2 | Метеоумови | `weather_by_date` | `((region_id, date), timestamp DESC)` | Погодні умови по регіону за день |
| 3 | Щоденні підсумки | `daily_generation_summary` | `(station_id, date)` | Добова генерація та макс. потужність |
| 4 | Архів / Аналітика | `archive_readings_by_region` | `((region_id, date), station_id)` | Порівняння всіх станцій у регіоні |

## Реалізація

### Підключення та створення Keyspace
```python
cluster = Cluster(contact_points, execution_profiles={EXEC_PROFILE_DEFAULT: profile})
session = cluster.connect()

session.execute(f"""
    CREATE KEYSPACE IF NOT EXISTS {keyspace}
    WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};
""")
session.set_keyspace(keyspace)
```

### Створення таблиць
```python
session.execute("""
CREATE TABLE IF NOT EXISTS realtime_readings_by_station (
    station_id text,
    timestamp timestamp,
    power_output decimal,
    voltage decimal,
    current decimal,
    panel_temp decimal,
    PRIMARY KEY (station_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);
""")
```

### Генерація даних та Batch-вставка
```python
batch = BatchStatement(batch_type=BatchType.LOGGED)

batch.add(prep_readings, (station_id, timestamp, power_output, voltage, current, panel_temp))
batch.add(prep_archive, (region_id, target_date, station_id, power_output, timestamp))

if len(batch) >= batch_size_limit:
    session.execute(batch)
```

### Аналітика
```python
query_all = f"""
    SELECT power_output, timestamp 
    FROM realtime_readings_by_station 
    WHERE station_id = '{target_station}'
"""

rows = session.execute(query_all)
```

## Аналіз результатів роботи

### 1. Продуктивність запису

- **Усього записано:** 144 000 записів в оперативні таблиці
- **Плюс:** 50 записів добових підсумків
- **Час вставки:** 22.91 сек
- **Пропускна здатність:** ≈ 6283 записів/сек

**Висновок:** Cassandra показала відмінну швидкодію для write-heavy IoT-навантаження. Batch по 100 записів допоміг уникнути помилки "Batch too large".

### 2. Продуктивність читання

- **Читання всієї партиції станції** (1440 записів): 0.0623 сек
- Завдяки `Partition Key = station_id` усі записи знаходяться на одному вузлі

### 3. Аналітика потужності

Для станції `solar_001`:

- **Середня потужність:** ≈ 12–15 МВт
- **Максимальна потужність:** ≈ 24.47 МВт

**3 пікові записи:**
- [10:51] 24.47 МВт
- [10:52] 24.35 МВт
- [10:53] 24.10 МВт

**Висновок:** Станція `solar_001` досягає піку генерації у денні години (10:50–11:00), що узгоджується з моделлю сонячної радіації.

## Оцінка структури даних

### 1. Оперативні дані

`PRIMARY KEY (station_id, timestamp DESC)` — оптимально для швидкого отримання останнього показання.

### 2. Архів / Аналітика

`PRIMARY KEY ((region_id, date), station_id)` — дозволяє аналізувати всі станції за день в межах однієї партиції.

### 3. Запобігання гарячим партиціям

Використання `(station_id, date)` або `(region_id, date)` рівномірно розподіляє дані по вузлах.

## Висновки

1. Cassandra є оптимальною СУБД для високочастотних IoT-даних, зокрема енергетичних систем
2. Правильний вибір Partition Key визначає продуктивність як запису, так і читання
3. Денормалізація в Cassandra — вимушена та ефективна стратегія, що замінює JOIN
4. Обмежені batch-и є ключем до стабільності кластера
5. Розроблена схема БД є масштабованою та відповідає вимогам реальних енергетичних моніторингових систем

## Контрольні питання

### 1. Різниця між Partition Key та Clustering Key

- **Partition Key** — розподіляє дані по вузлах
- **Clustering Key** — сортує дані всередині партиції

### 2. Чому Cassandra використовує денормалізацію?

Щоб уникнути JOIN та прискорити доступ до даних.

### 3. Як обирати ключ для часових рядів?

`PRIMARY KEY ((object_id, date), timestamp)`

### 4. Реплікаційний фактор

Кількість копій рядка в кластері. Впливає на доступність.

### 5. Навіщо ORDER BY timestamp DESC?

Для швидкого доступу до останнього показання — ключовий запит у IoT.

### 6. Ознаки занадто великої партиції

- розмір більше 100 МБ
- висока latency
- нерівномірне навантаження вузлів

### 7. Переваги Cassandra над SQL

- масштабованість
- швидкість запису
- висока доступність
