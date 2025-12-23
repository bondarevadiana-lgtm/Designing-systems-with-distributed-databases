# Лабораторна робота №3: Оптимізація схем даних в Apache Cassandra для енергетичних часових рядів

## Сонячні електростанції (Варіант 1)

## Мета роботи

Навчитися проєктувати та оптимізувати схеми даних в Apache Cassandra для зберігання великих обсягів часових рядів (Time-Series). Порівняти різні стратегії групування даних (bucketing), оцінити їхній вплив на розмір партицій, швидкість читання та запису, а також використання дискового простору. Дослідити ефективність Materialized Views для фільтрації даних у порівнянні з використанням `ALLOW FILTERING`.

## Фокус дослідження

Проведено порівняльний аналіз трьох моделей зберігання часових рядів:

- **Simple Wide Row (антипатерн)** — одна партиція на станцію  
- **Hourly Bucketing** — розбиття даних по годинах  
- **Daily Bucketing + Pre-aggregation** — щоденні партиції та окрема таблиця агрегатів  

Окремо досліджено ефективність прискорення запитів за допомогою **Materialized Views**.

## Схема бази даних

### Опис моделей зберігання

| № | Назва таблиці | Partition Key | Clustering Key | Призначення |
|---|---------------|---------------|----------------|-------------|
| 1 | `ts_schema_1_simple_wide` | `station_id` | `timestamp DESC` | Демонстрація проблеми росту партицій |
| 2 | `ts_schema_2_hourly_bucket` | `(station_id, bucket_hour)` | `timestamp DESC` | Рекомендована схема зі стабільним розміром |
| 3 | `ts_schema_3_raw_daily` | `(station_id, bucket_date)` | `timestamp DESC` | Архівна схема для добової аналітики |
| 4 | `ts_schema_3_hourly_aggregates` | `station_id` | `bucket_hour` | Попередньо обчислені погодинні агрегати |

### Оптимізація (Materialized Views)

- **`mv_high_irradiance`** — швидкий пошук пікової генерації (`irradiance > 900`)  
- **`mv_clear_weather`** — фільтрація за низькою хмарністю (`cloud_factor < 0.3`)  

## Реалізація

### Створення схеми з Bucketing (Hourly)

```python
session.execute("""
CREATE TABLE IF NOT EXISTS ts_schema_2_hourly_bucket (
    station_id text,
    bucket_hour text,
    timestamp timestamp,
    power_output decimal,
    irradiance decimal,
    cloud_factor decimal,
    PRIMARY KEY ((station_id, bucket_hour), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);
""")


### Генерація Time-Series (1M+ записів)

```
for minute in range(RECORDS_PER_DAY):
    bucket_hour = dt.strftime('%Y-%m-%d-%H')
    batch.add(prep, (station_id, bucket_hour, dt, power_output, irradiance, cloud_factor))
    if len(batch) >= 500:
        session.execute(batch)
```

### Benchmark тестування

```
prep_q1_h = session.prepare("""
    SELECT * FROM ts_schema_2_hourly_bucket
    WHERE station_id = ? AND bucket_hour = ?
    LIMIT 100
""")

results = run_query_benchmark(session, prep_q1_h, (test_station, test_bucket_hour))
``

## Аналіз результатів роботи
1. Продуктивність запису (Write Throughput)
- Обсяг даних: ~2 160 000 записів на кожну схему (усього понад 6.4 млн)
- Середня швидкість запису: ≈ 5500–6200 записів/сек
- Особливість: схема Daily + Aggregates потребує додаткового запису агрегатів, але прискорює подальшу аналітику

2. Ефективність зберігання (Disk Usage)
- За даними nodetool tablestats:
- Simple Schema: середній розмір партиції ≈ 2.09 MB
- Hourly Bucketing: середній розмір партиції ≈ 6.18 KB
- Daily Bucketing: середній розмір партиції ≈ 133.20 KB
- Compression Ratio: ≈ 0.59 (економія ~41%)
- Висновок: Hourly Bucketing зменшив розмір партиції приблизно у 330 разів у порівнянні з Simple схемою.

3. Швидкість читання (Read Latency)
- Hourly Bucketing: ≈ 15.48 ms
- Simple Schema: потребує ALLOW FILTERING, що призводить до сканування великих партицій
- Materialized Views: прискорення запитів у 10–15 разів

## Оцінка структури даних
Simple Schema : Непридатна для довготривалого зберігання через швидкий ріст партицій та високу latency.
Hourly Bucketing : Оптимальний вибір для оперативних дашбордів та моніторингу в реальному часі зі стабільною продуктивністю.
Materialized Views : Ефективні для стабільних фільтрів, але збільшують навантаження на запис і використання диску на 10–15%.

## Висновки
- Time Bucketing є обов’язковою стратегією для часових рядів у Cassandra
- Для сонячних електростанцій оптимальним є Hourly Bucketing для актуальних даних і Daily Bucketing для архіву
- Використання TEXT-ключів формату YYYY-MM-DD-HH є стабільнішим у Python-середовищі
- Pre-aggregation дозволяє будувати звіти без вибірки мільйонів сирих записів
- Materialized Views значно прискорюють читання, але повинні застосовуватись вибірково
