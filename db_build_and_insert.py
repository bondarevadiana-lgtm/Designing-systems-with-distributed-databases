from cassandra.cluster import Cluster
from cassandra.cluster import ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra import ConsistencyLevel
from cassandra.query import BatchStatement, BatchType
from datetime import datetime, timedelta
import random
import time

# --- константи варіанту ---
keyspace = 'solar_grid_monitoring'
contact_points = ['127.0.0.1']
num_stations = 50
region_id = 'Kyiv'
# фіксуємо дату для відтворюваності результатів
target_date = datetime(2025, 11, 24).date()
readings_per_hour = 60
# зменшуємо ліміт пакету для гарантованого уникнення "Batch too large"
batch_size_limit = 100 

# --- глобальні змінні ---
cluster = None
session = None

def get_session():
    global cluster, session
    if session is None:
        try:
            profile = ExecutionProfile(consistency_level=ConsistencyLevel.QUORUM)
            cluster = Cluster(contact_points, execution_profiles={EXEC_PROFILE_DEFAULT: profile})
            session = cluster.connect()
            print(f"підключення до cassandra успішне: {contact_points}")
            
            session.execute(f"create keyspace if not exists {keyspace} with replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};")
            session.set_keyspace(keyspace)
            print(f"keyspace '{keyspace}' створено та активовано.")
            
        except Exception as e:
            print(f"помилка підключення до cassandra: {e}")
            raise
    return session

def create_tables(session):
    
    session.execute("""
    create table if not exists realtime_readings_by_station (
        station_id text,
        timestamp timestamp,
        power_output decimal,
        voltage decimal,
        current decimal,
        panel_temp decimal,
        primary key (station_id, timestamp)
    ) with clustering order by (timestamp desc);
    """)

    session.execute("""
    create table if not exists weather_by_date (
        region_id text,
        date date,
        timestamp timestamp,
        irradiance decimal,
        cloud_factor decimal,
        air_temp decimal,
        primary key ((region_id, date), timestamp)
    ) with clustering order by (timestamp desc);
    """)
    
    session.execute("""
    create table if not exists daily_generation_summary (
        station_id text,
        date date,
        total_mwh decimal,
        max_power_mw decimal,
        uptime_hours int,
        primary key (station_id, date)
    );
    """)
    
    session.execute("""
    create table if not exists archive_readings_by_region (
        region_id text,
        date date,
        station_id text,
        power_output decimal,
        timestamp timestamp,
        primary key ((region_id, date), station_id)
    );
    """)
    
    print("усі 4 таблиці успішно створено або існують.")

def generate_and_insert_data(session):
    
    stations = [f'solar_{i:03d}' for i in range(1, num_stations + 1)]
    start_of_day = datetime.combine(target_date, datetime.min.time())
    
    total_records = 0
    batches_executed = 0
    
    prep_readings = session.prepare(
        "insert into realtime_readings_by_station (station_id, timestamp, power_output, voltage, current, panel_temp) values (?, ?, ?, ?, ?, ?)"
    )
    prep_archive = session.prepare(
        "insert into archive_readings_by_region (region_id, date, station_id, power_output, timestamp) values (?, ?, ?, ?, ?)"
    )
    
    batch = BatchStatement(batch_type=BatchType.LOGGED, consistency_level=ConsistencyLevel.QUORUM)
    
    start_insertion_time = time.time()

    for hour in range(24):
        cloud_factor = 0.0
        if 6 < hour < 18:
            cloud_factor = round(random.uniform(0.0, 1.0), 2)
            
        current_hour_time = start_of_day + timedelta(hours=hour)
        
        for minute in range(readings_per_hour):
            
            timestamp = current_hour_time + timedelta(minutes=minute)
            
            if minute == 0:
                irradiance = random.uniform(100.0, 1200.0) if 6 < hour < 18 else 0.0
                
                session.execute(
                    "insert into weather_by_date (region_id, date, timestamp, irradiance, cloud_factor, air_temp) values (%s, %s, %s, %s, %s, %s)",
                    (region_id, target_date, timestamp, irradiance, cloud_factor, random.uniform(5.0, 35.0))
                )
            
            for station_id in stations:
                base_power = random.uniform(0.5, 25.0) if 7 <= hour < 19 else 0.0
                power_output = round(base_power * (1 - cloud_factor * 0.5), 2) if base_power > 0 else 0.0
                
                voltage = round(random.uniform(350.0, 450.0), 2)
                current = round(power_output / 0.4, 2)
                panel_temp = round(random.uniform(25.0, 65.0), 2)
                
                batch.add(prep_readings, (station_id, timestamp, power_output, voltage, current, panel_temp))
                batch.add(prep_archive, (region_id, target_date, station_id, power_output, timestamp))
                total_records += 2
                
                if len(batch) >= batch_size_limit:
                    session.execute(batch)
                    batches_executed += 1
                    batch = BatchStatement(batch_type=BatchType.LOGGED, consistency_level=ConsistencyLevel.QUORUM)
                    
    if len(batch) > 0:
        session.execute(batch)
        batches_executed += 1
        
    end_insertion_time = time.time()
    
    for station_id in stations:
        session.execute(
            "insert into daily_generation_summary (station_id, date, total_mwh, max_power_mw, uptime_hours) values (%s, %s, %s, %s, %s)",
            (station_id, target_date, 
             round(random.uniform(5.0, 15.0), 2),
             round(random.uniform(15.0, 25.0), 2),
             random.randint(8, 12))
        )

    test_station = 'solar_001'
    count_query = f"select count(*) from realtime_readings_by_station where station_id = '{test_station}'"
    count_rows = session.execute(count_query)
    records_in_test_partition = count_rows.one()[0]

    print(f"генерація та пакетний запис завершено.")
    print(f"записано всього: {total_records} записів (у 2 таблиці) + {num_stations} summary.")
    print(f"виконано пакетів: {batches_executed}")
    print(f"перевірка: знайдено записів для {test_station}: {records_in_test_partition}")
    print(f"час вставки: {end_insertion_time - start_insertion_time:.2f} сек")
    return total_records

def analyze_data(session):
    
    print("\n--- аналіз даних (підваріант а) ---")
    print("ключовий параметр: power_output (потужність)")
    
    target_station = 'solar_001'
    
    # --- виконання аналізу та агрегації ---
    
    # 1. читаємо всі показання для агрегації (як до цього)
    query_all = f"select power_output, timestamp from realtime_readings_by_station where station_id = '{target_station}'"
    
    start_query_time = time.time()
    rows = session.execute(query_all)
    end_query_time = time.time()
    
    # 2. знаходимо максимальне значення та відповідний час
    power_values_with_time = []
    max_power = 0.00
    
    for row in rows:
        power = float(row.power_output)
        if power > max_power:
            max_power = power
        power_values_with_time.append((row.timestamp, power))

    count = len(power_values_with_time)
    if count == 0:
        avg_power = 0.00
    else:
        avg_power = sum(p for t, p in power_values_with_time) / count
        
    # 3. фільтруємо записи, щоб показати записи з максимальною потужністю
    # шукаємо 3 записи, коли потужність була максимальною
    peak_records = sorted([
        (t, p) for t, p in power_values_with_time if p >= max_power - 0.5  # беремо записи близькі до піку
    ], key=lambda x: x[0])[-3:]

    print(f"аналіз завершено за {end_query_time - start_query_time:.4f} сек.")
    
    print("\n--- результати аналізу (підваріант а) ---")
    print(f"об'єкт аналізу: станція {target_station} за {target_date}")
    print(f"кількість записів: {count}")
    print(f"середня потужність = {avg_power:.2f} мвт")
    print(f"максимальна потужність = {max_power:.2f} мвт")
    
    print("\nкороткий висновок:")
    if max_power > 20:
        print("станція solar_001 показує високі пікові значення, що свідчить про ефективну роботу в години максимальної сонячної радіації.")
    else:
        print("помірні показники. необхідно проаналізувати метеоумови (cloud_factor) для виявлення причин відсутності пікових значень.")
    
    print("\n--- тест ключового запиту: 3 записи пікової потужності ---")
    
    if peak_records:
        for timestamp, power in peak_records:
            print(f"  [{timestamp.strftime('%H:%M')}] потужність: {power:.2f} мвт")
    else:
        # це спрацює, якщо max_power = 0 (наприклад, станція не працювала)
        rows_latest = session.execute(f"select power_output, timestamp from realtime_readings_by_station where station_id = '{target_station}' limit 3;")
        print("показання з кінця доби (пік не зафіксовано):")
        for row in rows_latest:
            print(f"  [{row.timestamp.strftime('%H:%M')}] потужність: {row.power_output} мвт")
        
    print("\n--- підсумок ---")
    print(f"keyspace: {keyspace}")
    print(f"кількість створених/оновлених таблиць: 4")
    print("програма завершена. результати аналізу виведено вище.")

def main():
    try:
        session = get_session()
        create_tables(session)
        generate_and_insert_data(session)
        analyze_data(session)
    except Exception as e:
        print(f"програма аварійно завершена через помилку cassandra або підключення: {e}")
    finally:
        if cluster:
            cluster.shutdown()

if __name__ == "__main__":
    main()