from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, BatchType
from datetime import datetime, timedelta
import random
import time


KEYSPACE = 'solar_ts_optimization'
CONTACT_POINTS = ['127.0.0.1']
NUM_STATIONS = 50
DAYS_OF_DATA = 30 
RECORDS_PER_MINUTE = 1 
RECORDS_PER_DAY = 24 * 60 * RECORDS_PER_MINUTE 
BATCH_SIZE_LIMIT = 500


cluster = None
session = None
start_date = datetime(2025, 11, 1)

def get_session():
    """Створює сесію та Keyspace."""
    global cluster, session
    if session is None:
        try:
            cluster = Cluster(CONTACT_POINTS)
            session = cluster.connect()
            session.execute(f"CREATE KEYSPACE IF NOT EXISTS {KEYSPACE} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 3}};")
            session.set_keyspace(KEYSPACE)
            print(f"Підключення та keyspace '{KEYSPACE}' активовано.")
        except Exception as e:
            print(f"Помилка підключення до Cassandra: {e}")
            raise
    return session

def generate_bucket_hour(dt):

    return dt.strftime('%Y-%m-%d-%H')

def create_all_schemas(session):
    """Створює всі 3 схеми та Materialized Views."""
    print("\n--- Етап 1: Створення схем ---")


    session.execute("""
    CREATE TABLE IF NOT EXISTS ts_schema_1_simple_wide (
        station_id text,
        timestamp timestamp,
        power_output decimal,
        irradiance decimal,
        cloud_factor decimal,
        PRIMARY KEY (station_id, timestamp)
    ) WITH CLUSTERING ORDER BY (timestamp DESC);
    """)

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
    

    session.execute("""
    CREATE TABLE IF NOT EXISTS ts_schema_3_raw_daily (
        station_id text,
        bucket_date date,
        timestamp timestamp,
        power_output decimal,
        irradiance decimal,
        cloud_factor decimal,
        PRIMARY KEY ((station_id, bucket_date), timestamp)
    ) WITH CLUSTERING ORDER BY (timestamp DESC);
    """)

    session.execute("""
    CREATE TABLE IF NOT EXISTS ts_schema_3_hourly_aggregates (
        station_id text,
        bucket_hour text,
        avg_power decimal,
        max_power decimal,
        avg_irradiance decimal,
        PRIMARY KEY (station_id, bucket_hour)
    );
    """)

    print("Створення Materialized Views...")
    
    session.execute("""
    CREATE MATERIALIZED VIEW IF NOT EXISTS mv_high_irradiance AS
        SELECT station_id, bucket_hour, timestamp, irradiance, power_output
        FROM ts_schema_2_hourly_bucket
        WHERE station_id IS NOT NULL AND bucket_hour IS NOT NULL AND irradiance IS NOT NULL AND timestamp IS NOT NULL
        PRIMARY KEY ((station_id, bucket_hour), irradiance, timestamp)
        WITH CLUSTERING ORDER BY (irradiance DESC, timestamp DESC);
    """)

    session.execute("""
    CREATE MATERIALIZED VIEW IF NOT EXISTS mv_clear_weather AS
        SELECT station_id, bucket_hour, timestamp, cloud_factor, power_output
        FROM ts_schema_2_hourly_bucket
        WHERE station_id IS NOT NULL AND bucket_hour IS NOT NULL AND cloud_factor IS NOT NULL AND timestamp IS NOT NULL
        PRIMARY KEY ((station_id, bucket_hour), cloud_factor, timestamp)
        WITH CLUSTERING ORDER BY (cloud_factor ASC, timestamp DESC);
    """)
    print("Схеми та Materialized Views успішно створено.")


def insert_data(session, schema_number):
    """Генерує та вставляє дані в обрану схему."""
    stations = [f'SOLAR_{i:03d}' for i in range(1, NUM_STATIONS + 1)]
    total_records = 0
    batches_executed = 0
    
    if schema_number == 1:
        table_name = 'ts_schema_1_simple_wide'
        prep = session.prepare(f"INSERT INTO {table_name} (station_id, timestamp, power_output, irradiance, cloud_factor) VALUES (?, ?, ?, ?, ?)")
    elif schema_number == 2:
        table_name = 'ts_schema_2_hourly_bucket'
        prep = session.prepare(f"INSERT INTO {table_name} (station_id, bucket_hour, timestamp, power_output, irradiance, cloud_factor) VALUES (?, ?, ?, ?, ?, ?)")
    elif schema_number == 3:
        table_name_raw = 'ts_schema_3_raw_daily'
        table_name_agg = 'ts_schema_3_hourly_aggregates'
        prep_raw = session.prepare(f"INSERT INTO {table_name_raw} (station_id, bucket_date, timestamp, power_output, irradiance, cloud_factor) VALUES (?, ?, ?, ?, ?, ?)")
        prep_agg = session.prepare(f"INSERT INTO {table_name_agg} (station_id, bucket_hour, avg_power, max_power, avg_irradiance) VALUES (?, ?, ?, ?, ?)")
    else:
        print(f"Невідома схема: {schema_number}")
        return 0, 0.0

    print(f"\n--- Вставка даних у СХЕМУ {schema_number} ---")
    start_time = time.time()
    
    for day in range(DAYS_OF_DATA):
        current_date = start_date + timedelta(days=day)
        day_start_dt = datetime.combine(current_date, datetime.min.time())
        
        hourly_data = {s: {'power': [], 'irradiance': []} for s in stations}
        

        for minute in range(RECORDS_PER_DAY):
            dt = day_start_dt + timedelta(minutes=minute)
            hour = dt.hour
            
            if minute % 60 == 0:
                cloud_factor = round(random.uniform(0.0, 1.0), 2)

                bucket_hour = generate_bucket_hour(dt.replace(minute=0, second=0, microsecond=0))
            
            batch = BatchStatement(batch_type=BatchType.LOGGED)

            for station_id in stations:

                irradiance = random.uniform(200.0, 1000.0) if 7 <= hour < 19 else random.uniform(0, 50)
                power_base = random.uniform(0.5, 5.0) * (1 - cloud_factor * 0.5) if 7 <= hour < 19 else 0.0
                power_output = round(power_base * (irradiance / 1000), 4)

                if schema_number == 1:
                    batch.add(prep, (station_id, dt, power_output, irradiance, cloud_factor))

                elif schema_number == 2:
                    batch.add(prep, (station_id, bucket_hour, dt, power_output, irradiance, cloud_factor))
                    

                elif schema_number == 3:

                    batch.add(prep_raw, (station_id, current_date.date(), dt, power_output, irradiance, cloud_factor))
                    
                    hourly_data[station_id]['power'].append(power_output)
                    hourly_data[station_id]['irradiance'].append(irradiance)

                total_records += 1

                if len(batch) >= BATCH_SIZE_LIMIT:
                    session.execute(batch)
                    batches_executed += 1
                    batch = BatchStatement(batch_type=BatchType.LOGGED)


            if len(batch) > 0 and (schema_number != 3 or minute % 60 != 59): 
                session.execute(batch)
                batches_executed += 1
                batch = BatchStatement(batch_type=BatchType.LOGGED)



            if schema_number == 3 and minute % 60 == 59:
                for station_id in stations:
                    powers = hourly_data[station_id]['power']
                    irradiances = hourly_data[station_id]['irradiance']
                    
                    if powers:
                        avg_p = sum(powers) / len(powers)
                        max_p = max(powers)
                        avg_i = sum(irradiances) / len(irradiances)

                        session.execute(prep_agg, (station_id, bucket_hour, avg_p, max_p, avg_i))
                
                hourly_data = {s: {'power': [], 'irradiance': []} for s in stations} 


    if len(batch) > 0:
        session.execute(batch)
        batches_executed += 1


    end_time = time.time()
    write_time = end_time - start_time
    write_throughput = total_records / write_time
    
    print(f"Запис завершено. Записано: {total_records} записів.")
    print(f"Час: {write_time:.2f} сек.")
    print(f"Write Throughput: {write_throughput:.2f} записів/сек.")
    
    return write_time, write_throughput


def main_create_insert():
    try:
        session = get_session()
        create_all_schemas(session)
        
        print("\n--- Етап 2: Генерація даних та Write Benchmarking ---")
        insert_data(session, 1)
        insert_data(session, 2)
        insert_data(session, 3)
        
    except Exception as e:
        print(f"Програма завершена з помилкою: {e}")
    finally:
        if cluster:
            cluster.shutdown()

if __name__ == "__main__":
    main_create_insert()