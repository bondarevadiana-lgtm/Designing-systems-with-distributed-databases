from cassandra.cluster import Cluster
from datetime import datetime, timedelta
import random
import time


KEYSPACE = 'solar_ts_optimization'
CONTACT_POINTS = ['127.0.0.1']
NUM_STATIONS = 50
start_date = datetime(2025, 11, 1)


cluster = None
session = None

def get_session():
    """Підключається до існуючого Keyspace."""
    global cluster, session
    if session is None:
        try:
            cluster = Cluster(CONTACT_POINTS)
            session = cluster.connect(KEYSPACE)
            print(f"Підключення до '{KEYSPACE}' успішне.")
        except Exception as e:
            print(f"Помилка підключення до Cassandra. Переконайтесь, що дані завантажені: {e}")
            raise
    return session

def generate_bucket_hour(dt):
    """Генерує ключ партиції (Bucket) як текстовий рядок YYYY-MM-DD-HH."""
    return dt.strftime('%Y-%m-%d-%H')


def run_query_benchmark(session, prepared_query, params=None, iterations=100, is_filtering=False):
    """
    Вимірює latency для запиту, використовуючи Prepared Statement.
    Повертає словник з результатами для коректного виводу.
    """
    latencies = []
    

    actual_iterations = 10 if is_filtering else iterations 
        
    for _ in range(actual_iterations):
        start = time.time()
        
        if params is not None:
             session.execute(prepared_query, params)
        else:
             session.execute(prepared_query)
        
        end = time.time()
        latencies.append((end - start) * 1000) # у мс
        
    avg_latency = sum(latencies) / len(latencies)
    sorted_lat = sorted(latencies)
    p95_latency = sorted_lat[int(0.95 * (len(latencies) - 1))]
    p99_latency = sorted_lat[int(0.99 * (len(latencies) - 1))]
    

    return {
        'avg': f"{avg_latency:.2f}",
        'p95': f"{p95_latency:.2f}",
        'p99': f"{p99_latency:.2f}"
    }


def benchmark_all_queries(session):
    """Виконує тестування ключових запитів для всіх схем."""
    results = {}
    
    test_station = 'SOLAR_001'

    test_day_dt = start_date + timedelta(days=15)
    test_date_obj = test_day_dt.date()
    test_bucket_hour = generate_bucket_hour(test_day_dt.replace(hour=12))
    

    q2_start = test_day_dt.replace(hour=10, minute=0)
    q2_end = test_day_dt.replace(hour=16, minute=0)

    prep_q1_s = session.prepare("SELECT * FROM ts_schema_1_simple_wide WHERE station_id = ? LIMIT 100")
    prep_q1_h = session.prepare("SELECT * FROM ts_schema_2_hourly_bucket WHERE station_id = ? AND bucket_hour = ? LIMIT 100")
    prep_q1_d = session.prepare("SELECT * FROM ts_schema_3_raw_daily WHERE station_id = ? AND bucket_date = ? LIMIT 100")
    
    
    prep_q2_s = session.prepare("SELECT * FROM ts_schema_1_simple_wide WHERE station_id = ? AND timestamp >= ? AND timestamp <= ? LIMIT 1000 ALLOW FILTERING")
    prep_q2_h = session.prepare("SELECT * FROM ts_schema_2_hourly_bucket WHERE station_id = ? AND bucket_hour = ? AND timestamp >= ? AND timestamp <= ? LIMIT 1000")
    prep_q2_d = session.prepare("SELECT * FROM ts_schema_3_raw_daily WHERE station_id = ? AND bucket_date = ? AND timestamp >= ? AND timestamp <= ? LIMIT 1000")


    prep_q3 = session.prepare("SELECT station_id, bucket_hour, avg_power FROM ts_schema_3_hourly_aggregates WHERE station_id = ?")

    print("\n--- Етап 3: Тестування продуктивності запитів (Latency) ---")
    

    print("Тестування: Simple (Q1)...")
    results['Simple (Q1)'] = run_query_benchmark(session, prep_q1_s, (test_station,))
    
    print("Тестування: Hourly (Q1)...")
    results['Hourly (Q1)'] = run_query_benchmark(session, prep_q1_h, (test_station, test_bucket_hour))
    
    print("Тестування: Daily Raw (Q1)...")
    results['Daily Raw (Q1)'] = run_query_benchmark(session, prep_q1_d, (test_station, test_date_obj))
    
    print("Тестування: Simple (Q2)...")
    results['Simple (Q2)'] = run_query_benchmark(session, prep_q2_s, (test_station, q2_start, q2_end), is_filtering=True)
    
    print("Тестування: Hourly (Q2)...")
    results['Hourly (Q2)'] = run_query_benchmark(session, prep_q2_h, (test_station, test_bucket_hour, q2_start, q2_end))
    
    print("Тестування: Daily Raw (Q2)...")
    results['Daily Raw (Q2)'] = run_query_benchmark(session, prep_q2_d, (test_station, test_date_obj, q2_start, q2_end))

    print("Тестування: Daily Agg (Q3)...")
    results['Daily Agg (Q3)'] = run_query_benchmark(session, prep_q3, (test_station,))
            
    return results


def benchmark_materialized_views(session):
    """Тестування Materialized Views vs. ALLOW FILTERING"""
    
    test_station = 'SOLAR_001'
    test_day_dt = start_date + timedelta(days=15)
    test_bucket_hour = generate_bucket_hour(test_day_dt.replace(hour=12))

    prep_mv_high = session.prepare("SELECT * FROM mv_high_irradiance WHERE station_id = ? AND bucket_hour = ? AND irradiance > 900 LIMIT 100")
    prep_no_mv_high = session.prepare("SELECT * FROM ts_schema_2_hourly_bucket WHERE station_id = ? AND bucket_hour = ? AND irradiance > 900 LIMIT 100 ALLOW FILTERING")
    
    prep_mv_clear = session.prepare("SELECT * FROM mv_clear_weather WHERE station_id = ? AND bucket_hour = ? AND cloud_factor < 0.3 LIMIT 100")
    prep_no_mv_clear = session.prepare("SELECT * FROM ts_schema_2_hourly_bucket WHERE station_id = ? AND bucket_hour = ? AND cloud_factor < 0.3 LIMIT 100 ALLOW FILTERING")
    
    params = (test_station, test_bucket_hour)
    results = {}
    
    print("\nТест 1: High Irradiance Filter...")
    res_no_mv = run_query_benchmark(session, prep_no_mv_high, params, iterations=10, is_filtering=True)
    res_with_mv = run_query_benchmark(session, prep_mv_high, params, iterations=100)

    imp = float(res_no_mv['avg']) / float(res_with_mv['avg']) if float(res_with_mv['avg']) > 0 else 1.0
    
    results['High Irradiance'] = {
        'Without MV (ms)': res_no_mv['avg'],
        'With MV (ms)': res_with_mv['avg'],
        'Improvement': f"{imp:.1f}x"
    }

    print("Тест 2: Clear Weather Filter...")
    res_no_mv_c = run_query_benchmark(session, prep_no_mv_clear, params, iterations=10, is_filtering=True)
    res_with_mv_c = run_query_benchmark(session, prep_mv_clear, params, iterations=100)
    
    imp_c = float(res_no_mv_c['avg']) / float(res_with_mv_c['avg']) if float(res_with_mv_c['avg']) > 0 else 1.0
    
    results['Clear Weather'] = {
        'Without MV (ms)': res_no_mv_c['avg'],
        'With MV (ms)': res_with_mv_c['avg'],
        'Improvement': f"{imp_c:.1f}x"
    }
    
    return results


def main_benchmark():
    try:
        session = get_session()
        

        read_results = benchmark_all_queries(session)
        mv_results = benchmark_materialized_views(session)

        print("\n" + "="*55)
        print("          ФІНАЛЬНІ РЕЗУЛЬТАТИ ЛАБОРАТОРНОЇ РОБОТИ 3")
        print("="*55)
        
        print("\nТаблиця 1: Read Latency")
        print("| Схема | Query Type | Avg Latency (ms) | P95 Latency (ms) | P99 Latency (ms) |")
        print("| :--- | :--- | :---: | :---: | :---: |")
        
        keys_to_print = [
            'Simple (Q1)', 'Hourly (Q1)', 'Daily Raw (Q1)',
            'Simple (Q2)', 'Hourly (Q2)', 'Daily Raw (Q2)',
            'Daily Agg (Q3)'
        ]
        
        for k in keys_to_print:
            if k in read_results:
                 schema_name = k.split(' (')[0]
                 query_name = k.split(' (')[1].replace(')', '')
                 v = read_results[k]

                 print(f"| {schema_name} | {query_name} | {v['avg']} | {v['p95']} | {v['p99']} |")


        print("\nТаблиця 4: Вплив Materialized Views (MV)")
        print("| Query Type | Without MV (ms) | With MV (ms) | Improvement |")
        print("| :--- | :---: | :---: | :---: |")
        for k, v in mv_results.items():
            print(f"| {k} | {v['Without MV (ms)']} | {v['With MV (ms)']} | {v['Improvement']} |")
            
        print("\n--- Етап 5: Аналіз використання дискового простору ---")
        print("Для заповнення Таблиці 3 виконайте в терміналі команду:")
        print(f"nodetool tablestats {KEYSPACE}")
        
    except Exception as e:
        print(f"\nПрограма завершена з помилкою: {e}")

    finally:
        if cluster:
            cluster.shutdown()

if __name__ == "__main__":
    main_benchmark()