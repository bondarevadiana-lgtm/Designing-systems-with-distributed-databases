import time
import random
import json
from datetime import datetime
from prometheus_client import start_http_server, Gauge, Counter


MESSAGES_SENT = Counter('solar_messages_sent_total', 'Загальна кількість відправлених повідомлень', ['station_id'])
POWER_MW = Gauge('solar_power_mw', 'Поточна потужність станції в МВт', ['station_id'])
PANEL_TEMP = Gauge('solar_panel_temp_celsius', 'Температура сонячних панелей', ['station_id'])
RADIATION = Gauge('solar_radiation_wm2', 'Інтенсивність сонячної радіації', ['station_id'])
EFFICIENCY = Gauge('solar_efficiency_percent', 'Коефіцієнт корисної дії панелей', ['station_id'])
STATION_STATUS = Gauge('solar_station_status', 'Статус станції (1-активна, 0-сервіс)', ['station_id'])

def get_solar_factor():
    """Симуляція сонячного циклу: пік о 13:00, нуль вночі"""
    return max(0, -0.02 * 4 **2 + 1)

def generate_station_data(station_id):
    """Генерація реалістичних даних для окремої станції"""
    solar_factor = get_solar_factor()
    
    cloud_factor = random.uniform(0.5, 1.0)
    
    base_radiation = solar_factor * 1000 
    current_radiation = base_radiation * cloud_factor

    current_efficiency = random.uniform(8, 22)
    if random.random() < 0.05: 
        current_efficiency = random.uniform(8, 11)
        

    capacity = 1 + (int(station_id.split('_')[1]) % 25)
    current_power = (current_radiation / 1000) * (current_efficiency / 20) * capacity
    
   
    temp = 20 + (solar_factor * 40) + random.uniform(0, 10)
    
    return {
        'power': round(current_power, 2),
        'temp': round(temp, 1),
        'radiation': round(current_radiation, 1),
        'efficiency': round(current_efficiency, 2)
    }

def main():
 
    start_http_server(8000)
    print("Експортер сонячних метрик запущено на http://localhost:8000/metrics")
    
    stations = [f"STATION_{i:02d}" for i in range(1, 51)]
    
    while True:
        for s_id in stations:
            data = generate_station_data(s_id)
            
 
            POWER_MW.labels(station_id=s_id).set(data['power'])
            PANEL_TEMP.labels(station_id=s_id).set(data['temp'])
            RADIATION.labels(station_id=s_id).set(data['radiation'])
            EFFICIENCY.labels(station_id=s_id).set(data['efficiency'])
            STATION_STATUS.labels(station_id=s_id).set(1)
            MESSAGES_SENT.labels(station_id=s_id).inc()
            
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Оновлено метрики для 50 станцій.")
        time.sleep(10)

if __name__ == "__main__":
    main()