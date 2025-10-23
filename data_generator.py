import random
import json
import time

def generate_solar_data(device_id):
    # device_id: "SOLAR_KV_001" до "SOLAR_KV_050" [cite: 557]
    # power_output: 0.5-25.0 кВт [cite: 558]
    # irradiance: 100.0-1200.0 Вт/м² [cite: 566]
    # cloud_factor: 0.0-1.0 (0=ясно, 1=хмарно) [cite: 567]
    return {
        "device_id": device_id,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "power_output": round(random.uniform(0.5, 25.0), 2),
        "temperature": round(random.uniform(25.0, 65.0), 1),
        "voltage": round(random.uniform(350.0, 450.0), 1),
        "current": round(random.uniform(10.0, 60.0), 1),
        "status": random.choice(["generating", "standby"]),
        "irradiance": round(random.uniform(100.0, 1200.0), 1),
        "cloud_factor": round(random.uniform(0.0, 1.0), 2),
        "panel_tilt": random.choice(["optimal", "fixed"])
    }
print(generate_solar_data("SOLAR_KV_015"))
