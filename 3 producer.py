from kafka import KafkaProducer
import json
import time
from random import choice, randint
from datetime import datetime, timezone

KAFKA_BROKER = "rc1a-fhvkfepi1uj85ud1.mdb.yandexcloud.net:9091"
USERNAME = "producer_user"
PASSWORD = "hgfdsa12"
SSL_CERT_PATH = "CA.pem"  
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-512",
    sasl_plain_username=USERNAME,
    sasl_plain_password=PASSWORD,
    ssl_cafile=SSL_CERT_PATH,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def create_event():
    statuses = ["approved", "rejected", "manual_review"]
    risk_levels = ["low", "medium", "high"]
    doc_types = ["passport", "driver_license", "utility_bill"]
    regions = ["DE-HE", "FR-IDF", "PL-MZ", "CZ-PR"]
    
    return {
        "application_id": f"loan_{randint(1, 1000000)}",
        "customer": {
            "customer_id": f"cust_{randint(1, 9999)}",
            "region": choice(regions)
        },
        "loan": {
            "amount": randint(5000, 50000),
            "term_months": choice([12, 24, 36, 48, 60])
        },
        "scoring": {
            "score": randint(300, 850),
            "risk_level": choice(risk_levels)
        },
        "documents": [
            {"type": choice(doc_types), "status": "verified"} 
            for _ in range(randint(1, 3))
        ],
        "decision_status": choice(statuses),
        "submitted_at": datetime.now(timezone.utc).isoformat()
    }

print("Отправка в Kafka...")
total_bytes = 0
target_bytes = 20 * 1024 * 1024
message_count = 0

while total_bytes < target_bytes:
    event = create_event()
    message_bytes = len(json.dumps(event).encode('utf-8'))
    producer.send('loan_applications', value=event)
    total_bytes += message_bytes
    message_count += 1
    if message_count % 100 == 0:
        print(f"Отправлено {total_bytes / (1024*1024):.2f} МБ ({message_count} сообщений)")
    time.sleep(0.01)

producer.flush()
producer.close()
print(f"Готово. {message_count} сообщений, {total_bytes / (1024*1024):.2f} МБ")