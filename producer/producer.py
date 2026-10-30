import json
import uuid
import random
from datetime import datetime, timedelta

events = ['created', 'picked_up', 'in_transit', 'delivered', 'cancelled']

def generate_sla_event():
    """Genera un evento de entrega SLA con el contrato JSON completo."""
    event = {
        'event_id': f'evt-{uuid.uuid4().hex[:6]}',
        'event_type': 'delivery_status',
        'event_time': datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'order_id': f'order-{random.randint(100, 999)}',
        'zone': random.choice(['Norte', 'Sur', 'Este', 'Oeste', 'Centro']),
        'distance_km': round(random.uniform(0.5, 15.0), 1),
        'promised_at': (datetime.now() - timedelta(minutes=random.randint(5, 30))).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'picked_up_at': (datetime.now() - timedelta(minutes=random.randint(0, 20))).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'delivered_at': (datetime.now() + timedelta(minutes=random.randint(-10, 10))).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'status': random.choice(events)
    }
    return event

if __name__ == '__main__':
    # Generar y guardar un evento de muestra
    with open('sla_event_sample.json', 'w') as f:
        json.dump(generate_sla_event(), f, indent=2)
    
    # Generar y guardar un lote de 5 eventos
    with open('sla_events_batch.json', 'w') as f:
        json.dump([generate_sla_event() for _ in range(5)], f, indent=2)
    
    print('Producer events generated successfully')