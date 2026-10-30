"""
Tests unitarios para validación de KPIs y alertas SLA - RapidGo
Autor: Kevin García
"""

import pytest
import json
from datetime import datetime

def load_sample_event():
    """Carga un evento de muestra para testing."""
    with open('../samples/sla_event.json', 'r') as f:
        return json.load(f)

def test_event_has_required_fields():
    """Test que el evento tiene todos los campos requeridos del contrato."""
    event = load_sample_event()
    required_fields = [
        'event_id', 'event_type', 'event_time', 'order_id',
        'zone', 'distance_km', 'promised_at', 'picked_up_at',
        'delivered_at', 'status'
    ]
    for field in required_fields:
        assert field in event, f"Campo faltante: {field}"

def test_event_status_valid():
    """Test que el status es uno de los valores permitidos."""
    event = load_sample_event()
    valid_statuses = ['created', 'picked_up', 'in_transit', 'delivered', 'cancelled']
    assert event['status'] in valid_statuses, f"Status inválido: {event['status']}"

def test_distance_positive():
    """Test que la distancia es positiva."""
    event = load_sample_event()
    assert event['distance_km'] > 0, f"Distancia debe ser positiva: {event['distance_km']}"

def test_event_time_format():
    """Test que el event_time tiene formato ISO 8601."""
    import re
    event = load_sample_event()
    pattern = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$'
    assert re.match(pattern, event['event_time']), f"Formato de tiempo inválido: {event['event_time']}"

def test_kpi_delivery_time():
    """Test de cálculo de KPI: tiempo de entrega."""
    def calculate_delivery_time(event):
        picked = datetime.fromisoformat(event['picked_up_at'].replace('Z', '+00:00'))
        delivered = datetime.fromisoformat(event['delivered_at'].replace('Z', '+00:00'))
        duration_minutes = (delivered - picked).total_seconds() / 60
        return duration_minutes
    
    event = load_sample_event()
    duration = calculate_delivery_time(event)
    # El KPI objetivo es <= 45 minutos
    assert duration <= 45, f"Tiempo de entrega exceeds KPI: {duration:.1f} min > 45 min"

def test_kpi_percentage_outside_sla():
    """Test de cálculo de KPI: porcentaje fuera de SLA."""
    def calculate_percentage_outside(events_list):
        total = len(events_list)
        if total == 0:
            return 0
        outside = sum(1 for e in events_list if e.get('delivered_at', '') > e.get('promised_at', ''))
        return (outside / total) * 100
    
    # Con un evento donde delivered > promised
    test_event = {
        'event_id': 'test-001',
        'promised_at': '2026-10-29T13:00:00Z',
        'delivered_at': '2026-10-29T14:30:00Z'
    }
    # Crear lista con 1 evento fuera SLA y 9 dentro
    test_events = [test_event] + [{'promised_at': '2026-10-29T13:00:00Z', 'delivered_at': '2026-10-29T12:30:00Z'}] * 9
    percentage = calculate_percentage_outside(test_events)
    # KPI objetivo <= 10%
    assert percentage <= 10, f"Porcentaje fuera SLA exceeds KPI: {percentage:.1f}% > 10%"