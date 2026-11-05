from semana3.kevin.reporting import summarize_records, save_report
import json

def test_deduplicated_metrics():
    rows=[{'order_id':'1','event_time':'2026-01-01','status':'in_transit'},
          {'order_id':'1','event_time':'2026-01-02','status':'delivered','delivery_minutes':50,'is_late':True},
          {'order_id':'2','event_time':'2026-01-02','status':'delivered','delivery_minutes':30,'is_late':False}]
    result=summarize_records(rows)
    assert result['pedidos_unicos']==2
    assert result['tiempo_promedio_min']==40
    assert result['porcentaje_fuera_sla']==50

def test_report_written(tmp_path):
    output=save_report({'metrics':{'pedidos_unicos':3}},tmp_path)
    assert json.loads((tmp_path/'sla_semana3_latest.json').read_text())['metrics']['pedidos_unicos']==3
    assert output['size_bytes']>0
