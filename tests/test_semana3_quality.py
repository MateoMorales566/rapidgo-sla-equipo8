from semana3.julian.quality import evaluate_quality
import pytest

def good_profile():
    return {'columns': ['order_id','status','zone','event_time','promised_at'],
            'total_rows': 1, 'invalid_count': 0, 'sample_problems': []}

def test_quality_passes():
    assert evaluate_quality(good_profile())['passed']

def test_quality_rejects_missing_column():
    p=good_profile();p['columns'].remove('zone')
    with pytest.raises(ValueError, match='QUALITY_GATE_FAILED'):
        evaluate_quality(p)

def test_quality_rejects_bad_status():
    p=good_profile();p['invalid_count']=1; p['sample_problems']=['Fila 0: estado no válido']
    with pytest.raises(ValueError, match='QUALITY_GATE_FAILED'):
        evaluate_quality(p)

def test_quality_rejects_no_rows():
    p=good_profile();p['total_rows']=0
    with pytest.raises(ValueError, match='QUALITY_GATE_FAILED'):
        evaluate_quality(p)
