import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("quality_gate", ROOT / "scripts" / "quality_gate.py")
qg = importlib.util.module_from_spec(spec)
spec.loader.exec_module(qg)


def test_gate_falla_en_repo_vacio(tmp_path):
    errors = qg.check_all(tmp_path)
    assert len(errors) > 0


def test_gate_detecta_readme_incompleto(tmp_path):
    (tmp_path / "README.md").write_text("solo texto", encoding="utf-8")
    errors = qg.check_all(tmp_path)
    assert any("README no menciona" in e for e in errors)


def test_gate_en_repositorio_real():
    assert qg.check_all(ROOT) == []
