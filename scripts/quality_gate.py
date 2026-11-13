"""Quality Gate RapidGo (Semana 4).

Verifica que el repositorio tenga lo mínimo para liberar v1.0.0.
Uso: python scripts/quality_gate.py
Código de salida 0 = OK, 1 = falla (el release no debe crearse).
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

REQUIRED_PATHS = [
    "README.md",
    "docker-compose.yml",
    "requirements.txt",
    "requirements-airflow.txt",
    ".github/workflows/ci.yml",
    ".github/workflows/release.yml",
    "producer",
    "spark",
    "sla",
    "tests",
    "samples/sla_event.json",
    "airflow/dags",
    "datalake/README.md",
    "docs/arquitectura.png",
    "docs/contrato_evento.json",
    "docs/kpis_alertas.md",
    "docs/checkpoints/semana1.md",
    "docs/checkpoints/semana2.md",
    "docs/checkpoints/semana3.md",
    "docs/evidencias.md",
    "docs/releases.md",
    "scripts/quality_gate.py",
]

README_KEYWORDS = [
    "Kafka",
    "Spark",
    "Airflow",
    "KPI",
    "Docker",
    "Instalación",
    "Ejecución",
    "Resultados",
    "Responsables",
]

COMPOSE_SERVICES = ["kafka", "spark-job", "airflow"]
MIN_ADRS = 3
MIN_DAG_TASKS = 5


def check_all(root: Path) -> list:
    errors = []

    for rel in REQUIRED_PATHS:
        if not (root / rel).exists():
            errors.append(f"Falta: {rel}")

    readme = root / "README.md"
    if readme.exists():
        text = readme.read_text(encoding="utf-8", errors="ignore")
        for kw in README_KEYWORDS:
            if kw.lower() not in text.lower():
                errors.append(f"README no menciona '{kw}'")

    compose = root / "docker-compose.yml"
    if compose.exists():
        text = compose.read_text(encoding="utf-8", errors="ignore")
        for service in COMPOSE_SERVICES:
            if not re.search(rf"^\s{{2}}{re.escape(service)}:", text, flags=re.MULTILINE):
                errors.append(f"docker-compose.yml no define el servicio '{service}'")

    decisions = root / "docs" / "decisions"
    adrs = list(decisions.glob("ADR-*.md")) if decisions.exists() else []
    if len(adrs) < MIN_ADRS:
        errors.append(f"Se esperaban >= {MIN_ADRS} ADR en docs/decisions (hay {len(adrs)})")

    dags = list((root / "airflow" / "dags").glob("*.py")) if (root / "airflow" / "dags").exists() else []
    if not dags:
        errors.append("No hay DAGs en airflow/dags")
    else:
        max_tasks = 0
        for dag in dags:
            src = dag.read_text(encoding="utf-8", errors="ignore")
            n = len(re.findall(r"\w+Operator\s*\(", src)) + len(re.findall(r"@task", src))
            max_tasks = max(max_tasks, n)
        if max_tasks < MIN_DAG_TASKS:
            errors.append(f"El DAG debe tener >= {MIN_DAG_TASKS} tasks (detectadas {max_tasks})")

    return errors


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    errors = check_all(root)
    if errors:
        print("QUALITY GATE: FALLO")
        for error in errors:
            print(f" - {error}")
        return 1
    print("QUALITY GATE: OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
