"""Compatibilidad: la implementación del quality gate vive en ``semana3.quality``."""
from semana3.quality import evaluate_quality, inspect_parquet

__all__ = ["inspect_parquet", "evaluate_quality"]
