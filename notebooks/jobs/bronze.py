
# Databricks notebook source

from pathlib import Path
import sys

PROJECT_ROOT = Path.cwd().resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.bronze import run_bronze

config_path = PROJECT_ROOT / "config" / "tables.yml"

run_bronze(
    spark,
    dbutils,
    str(config_path)
)
