import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

SCRIPTS = [
    "src/ingestion/generate_retail_data.py",
    "src/etl/transform_retail_orders.py",
    "src/etl/validate_retail_orders.py",
    "src/etl/aggregate_retail_orders.py",
]

def run_script(script_path):
    full_path = PROJECT_ROOT / script_path
    print(f"\nRunning {script_path} ...")
    result = subprocess.run([sys.executable, str(full_path)], cwd=PROJECT_ROOT)
    if result.returncode != 0:
        raise SystemExit(f"Pipeline failed while running {script_path}")

def main():
    for script in SCRIPTS:
        run_script(script)
    print("\nRetail pipeline completed successfully.")

if __name__ == "__main__":
    main()