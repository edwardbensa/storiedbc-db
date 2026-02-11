"""Run full ETL pipeline with realtime logging"""

# Imports
import sys
import subprocess
from loguru import logger
from src.utils.files import wipe_directory
from src.config import DATA_DIR


def run_script(script_path):
    """Runs a script and returns the exit code."""
    logger.info(f">>> Starting: {script_path}")
    try:
        # We use the return code to decide the next move
        process = subprocess.run([sys.executable, script_path], check=False, text=True)
        return process.returncode
    except Exception as e:  # pylint: disable=W0718
        logger.error(f"Execution error: {e}")
        return 1 # Generic error


def main():
    """Main function to run the ELT pipeline in phases."""
    # Phase 1: Extraction
    run_script("src/etl/extract_gsheet.py")

    # Phase 2: Staging to Main
    staging2main_status = run_script("src/etl/transform_staging2main.py")

    if staging2main_status == 0:
        run_script("src/etl/load_mongo.py")
        run_script("src/etl/sync_images.py")
    elif staging2main_status == 10:
        logger.info("No staging updates. Skipping main DB load and image sync phase.")

    # Phase 3: Main to Aura
    main2aura_status = run_script("src/etl/transform_main2aura.py")

    if main2aura_status == 0:
        run_script("src/etl/load_aura.py")
    elif main2aura_status == 10:
        logger.success("AuraDB is already in sync with Main. Pipeline complete.")

if __name__ == "__main__":
    wipe_directory(DATA_DIR, terminate=False)
    main()
