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
    """Main function to run the ELT pipeline in sequence."""
    scripts = [
        "src/etl/extract_gsheet.py",
        "src/etl/transform_bronze.py", # Gatekeeper
        "src/etl/load_mongo.py",
        "src/etl/sync_images.py",
        "src/etl/transform_silver.py",
        "src/etl/load_aura.py"
    ]

    for script in scripts:
        exit_code = run_script(script)

        if exit_code == 0:
            continue # Normal success, keep going

        if exit_code == 10:
            logger.success("Pipeline stopped gracefully: No new data to process.")
            sys.exit(0) # Exit the whole ETL with success

        # Any other code is a failure
        logger.critical(f"Pipeline aborted: {script} failed with code {exit_code}")
        sys.exit(1)

    logger.success("ETL Pipeline completed successfully!")

if __name__ == "__main__":
    wipe_directory(DATA_DIR)
    main()
