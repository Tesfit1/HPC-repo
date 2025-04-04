import logging

logging.basicConfig(filename="batch.log", level=logging.INFO, format="%(asctime)s - %(message)s")

def run_script(script_name):
    try:
        logging.info(f"Running {script_name}...")
        result = subprocess.run(["python", script_name], check=True, capture_output=True, text=True)
        logging.info(result.stdout)
    except subprocess.CalledProcessError as e:
        logging.error(f"Error in {script_name}: {e.stderr}")
        raise
