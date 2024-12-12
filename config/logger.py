import logging
import os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))

logger = logging.getLogger("MEDWARE")
formatter = logging.Formatter(
    "[%(asctime)s] %(levelname)s in %(module)s: %(message)s"
)
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(f"{project_root}/logs/medware.log")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

if __name__ == "__main__":
    logger.info("Starting MEDWARE")
    logger.info(f"Project root: {project_root}")
    logger.info("Finished MEDWARE")