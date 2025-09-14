# src/utils/logger_setup.py
import logging
import os
from logging.handlers import RotatingFileHandler

# Persistent log directory on DBFS (survives cluster restarts)
log_dir = "/dbfs/Workspace/Users/malgorzata.gardzinska@tlen.pl/DataBricksStock/logs"
os.makedirs(log_dir, exist_ok=True)

# Log file name (same file for all pipeline runs)
log_file = os.path.join(log_dir, "pipeline.log")

# Create logger
logger = logging.getLogger("PipelineLogger")
logger.setLevel(logging.INFO)  # use logging.DEBUG for more detailed logs

# Add handlers only once
if not logger.hasHandlers():
    # Console output (optional, useful during job runs)
    console_handler = logging.StreamHandler()
    
    # File output with rotation (10MB per file, keep last 5 backups)
    file_handler = RotatingFileHandler(log_file, maxBytes=10_000_000, backupCount=5)
    
    # Log format
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)s | %(name)s | %(message)s'
    )
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    
    # Attach handlers
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
