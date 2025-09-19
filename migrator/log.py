import logging
import os
from datetime import datetime
from typing import Optional

_LOGGER_CREATED = False


def setup_logger(log_dir: Optional[str] = None, level: int = logging.INFO) -> logging.Logger:
	global _LOGGER_CREATED
	logger = logging.getLogger("migrator")
	if _LOGGER_CREATED:
		return logger
	logger.setLevel(level)

	formatter = logging.Formatter(
		fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
		datefmt="%Y-%m-%d %H:%M:%S",
	)

	# Console handler
	ch = logging.StreamHandler()
	ch.setLevel(level)
	ch.setFormatter(formatter)
	logger.addHandler(ch)

	# File handler
	if log_dir is None:
		log_dir = os.path.join(os.getcwd(), "logs")
	os.makedirs(log_dir, exist_ok=True)
	log_file = os.path.join(log_dir, f"migrator_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
	fh = logging.FileHandler(log_file, encoding="utf-8")
	fh.setLevel(level)
	fh.setFormatter(formatter)
	logger.addHandler(fh)

	logger.debug("Logger initialized. Log file: %s", log_file)
	_LOGGER_CREATED = True
	return logger
