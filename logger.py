# logger.py
import logging

LOG_FILE = "lookup.log"

# Создаем логгер
logger = logging.getLogger("GTIN_Lookup")
logger.setLevel(logging.INFO)  # Можно DEBUG для детальной информации

# Файл логов
file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(file_formatter)

# Добавляем обработчики
if not logger.handlers:
    logger.addHandler(file_handler)