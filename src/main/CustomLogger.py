import logging

class CustomLogger:

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

    def log_custom_with_message(self, message: str, custom_level: int) -> None:
        self.logger.log(custom_level, message)

    def return_type_level(self, level: str) -> int:
        levels = {
            "ERROR": logging.ERROR,
            "INFO": logging.INFO
        }
        return levels.get(level.upper(), logging.INFO)