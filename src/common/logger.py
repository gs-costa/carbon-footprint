import logging


class Logger:
    """Logger class for the application."""

    def __init__(self, name: str | None = None, level: int = logging.INFO):
        self.logger = logging.getLogger(name or __name__)
        self.logger.setLevel(level)

        if not self.logger.handlers:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(level)

            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
            )
            console_handler.setFormatter(formatter)

            self.logger.addHandler(console_handler)

    def error(self, msg: str) -> None:
        self.logger.error(msg)

    def warning(self, msg: str) -> None:
        self.logger.warning(msg)

    def info(self, msg: str) -> None:
        self.logger.info(msg)
