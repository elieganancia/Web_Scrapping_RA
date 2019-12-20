import logging
import sys


class Scrappy_logger:

    def __init__(self):
        self.filename = "RA_Scrappy.log"
        self.logger = logging.getLogger("scrappy_loggger")
        self.logger.setLevel(logging.INFO)

        self.file_handler = logging.FileHandler(self.filename)
        self.file_handler.setLevel(logging.INFO)

        self.format_log = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.file_handler.setFormatter(self.format_log)

        self.logger.addHandler(self.file_handler)

    def info(self, message_):
        self.logger.info(message_)

    def warning(self, message_):
        self.logger.warning(message_)

    def error(self, message_):
        self.logger.error(message_)

    def critical(self, message_):
        self.logger.critical(message_)


class Scrappy_info:

    def __init__(self):
        self.filename = "RA_Scrappy.log"
        self.logger = logging.getLogger("scrappy_information")
        self.logger.setLevel(logging.INFO)

        self.logger.addHandler(logging.StreamHandler(sys.stdout))

    def info(self, message_):
        self.logger.info(message_)