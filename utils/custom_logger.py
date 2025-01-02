import logging
import logging.handlers
import os
from datetime import datetime
import inspect

class CustomLogger:
    def __init__(self, log_dir='other', level=logging.DEBUG, stream_level=logging.INFO):
        path = os.path.join('dump/', log_dir)
        self.log_dir = path
        self.level = level
        self.stream_level = stream_level
        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir)
    
    def get_caller_module_name(self):
        frame = inspect.stack()[2]
        module = inspect.getmodule(frame[0])
        if module:
            module_name = os.path.basename(module.__file__).replace('.py', '')
        else:
            module_name = 'unknown_module'
        return module_name

    def setup_logger(self, name=None):
        if name is None:
            name = self.get_caller_module_name()

        logger = logging.getLogger(name)
        logger.setLevel(self.level)

        # Create file handler which logs even debug messages
        log_filename = f"{name}_{datetime.now().strftime('%Y-%m-%d')}.log"
        file_handler = logging.handlers.RotatingFileHandler(os.path.join(self.log_dir, log_filename), maxBytes=10485760, backupCount=5)
        # file_handler = logging.FileHandler(os.path.join(self.log_dir, log_filename))
        file_handler.setLevel(self.level)

        # Create console handler with a higher log level
        console_handler = logging.StreamHandler()
        console_handler.setLevel(self.stream_level)

        # Create formatter and add it to the handlers
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # Add the handlers to the logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        return logger

# Usage Example
if __name__ == "__main__":
    custom_logger = CustomLogger()
    logger = custom_logger.setup_logger()
    logger.debug("This is a debug message")
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")
    logger.critical("This is a critical message")
