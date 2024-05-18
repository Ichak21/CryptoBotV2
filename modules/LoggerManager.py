import logging
import os



MODULE_NAME = "MDM - Market Data Manager"

PATH_LOGS = '../logs/crypto_bots.log'


def path_from_relative_to_absolut(relative:str):
    #Create absolut path with relative one 
    script_directory = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(script_directory, relative)

class LoggerManager:
    def __init__(self, path:str=PATH_LOGS):
        # Configuration du journal
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            filename=path_from_relative_to_absolut(path),
        )
        # Création d'un objet logger pour cette classe
        self.logger = logging.getLogger(__name__)

    def log_info(self, module, message):
        self.logger.info(f'{module} | {message}')

    def log_warning(self, module, message):
        self.logger.warning(f'{module} | {message}')

    def log_error(self, module, message):
        self.logger.error(f'{module} | {message}')
        
# =========================================Example utilisation -  ======================================================

def main():
    logs = LoggerManager()
    logs.log_info("MDM", "loool1")
    logs.log_error("MDM", "loool2")
    logs.log_warning("MDM", "loool3")
    
if __name__ == "__main__":
    main()