from LocalLocation import LocalLocation
from logger import Logger
from lambd import Lambda
from postgreDbServer import PostgreDbServer


def start_process():
 
    local=LocalLocation()
    postgre = PostgreDbServer()
   
    try:
        list_works, list_repositories = local.readConfigFile("config/repository_config.yml")
        logger = Logger(list_repositories['log_repo'], "logger.log")
        lam = Lambda(local, postgre, logger)
        lam.setRepositories()
        lam.load_data()

    except Exception as e:
        logger.error("ERROR:{}".format(e))
        raise

if __name__ == "__main__":
    start_process()