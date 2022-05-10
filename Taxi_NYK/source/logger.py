import logging
import inspect
import shutil

class Logger():

    def __init__(self, repository, file_name):
        path = repository+"\\"+file_name
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self.formatter = logging.Formatter('%(asctime)s:%(mod_name)s,%(levelname)s:%(message)s')
        self.file_handler = logging.FileHandler(path)
        self.file_handler.setFormatter(self.formatter)
        self.logger.addHandler(self.file_handler)
        

    def debug(self, msg):
        module_stack = inspect.stack()[1][1]
        mod_name = inspect.getmodulename(module_stack)
        d = {'mod_name': mod_name}
        self.logger.debug(msg, extra=d)

    def error(self, msg):
        module_stack = inspect.stack()[1][1]
        mod_name = inspect.getmodulename(module_stack)
        d = {'mod_name': mod_name}
        self.logger.error(msg, extra=d)