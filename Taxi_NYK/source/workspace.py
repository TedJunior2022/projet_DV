import yaml
class Workspace:

    def __init__(self, repository):
        self.repository = repository
        

    
    def get_repository(self):
        print("getter method called")
        return self.repository
       
     # a setter function
 
    def set_repository(self, repository):
        self.repository = repository

    def get_fileNames(self):
        print("getter method called")
        return self.fileNames
       
     # a setter function
 
    def set_fileNames(self, fileNames):
        self.fileNames = fileNames

    def __str__(self):
        return "I am the workspace " + self.repository

def workspace_constructor(loader: yaml.SafeLoader, node: yaml.nodes.MappingNode) -> Workspace:
    return Workspace(**loader.construct_mapping(node))