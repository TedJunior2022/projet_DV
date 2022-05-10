import yaml

class File:

    def __init__(self, regex, file_name):
        self.regex = regex
        self.file_name = file_name


def file_constructor(loader: yaml.SafeLoader, node: yaml.nodes.MappingNode) -> File:
    return File(**loader.construct_mapping(node))





