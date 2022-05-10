import yaml

class DevEntity:
  """Employee class."""
  def __init__(self, name, fields, business_key):
      self.name = name
      self.fields = fields
      self.business_key = business_key

def entity_constructor(loader: yaml.SafeLoader, node: yaml.nodes.MappingNode) -> DevEntity:
    return DevEntity(**loader.construct_mapping(node))

