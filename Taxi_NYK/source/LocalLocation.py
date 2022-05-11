import logging
from location import Location 
from DevEntity import DevEntity
from satellite import Satellite
from workspace import Workspace
from link import Link
from file import File
from yamlinclude import YamlIncludeConstructor
import file
import link
import satellite
import DevEntity
import workspace
from Hub import Hub
import yaml
from pathlib import Path
import shutil


class LocalLocation(Location):
   
    def get_Objectloader(self):
        loader = yaml.SafeLoader
        YamlIncludeConstructor.add_to_loader_class(loader_class = loader,base_dir = "")
        loader.add_constructor("!DevEntity", DevEntity.entity_constructor)
        loader.add_constructor("!Link", link.link_constructor)
        loader.add_constructor("!File", file.file_constructor)
        return loader

    def get_Configloader(self):
        loader = yaml.SafeLoader
        loader.add_constructor("!Workspace", workspace.workspace_constructor)
        return loader

    def readObjectFile(self,location):
        data = yaml.load(open(location, "rb"), Loader=self.get_Objectloader())
        hubs = []
        satellites = []
        links = []
        if data["DevEntities"] != None: 
            list_DvEntity = data["DevEntities"]
            for o in list_DvEntity:
                name_hub = "HUB_"+o.name
                name_sat = "SAT_"+o.name
                hubs.append(Hub(name_hub, o.business_key, o.fields))
                satellites.append(Satellite(name_sat, o.business_key, o.fields))

        if data["Links"] != None :
            list_Links = data["Links"]
            for l in list_Links:
                name_link = "LINK_"+l.name
                member = l.member
                for i in member:
                    print(i)
                if l.fields != None:
                    fields = l.fields
                    business_key = l.business_key
                links.append(Link(name_link, member, fields, business_key))
        
        return hubs, satellites, links

    def readObjectLinksFile(self,location):
        data = yaml.load(open(location, "rb"), Loader=self.get_Objectloader())
        links = []
        if data["Links"] != None :
            list_Links = data["Links"]
            for l in list_Links:
                name_link = "LINK_"+l.name
                member = l.member
                links.append(Link(name_link, member))
            return links

    def readConfigFile(self,location):
        data = yaml.load(open(location, "rb"), Loader=self.get_Configloader())
        list_repositories = data["Others"]
        works = []
        list_Works = data["Workspaces"]
        for o in list_Works:
            p = Path(o.repository)
            if p.exists() & p.is_dir() :
                works.append(Workspace(o.repository))
            
        return list_Works, list_repositories

    def readRegexFile(self, location):
        data = yaml.load(open(location, "rb"), Loader=self.get_Objectloader())
        files = []
        list_files = data["Files"]
        for o in list_files:
            regex = o.regex
            file_name = o.file_name
            files.append(File(regex, file_name))

        return files
    
    def moveFile(self, src, dst):
        shutil.move(src, dst)