from workspace import Workspace
from logger import Logger
from LocalLocation import LocalLocation
from pathlib import Path
from dvengine import DvEngine
from DevEntity import DevEntity
from link import Link
from dbServer import DbServer
from postgreDbServer import PostgreDbServer
from Hub import Hub
from satellite import Satellite
import os
from pathlib import Path
import re

class Lambda:

    def __init__(self,LocalLocation, PostgreDbServer, Logger):
       
        self.Logger = Logger
        self.LocalLocation = LocalLocation
        self.PostgreDbServer = PostgreDbServer
        self.Workspace = None
        self.files_name = None
        self.log_format = None
        self.file_repository = None
        self.archive_repository = None
        self.error_repository = None
        self.log_repository = None
       

    def setRepositories(self):
        workspaces = []
        local = self.LocalLocation
        if os.path.exists('config/repository_config.yml'):
            list_workspace, list_repo = local.readConfigFile('config/repository_config.yml')
        if os.path.isdir(list_repo['archive_repo']):
            self.archive_repository = list_repo['archive_repo']
        if os.path.isdir(list_repo['error_repo']):
            self.error_repository = list_repo['error_repo']
        if os.path.isdir(list_repo['log_repo']):
            self.log_repository = list_repo['log_repo']
        for workspace in list_workspace:
            self.files_name = self.extractFile(workspace.repository)
            self.Workspace = Workspace(workspace.repository)
            self.Workspace.set_fileNames(self.files_name)
               
        message = "Started reading my Repository"
        self.Logger.debug(message)

    def load_data(self) :
        local = self.LocalLocation
        #link = Link()
        dvengine = DvEngine(None, None)
        postgre = self.PostgreDbServer
        self.Logger.debug("Let's establish a connection with the DbServer")
        database = postgre.connectTOdb("postgres", "Azerty00+")
        if postgre.checkIfDBexists(database, 'yellow_tripdata')[0] != 'OK':
            postgre.executeRequest(database, dvengine.getSqlQueryCreateDb('yellow_tripdata'))
            self.Logger.debug("I created the Database {}".format(self.files_name[0].rsplit("_", 1)[0]))
        
        try:
            for file in self.files_name :
                print("file> ",file)
                db_name = 'yellow_tripdata'
                suffix = file.split("_", 2)[1]
                database = postgre.connectTOdbComplete(db_name,"postgres", "Azerty00+")
                postgre.executeRequest(database, dvengine.getSqlQueryCreateSchema('Staging'))
                # print(postgre.checkIfTableExists(database, 'stg_'+suffix))
                #postgre.dropstagingTable(database, 'staging.stg_'+suffix)
                if postgre.checkIfTableExists(database, 'stg_'+suffix) != 'OK':
                    request2 = dvengine.getSqlQueryCreateStaging(self.Workspace.repository, file, 'Staging')
                    postgre.executeRequest(database, request2)
                    self.Logger.debug("I created the Staging{}".format(suffix))
                path = self.Workspace.repository
                file_name = os.path.basename(file)
                print("file_name> ",file_name)
                postgre.loadFile(database, path+'\\'+file_name, 'staging' , 'stg_'+suffix, ',')
                postgre.executeRequest(database, dvengine.getSqlQueryCreateSchema('raw_dv'))
                list_files = local.readRegexFile("config/files_config.yml")
                for f in list_files:
                    if re.match(f.regex, file):
                        config = f.file_name
                        print("config> ",config)
                list_hubs, list_satellites, list_links = local.readObjectFile(config)
                print('i found >'+ str(len(list_links)) +" link.")
                message = "Started reading my Objects"
                self.Logger.debug(message)
                message= 'I found {} HUB, {} Satellite'.format(len(list_hubs), len(list_satellites))
                self.Logger.debug(message)

                for entity in list_hubs:
                    print('new hub> ',entity.name)
                    name = entity.name.removeprefix('HUB_')
                    hub = Hub(entity.name, entity.business_key, entity.fields)
                    dvengine = DvEngine(hub, None)
                    if postgre.checkIfTableExists(database, entity.name.lower()) != 'OK':
                        request1 = dvengine.getSqlQueryCreateDevEntity('raw_dv', None)
                        postgre.executeRequest(database, request1)
                        self.Logger.debug("I created the {}".format(entity.name))
                    request = dvengine.insertiORupdateEntity(name)
                    postgre.executeRequest(database, request)
                for entity in list_satellites:
                    print("new sat")
                    name = entity.name.removeprefix('SAT_')
                    sat = Satellite(entity.name, entity.business_key, entity.fields)
                    dvengine = DvEngine(sat, None)
                    if postgre.checkIfTableExists(database, entity.name.lower()) != 'OK':
                        request1 = dvengine.getSqlQueryCreateDevEntity2('raw_dv', None)
                        postgre.executeRequest(database, request1)
                    request = dvengine.insertiORupdateEntity(name)
                    postgre.executeRequest(database, request)
                    #dvengine.insertiORupdateEntity(file_name)
                    self.Logger.debug("I created the {}".format(entity.name))

                for link in list_links:
                    print("new link")
                    l = Link(link.name, link.member, link.fields, link.business_key)
                    dvengine = DvEngine(None, l)
                    if postgre.checkIfTableExists(database, link.name.lower()) != 'OK':
                        request1 = dvengine.getSqlQueryCreateLink2(None, 'raw_dv')
                        postgre.executeRequest(database, request1)
                    request = dvengine.insertiORupdateLink2(list_hubs, file)
                    postgre.executeRequest(database,request)
                    name = link.name.removeprefix('LINK_')
                    satellite = Satellite('SAT_'+name, link.business_key, link.fields)
                    print('iciiiiiiiiiii')
                    print(satellite.business_key)
                    dvengine1 = DvEngine(satellite, None)
                    name = 'sat_'+ name.lower()
                    print('ici non')
                    print(satellite)
                    if postgre.checkIfTableExists(database, name) != 'OK':
                        postgre.executeRequest(database,dvengine1.getSqlQueryCreateDevEntity2('raw_dv', None))
                    request = dvengine1.insertiORupdateEntity(file.split("_", 2)[1])
                    postgre.executeRequest(database,dvengine1.insertiORupdateEntity(file.split("_", 2)[1]))

                local.moveFile(self.Workspace.repository+'\\'+file_name, self.archive_repository+'\\'+file_name)


        except Exception as e:
            self.Logger.error("ERROR:{}".format(e))
            local.moveFile(self.Workspace.repository+'\\'+file_name, self.error_repository+'\\'+file_name)
            raise
               
            

    def extractFile(self, repository):
        list_files = []
        for file in os.listdir(repository):
            match = re.search("\.csv$", file)
            if match :
                list_files.append(file)
        return list_files  