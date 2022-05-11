from DevEntity import DevEntity
from link import Link
import pandas
import os

class DvEngine:

    def __init__(self, DevEntity, Link):
        self.DevEntity = DevEntity
        self.Link = Link


    def getSqlQueryCreateDb(self, db_name):
        request = 'create database ' + db_name
        return request

    def getSqlQueryCreateSchema(self, schema_name):
        request = 'create schema IF NOT EXISTS ' + schema_name
        return request

    def getSqlQueryCreateDevEntity(self, schema_name, file_name):
        entity = self.DevEntity
        if type(entity).__name__ == 'Hub':
            begin = 'create table ' +schema_name+'.'+ entity.name + ' ( ' + entity.name.removeprefix('HUB_') + 'HashedKey' + \
                      ' UUID, ' +entity.name.removeprefix('HUB_')+'business_key VARCHAR(100), '
            end = 'load_date TIMESTAMP, source VARCHAR(100), PRIMARY KEY (' + entity.name.removeprefix('HUB_') + 'HashedKey))'
            final = ''.join((begin,end))
            return final
        else:
            begin = 'create table ' +schema_name+'.'+entity.name+' ( ' + entity.name.removeprefix('SAT_') + 'HashedKey' + \
                      ' UUID,'
            end = ''
            for i in entity.fields :
                body = i + ' ' + entity.fields[i]['type'] + '(' + str(entity.fields[i]['taille']) + '), '
                if entity.fields[i]['type'] == 'int4':
                    body = i + ' ' + entity.fields[i]['type'] + ','
                end = body + end 
            final = 'load_date TIMESTAMP, source VARCHAR(100), extract_date TIMESTAMP, end_date TIMESTAMP, Hash_diff UUID, PRIMARY KEY (' + entity.name.removeprefix('SAT_') + 'HashedKey, load_date))'
            final = begin + end + final
            return final 
        
    def getSqlQueryCreateDevEntity2(self, schema_name, file_name):
        entity = self.DevEntity
        if type(entity).__name__ == 'Hub':
            begin = 'create table ' +schema_name+'.'+ entity.name + ' ( ' + entity.name.removeprefix('HUB_') + 'HashedKey' + \
                      ' UUID, ' +entity.name.removeprefix('HUB_')+'business_key VARCHAR(100), '
            end = 'load_date TIMESTAMP, source VARCHAR(100), PRIMARY KEY (' + entity.name.removeprefix('HUB_') + 'HashedKey))'
            final = ''.join((begin,end))
            return final
        else:
            print(entity.name)
            print(file_name)
            begin = 'create table ' +schema_name+'.'+entity.name+' ( ' + entity.name.removeprefix('SAT_') + 'HashedKey' + \
                      ' UUID,'
            end = ''
            for i in entity.fields :
                body = i + ' ' + entity.fields[i]['type'] + '(' + str(entity.fields[i]['taille']) + '), '
                if entity.fields[i]['type'] in ['int4','float4']  :
                    body = i + ' ' + entity.fields[i]['type'] + ','
                end = body + end 
            final = 'load_date TIMESTAMP, source VARCHAR(100), extract_date TIMESTAMP, end_date TIMESTAMP, Hash_diff UUID, PRIMARY KEY (' + entity.name.removeprefix('SAT_') + 'HashedKey, load_date))'
            final = begin + end + final
            return final 
    
    def getSqlQueryCreateLink(self, entities, schema_name):
        link = self.Link
        begin = 'create table ' +schema_name+'.'+ link.name + ' ( ' + link.name.removeprefix('LINK_') + 'HashedKey CHAR(100), load_date TIMESTAMP, source VARCHAR(100),'
        request = ''
        for m in link.member:
            rest = m+ 'HashedKey CHAR(100), '
            request = request + rest
        final = ''.join((begin, request)) 

        final = final + ' PRIMARY KEY (' + link.name.removeprefix('LINK_')+ 'HashedKey) ' 
        end =''
        for m in link.member:
           body = ' ,CONSTRAINT '+m+'_ibfk FOREIGN KEY ('+m+ 'HashedKey'+') ' +\
                        ' REFERENCES '+ schema_name+'.HUB_'+ m +'('+ m + 'HashedKey'+')'
           end = end + body
        final = final + end + ')'
        return final
        

    def getSqlQueryCreateStaging(self, repository, file_name, schema_name):
        path = repository +"\\" + file_name
        columns = self.getColumnsName(path)
        suffix = file_name.split("_", 2)[1]
        begin = 'CREATE TABLE ' +schema_name+'.STG_' + suffix
        end =''
        for col in columns :
            body = col +' TEXT, '
            end = end + body
           
        request = ' ( '.join((begin, end))
        request = self.rreplace(request, ',', ')')
        return request

    def createStaging(self):
        entity = self.DevEntity
        name = entity.name.rsplit("_", 2)[2]
        begin = 'CREATE TABLE STAGING_' + name
        end = ''
        for i in entity.fields :
                body = i + ' ' + entity.fields[i]['type'] + '(' + str(entity.fields[i]['taille']) + '), '
                end = body + end 
        request = ' ( '.join((begin, end))
        request = self.rreplace(request, ',', ')')

        return request

    def insertiORupdateEntity(self, file_name):
        entity = self.DevEntity
        if type(entity).__name__ == 'Hub':
            begin = 'INSERT INTO raw_dv.'+ entity.name + ' SELECT CAST(MD5(concat('
            end =''
            for bk in entity.business_key:
                body = 'UPPER(TRIM(COALESCE('+bk + '::text))), \',\' , '
                end = body + end
                
            end = self.rreplace(end,', \',\' , ','')
            request = begin + end+')) AS UUID), concat( ' + end +  '), CURRENT_DATE, \'CSV\' FROM staging.STG_' + file_name + ' ON CONFLICT DO NOTHING'
            return request

        elif type(entity).__name__ == "Satellite":
            print("entity name> ",entity.name)
            begin = 'INSERT INTO raw_dv.' +entity.name+' SELECT CAST(MD5(concat('
            end =''
            first = ''
            for bk in entity.business_key:
                body = 'UPPER(TRIM(COALESCE('+bk + '::text))), \',\' , '
                first = body + first
                print("first> ")
                print(first)
                
            first = self.rreplace(first,', \',\' , ',')) AS UUID), ')
            end =''
            for i in entity.fields:
                type_field = entity.fields[i]['type']
                body = 'COALESCE(stg.'+i +f'::{type_field}) ,'
                end = body + end
            end = self.rreplace(end,', \',\' , ','')
            print("end> ")
            print(end)

            request = begin + first + end+' CURRENT_DATE, \'CSV\', CURRENT_DATE, NULL, CAST(MD5(concat('
            last =''
            for field in entity.fields:
                body = 'UPPER(TRIM(COALESCE('+field + '::text))), \',\' , '
                last = body + last
            last = self.rreplace(last,', \',\' , ','')
            request = request + last + ')) AS UUID ) FROM staging.STG_' + file_name + ' stg WHERE NOT EXISTS (' + \
                      'SELECT \'1\' FROM raw_dv.'+entity.name+ ' sat ' + \
                      'WHERE sat.hash_diff = CAST(MD5(concat('
            end =''
            for field in entity.fields:
                body = 'UPPER(TRIM(COALESCE(stg.'+field + '::text))), \',\' , '
                end = body + end
            end = self.rreplace(end,', \',\' , ',')) AS UUID)')
            request = request + end + ' AND CAST(MD5(concat('
            last =''
            for bk in entity.business_key:
                body = 'UPPER(TRIM(COALESCE(stg.'+bk + '::text))), \',\' , '
                last = body + last
            last = self.rreplace(last,', \',\' , ',')) AS UUID)')

            final = last + ' = sat.'+entity.name.removeprefix('SAT_')+'hashedkey) ON CONFLICT DO NOTHING'
            print("request + final>>")
            print(request + final)
            return request + final

    # def insertiORupdateLink(self, entities, file_name):
    #     suffix = file_name.split("_", 2)[1]
    #     link = self.Link
    #     begin = 'INSERT INTO raw_dv.'+ link.name+ ' SELECT DISTINCT MD5(concat('
    #     request = ''
    #     end1=''
    #     end2=''
    #     end3= ''
    #     end=''
    #     end_f = ''
    #     mid=''
    #     befor =''
    #     member = link.member
    #     for i in member:
    #         body = 'BTRIM(LOWER('+i[0:2] +'.'+ member[i]['business_key'] + ')) , \',\' ,'
    #         end1 = end1 + body
    #     end1 = self.rreplace(end1,' , \',\' ,','))')
    #     request = begin + end1 + ' , CURRENT_DATE, \'CSV\', '
    #     for i in member:
    #         body = 'MD5(concat(BTRIM(LOWER('+i[0:2] +'.'+ member[i]['business_key'] + ')))),'
    #         end2 = end2 + body
    #     end2 = self.rreplace(end2, ',', ' FROM ')
    #     request = request + end2
    #     for i in member:
    #         body = ' staging.stg_'+i+' '+i[0:2]+','
    #         end3 = end3 + body
    #     end3 = self.rreplace(end3, ',',' WHERE ')
    #     request = request +end3
    #     for i in member:
    #         for fk in member[i]['foreign_key']:
    #             print(len(member[i]['foreign_key']))
    #             if len(member[i]['foreign_key']) > 1 :
    #                 befor =  i[0:2]  +'.'+ member[i]['business_key']+ ' IN ('
    #                 body = suffix[0:2]+'.'+fk + ' , '
    #                 mid = mid + body 
                    
    #             else:
    #                 body2 = i[0:2] +'.'+ member[i]['business_key']  + '=' + suffix[0:2]+ '.' +fk + ' AND ' 
    #                 end_f = end_f + body2 
    #     if mid != '':
    #         mid = self.rreplace(mid, ' , ', '')
    #         end = end + befor + mid +') AND '+ end_f 
    #         print(befor)
    #         print('je suis end', end)
    #     else :
    #         end = end + end_f
    #     end = self.rreplace(end, ' AND ', '')
    #     request = request + end + ' ON CONFLICT DO NOTHING'
    #     print(request)   
    #     return request


    def getSqlQueryCreateLink2(self, entities, schema_name):
        link = self.Link
        begin = 'create table ' +schema_name+'.'+ link.name + ' ( ' + link.name.removeprefix('LINK_') + 'HashedKey UUID , load_date TIMESTAMP, source VARCHAR(100),'
        request = ''
        for m in link.member:
            rest = m.removeprefix('Hub_')+ 'HashedKey  UUID, '
            request = request + rest
        final = ''.join((begin, request)) 

        final = final + ' PRIMARY KEY (' + link.name.removeprefix('LINK_')+ 'HashedKey) ' 
        final = final + ')'
        return final

    
    # def get_bk(self,bk):
    #     if type(bk) == list:
    #         return bk[0]
    #     return bk

    def insertiORupdateLink2(self, entities, file_name):
        suffix = file_name.split("_", 2)[1]
        link = self.Link
        begin = 'INSERT INTO raw_dv.'+ link.name+ ' SELECT DISTINCT CAST(MD5(concat('
        request = ''
        end1=''
        end2=''
        for bk in link.business_key:
             body = 'UPPER(TRIM(COALESCE('+bk + '::text))), \',\' , '
             end1 = body + end1
        end1 = self.rreplace(end1,', \',\' , ','')
        request = begin + end1+')) AS UUID), CURRENT_DATE, \'CSV\','
        for bk in link.business_key:
            body = 'CAST(MD5(concat(BTRIM(LOWER('+bk + '::text)))) AS UUID),'
            end2 = end2 + body
        end2 = self.rreplace(end2, ',', ' FROM')
        request = request + end2 +' staging.stg_' +suffix+ ' ON CONFLICT DO NOTHING'
        return request

    def rreplace(self, s, old, new):
        return (s[::-1].replace(old[::-1],new[::-1], 1))[::-1]

    def getColumnsName(self, path):
        file = pandas.read_csv(path, dtype='unicode')
        return file.columns