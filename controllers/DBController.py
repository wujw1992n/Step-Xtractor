from mongoengine import *

from typing import List


class DBController:
    def __init__(self):
        self.connection = None
    
    def create_connection(self, db_name: str,url: str, port: int = 27017, password: str = ''):
        '''
        :param db_name: stepX
        :param url: localhost
        :param port: 27017
        :param password: None
        :return: pymongo.mongo_client.MongoClient
        '''
        self.connection = connect(db_name, host=url, port=port)


    def findOne(self, document: Document, query: dict) -> Document:
        '''
        :param document: Project
        :param query: {'projectId': 'P123123123'}
        :return: Project Object
        '''
        return document.objects(__raw__=query)[0]

    def find(self, document: Document, query: dict) -> List[Document]:
        '''
        :param document: Project
        :param query: {'projectId': 'P123123123'}
        :return: Array of Project Object
        '''
        return document.objects(__raw__=query)

    def updateOrSave(self, document: Document, query: dict, content:list) -> int:
        '''
        :param document: Project
        :param query: {'projectId': 'P123123123'}
        :param content: [{'field': 'title', 'value': 'Ron'},{'field': 'abstract', 'value': 'Ron'}]
        :return: 1 if was a success
        '''
        return document.objects(__raw__=query).update_one(**dict(('set__' + data["field"],data["value"]) for data in content), upsert=True)

    def save(self, document: Document, content:dict) -> int:
        '''
        :param document: Project
        :param query: {'projectId': 'P123123123'}
        :param content: [{'field': 'title', 'value': 'Ron'},{'field': 'abstract', 'value': 'Ron'}]
        :return: 1 if was a success
        '''
        new_document = document()
        if len(content)> 0:
            for param in content:
                setattr(new_document, param["field"], param["value"])
            return new_document.save()
        else:
            return False

