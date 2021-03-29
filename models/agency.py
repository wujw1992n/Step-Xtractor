from mongoengine import Document,StringField, IntField




class Agencies(Document):
    agencyID = IntField()
    projectId = StringField()
    link = StringField(required=False)
    name = StringField()
    status = StringField()
