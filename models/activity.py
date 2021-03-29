from mongoengine import Document,StringField,BooleanField, ReferenceField, IntField, FloatField




class Activities(Document):
    activityId = IntField()
    agencyId = IntField()
    bankFinanced = FloatField()
    contractType = StringField()
    description = StringField()
    estimatedAmount = FloatField()
    link = StringField()
    marketApproach = StringField()
    processStatus = StringField()
    procurementCategory = StringField()
    procurementMethod = StringField()
    projectId = StringField()
    referenceNo = StringField()
    retroactiveFinancing = BooleanField()
    procurementProcess = StringField()
    status = StringField()
    reviewType = StringField()