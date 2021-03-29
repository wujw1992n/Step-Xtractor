from mongoengine import Document, StringField, DateTimeField, BooleanField, IntField, ListField

class ContractsTerminations(Document):
    contractId = IntField(unique=True)
    ccpe = BooleanField()
    ccsc = BooleanField()
    data = BooleanField()
    dbpcc = BooleanField()
    dcpe = BooleanField()
    mproc = BooleanField()
    oth = BooleanField()
    reason = StringField()
    terminationDate = DateTimeField(null=True)
    isTerminated = BooleanField()
    contractTerminationReasons = ListField()