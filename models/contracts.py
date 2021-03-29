from mongoengine import Document,StringField, DateTimeField, IntField, FloatField



class Contracts(Document):
    activityId = IntField()
    contractId = IntField(unique=True)
    stepActivityId = IntField()
    agencyId = IntField()
    awardId = IntField()
    baseAmount = FloatField()
    baseCurrency = StringField()
    baseExchangeRate = FloatField()
    baseAmountEquivalence = FloatField()
    description = StringField()
    org = StringField()
    referenceNo = StringField()
    status = StringField()
    totalAmount = FloatField()
    totalCurrency = StringField()
    totalAmountEquivalence = StringField()
    link = StringField()
    projectId = StringField()
    signedDate = DateTimeField()
    amendmentAmount = FloatField()
    completionDate = DateTimeField()
    duration = StringField()