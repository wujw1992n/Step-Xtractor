from mongoengine import Document, StringField, DateTimeField, IntField,FloatField


class Loans(Document):
    projectId = StringField()
    amount = StringField(required=False)
    approvalDate = DateTimeField(null=True)
    closingDate = DateTimeField(null=True)
    contractAmountPaid = FloatField()
    disbursedAmountPaid = FloatField()
    effectivenessDate = DateTimeField(null=True)
    agreementNo = StringField()
    relatedActivities = IntField()
