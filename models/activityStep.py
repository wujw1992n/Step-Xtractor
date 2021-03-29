from mongoengine import Document,StringField, ReferenceField, IntField, DateTimeField,BooleanField



class ActivitySteps(Document):
    stepActivityId = IntField(unique=True)
    activityId = IntField()
    actualDate = DateTimeField()
    checked = BooleanField() # Falta
    inProgress = BooleanField()
    originalDate = DateTimeField()
    originalDateDays = IntField(min_value=0)
    plannedYear = IntField(min_value=1900, max_value=3000)
    revisedDate = DateTimeField()
    revisedDays = IntField()
    runningDays = IntField()
    runningDate = DateTimeField()
    stepName = StringField()

