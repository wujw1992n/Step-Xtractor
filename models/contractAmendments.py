from mongoengine import Document, StringField,BooleanField, IntField, FloatField



class ContractAmendments(Document):
    contractId = IntField()
    hasAmendments= BooleanField()
    amendmentNumber = IntField()
    substitutionStaff = BooleanField()
    variationOrder = BooleanField()
    other = BooleanField()
    modificationScopeServices = BooleanField()
    changeTimePerformance = BooleanField()
    contractDurationNoObjection = FloatField()
    timeUnit = StringField()
    durationRevised = FloatField()
    timeUnitDuration = StringField()
    changeTermsConditions = BooleanField()
    amendmentId = IntField(unique=True)

    changePriceAdjustmentsCPA = BooleanField()
    currentContractCurrencyCPA = StringField()
    currentContractAmountCPA = FloatField()
    contractAmendmentCurrencyCPA = StringField()
    contractAmendmentAmountCPA = FloatField()
    contractAmendmentExchangeRateCPA = FloatField()
    contractAmendmentAmountDollarCPA = FloatField()
    currencyContractPlusContractAmendmentCPA = StringField()
    currentContractAmountPlusContractAmendmentAmountCPA = FloatField()

    changeContractAmountCCA = BooleanField()
    currentContractCurrencyCCA = StringField()
    currentContractAmountCCA = FloatField()
    contractAmendmentCurrencyCCA = StringField()
    contractAmendmentAmountCCA = FloatField()
    contractAmendmentExchangeRateCCA = FloatField()
    contractAmendmentAmountDollarCCA = FloatField()
    currencyContractPlusContractAmendmentCCA = StringField()
    currentContractAmountPlusContractAmendmentAmountCCA = FloatField()


