import time

import pandas as pd
from controllers.DBController import DBController
from controllers.extractorController import ExtractorController
from controllers.manipulationController import ManipulationController

from pandas.core.frame import DataFrame
import math

from models.contractAmendments import ContractAmendments
from models.contractsTermination import ContractsTerminations


class ExtractAmendments(ManipulationController):
    def __init__(self, extractor: ExtractorController):
        self.extractor = extractor

    def get_amendments_info(self, project_id: str, activity_id: int, contract_id: int, df_all_currency_list: DataFrame = None) -> DataFrame:
        try:
            url = "https://stepapi2.worldbank.org/secure/api/1.0/activity/" + str(project_id) + "/" + str(
                activity_id) + "/1615?lang=EN&ctrNo=" + str(
                contract_id) + "&contractAction=V&isArchive=N&projectId=" + str(project_id)
            data = self.extractor.get_data(url)
            if data != {}:
                data = data['data']
                amendment_data = data['activityStepAmendment']['amendments']
                duration_no_objection = (data['contractAddendumNoObjData']['duration'],
                                         data['contractAddendumNoObjData']['durationType'])
                base_amendment_dict = self.get_base_amendment_dict()

                if (len(amendment_data) > 0):
                    amendment_list = self.get_amendmend_codes_list(data["supplierMasterData"])
                    base_amendment_dict = self.set_base_amendment_dict(base_amendment_dict, amendment_data, amendment_list,
                                                                       df_all_currency_list, contract_id,
                                                                       duration_no_objection)

                else:
                    base_amendment_dict = {}

                formated_data = self.filter_dict_list(request_fields={"Contract_id",
                                                                      "Has Amendments",
                                                                      "Nº Amendment",
                                                                      "SOS",
                                                                      "VCO",
                                                                      "OTH",
                                                                      "MSS",
                                                                      "CTP",
                                                                      "Contract Duration at No Objection",
                                                                      "Time unit",
                                                                      "Duration Revised",
                                                                      "Time unit duration",
                                                                      "CTC",
                                                                      "CPA",
                                                                      "Current Contract Currency (CPA)",
                                                                      "Current Contract Amount (CPA)",
                                                                      "Contract Amendment Currency (CPA)",
                                                                      "Contract Amendment Amount (CPA)",
                                                                      "Contract Amendment Exchange Rate (CPA)",
                                                                      "Contract Amendment Amount in dollar (CPA)",
                                                                      "Currency: Current Contract Amount + Contract Amendment Amount (CPA)",
                                                                      "Current Contract Amount + Contract Amendment Amount (CPA)",
                                                                      "CCA",
                                                                      "Current Contract Currency (CCA)",
                                                                      "Current Contract Amount (CCA)",
                                                                      "Contract Amendment Currency (CCA)",
                                                                      "Contract Amendment Amount (CCA)",
                                                                      "Contract Amendment Exchange Rate (CCA)",
                                                                      "Contract Amendment Amount in dollar (CCA)",
                                                                      "Currency: Current Contract + Contract Amendment (CCA)",
                                                                      "Current Contract Amount + Contract Amendment Amount (CCA)",
                                                                      "Amendment_id"},
                                                      new_fields=[{'Contract_id': 'contractId'},
                                                                  {'Has Amendments': 'hasAmendments'},
                                                                  {'Nº Amendment': 'amendmentNumber'},
                                                                  {'SOS': 'substitutionStaff'},
                                                                  {'VCO': 'variationOrder'}, {'OTH': 'other'},
                                                                  {'MSS': 'modificationScopeServices'},
                                                                  {'CTP': 'changeTimePerformance'},
                                                                  {
                                                                      'Contract Duration at No Objection': 'contractDurationNoObjection'},
                                                                  {'Time unit': 'timeUnit'},
                                                                  {'Duration Revised': 'durationRevised'},
                                                                  {'Time unit duration': 'timeUnitDuration'},
                                                                  {'CTC': 'changeTermsConditions'},

                                                                  {'CPA': 'changePriceAdjustmentsCPA'},
                                                                  {
                                                                      'Current Contract Currency (CPA)': 'currentContractCurrencyCPA'},
                                                                  {
                                                                      'Current Contract Amount (CPA)': 'currentContractAmountCPA'},
                                                                  {
                                                                      'Contract Amendment Currency (CPA)': 'contractAmendmentCurrencyCPA'},
                                                                  {
                                                                      'Contract Amendment Amount (CPA)': 'contractAmendmentAmountCPA'},
                                                                  {
                                                                      'Contract Amendment Exchange Rate (CPA)': 'contractAmendmentExchangeRateCPA'},
                                                                  {
                                                                      'Contract Amendment Amount in dollar (CPA)': 'contractAmendmentAmountDollarCPA'},
                                                                  {
                                                                      'Currency: Current Contract Amount + Contract Amendment Amount (CPA)': 'currencyContractPlusContractAmendmentCPA'},
                                                                  {
                                                                      'Current Contract Amount + Contract Amendment Amount (CPA)': 'currentContractAmountPlusContractAmendmentAmountCPA'},

                                                                  {'CCA': 'changeContractAmountCCA'},
                                                                  {
                                                                      'Current Contract Currency (CCA)': 'currentContractCurrencyCCA'},
                                                                  {
                                                                      'Current Contract Amount (CCA)': 'currentContractAmountCCA'},
                                                                  {
                                                                      'Contract Amendment Currency (CCA)': 'contractAmendmentCurrencyCCA'},
                                                                  {
                                                                      'Contract Amendment Amount (CCA)': 'contractAmendmentAmountCCA'},
                                                                  {
                                                                      'Contract Amendment Exchange Rate (CCA)': 'contractAmendmentExchangeRateCCA'},
                                                                  {
                                                                      'Contract Amendment Amount in dollar (CCA)': 'contractAmendmentAmountDollarCCA'},
                                                                  {
                                                                      'Currency: Current Contract + Contract Amendment (CCA)': 'currencyContractPlusContractAmendmentCCA'},
                                                                  {
                                                                      'Current Contract Amount + Contract Amendment Amount (CCA)': 'currentContractAmountPlusContractAmendmentAmountCCA'},
                                                                  {'Amendment_id': 'amendmentId'}
                                                                  ], data=base_amendment_dict)
                return pd.DataFrame(formated_data)
            else:
                return {}
        except Exception as error:
            print("|Error in Amendments| {}".format(error))
            return {}

    def set_base_amendment_dict(self,base_amendment_dict,amendment_data,amendment_list,df_all_currency_list:DataFrame,contract_id,duration_no_objection):
        for amendment in amendment_data.values():
            base_amendment_dict, amend_list_added, has_amendment = self.set_contract_type_list_codes(amendment['contractAddTypeList'],base_amendment_dict)
            base_amendment_dict['Contract_id'].append(contract_id)
            base_amendment_dict['Contract Duration at No Objection'].append(duration_no_objection[0])
            base_amendment_dict['Time unit'].append(duration_no_objection[1])
            amendment_current_amount_info = amendment['ctrCurrList'][0] if (len(amendment['ctrCurrList']) > 0) else {
                'currencyAmount': 0, 'waers': ''}
            amendment_amount_info = amendment['ctrCurrAddList'][0] if (len(amendment['ctrCurrAddList']) > 0) else {
                'currencyAmount': 0, 'waers': ''}
            amendment_amendment_final_info = amendment['ctrCurrentTotal'][0] if (
                        len(amendment['ctrCurrentTotal']) > 0) else {'currencyAmount': 0, 'waers': ''}

            for amend in amendment_list:#["SOS","VCO","OTH","MSS","CTP","CTC","CPA","CCA"]: #amendment_list
                if (amend not in amend_list_added):
                    base_amendment_dict[amend].append(False)

            change_list = amendment['ctrAddAddendumList']
            if ('CTP' in amend_list_added):
                base_amendment_dict["Duration Revised"].append(change_list[0]['duration'])
                base_amendment_dict["Time unit duration"].append(change_list[0]['durationType'])
            else:
                base_amendment_dict["Duration Revised"].append('0')
                base_amendment_dict["Time unit duration"].append('')

            if ('CCA' in amend_list_added):
                df_all_currency_list = df_all_currency_list[df_all_currency_list["addNo"]==str(amendment["amendmentNumber"])]
                df_all_currency_list = df_all_currency_list[df_all_currency_list["contractNo"]==str(contract_id)]
                if (len(df_all_currency_list) > 0):
                    amendment_ex_rate = [ex for ex in df_all_currency_list.itertuples()][0]._asdict()
                else:
                    amendment_ex_rate = {'exchangeRate': 0, 'usdAmount': 0}
                base_amendment_dict["Current Contract Amount (CCA)"].append(
                    amendment_current_amount_info['currencyAmount'])
                base_amendment_dict["Current Contract Currency (CCA)"].append(amendment_current_amount_info['waers'])
                base_amendment_dict["Contract Amendment Amount (CCA)"].append(amendment_amount_info['currencyAmount'])
                base_amendment_dict["Contract Amendment Currency (CCA)"].append(amendment_amount_info['waers'])
                base_amendment_dict["Contract Amendment Exchange Rate (CCA)"].append(amendment_ex_rate['exchangeRate'])
                base_amendment_dict["Contract Amendment Amount in dollar (CCA)"].append(amendment_ex_rate['usdAmount'])
                base_amendment_dict["Currency: Current Contract + Contract Amendment (CCA)"].append(
                    amendment_amendment_final_info['waers'])
                base_amendment_dict["Current Contract Amount + Contract Amendment Amount (CCA)"].append(
                    amendment_amendment_final_info['currencyAmount'])
            else:
                base_amendment_dict["Current Contract Amount (CCA)"].append(0)
                base_amendment_dict["Current Contract Currency (CCA)"].append('')
                base_amendment_dict["Contract Amendment Amount (CCA)"].append(0)
                base_amendment_dict["Contract Amendment Currency (CCA)"].append('')
                base_amendment_dict["Contract Amendment Exchange Rate (CCA)"].append(0)
                base_amendment_dict["Contract Amendment Amount in dollar (CCA)"].append(0)
                base_amendment_dict["Currency: Current Contract + Contract Amendment (CCA)"].append('')
                base_amendment_dict["Current Contract Amount + Contract Amendment Amount (CCA)"].append(0)

            if ('CPA' in amend_list_added):
                df_all_currency_list = df_all_currency_list[
                    df_all_currency_list["addNo"] == str(amendment["amendmentNumber"])]
                if (len(df_all_currency_list) > 0):
                    amendment_ex_rate = [ex for ex in df_all_currency_list.itertuples()][0]._asdict()
                else:
                    amendment_ex_rate = {'exchangeRate': 0, 'usdAmount': 0}
                base_amendment_dict["Current Contract Amount (CPA)"].append(
                    amendment_current_amount_info['currencyAmount'])
                base_amendment_dict["Current Contract Currency (CPA)"].append(amendment_current_amount_info['waers'])
                base_amendment_dict["Contract Amendment Amount (CPA)"].append(amendment_amount_info['currencyAmount'])
                base_amendment_dict["Contract Amendment Currency (CPA)"].append(amendment_amount_info['waers'])
                base_amendment_dict["Contract Amendment Exchange Rate (CPA)"].append(amendment_ex_rate['exchangeRate'])
                base_amendment_dict["Contract Amendment Amount in dollar (CPA)"].append(amendment_ex_rate['usdAmount'])
                base_amendment_dict["Current Contract Amount + Contract Amendment Amount (CPA)"].append(
                    amendment_amendment_final_info['currencyAmount'])
                base_amendment_dict["Currency: Current Contract Amount + Contract Amendment Amount (CPA)"].append(
                    amendment_amendment_final_info['waers'])
            else:
                base_amendment_dict["Current Contract Amount (CPA)"].append(0)
                base_amendment_dict["Current Contract Currency (CPA)"].append('')
                base_amendment_dict["Contract Amendment Amount (CPA)"].append(0)
                base_amendment_dict["Contract Amendment Currency (CPA)"].append('')
                base_amendment_dict["Contract Amendment Exchange Rate (CPA)"].append(0)
                base_amendment_dict["Contract Amendment Amount in dollar (CPA)"].append(0)
                base_amendment_dict["Currency: Current Contract Amount + Contract Amendment Amount (CPA)"].append('')
                base_amendment_dict["Current Contract Amount + Contract Amendment Amount (CPA)"].append(0)
            base_amendment_dict['Has Amendments'].append(has_amendment)
            base_amendment_dict['Nº Amendment'].append(str(amendment["amendmentNumber"]))
            base_amendment_dict['Amendment_id'].append(str(contract_id) + str(amendment["amendmentNumber"]))
            amend_list_added = []
        return base_amendment_dict

    def get_amendmend_codes_list(self, supplierMasterData):
        return pd.DataFrame(supplierMasterData).addType.values

    def set_contract_type_list_codes(self,contractAddTypeList: list, base_amendment_dict: dict ):
        amend_list_added = []
        has_amendment = False
        for element in contractAddTypeList:  # for any amendment reason checkbox checked
            if (element['code'] in base_amendment_dict.keys()):
                base_amendment_dict[element['code']].append(True)
                amend_list_added.append(element['code'])
                has_amendment = True
        return base_amendment_dict, amend_list_added, has_amendment

    def get_blank_amendment_dict(self, contract_id: int, duration_no_obj:tuple) -> dict:
        return {"Contract_id":[contract_id],
                             "Has Amendments":[False],
                             "Nº Amendment":[0],
                             "SOS":[False],
                             "VCO":[False],
                             "OTH":[False],
                             "MSS":[False],
                             "CTP":[False],
                             "Contract Duration at No Objection":[duration_no_obj[0]],
                             "Time unit":[duration_no_obj[1]],
                             "Duration Revised":['0'],
                             "Time unit duration":[''],
                             "CTC":[False],
                             "CPA":[False],
                             "Current Contract Currency (CPA)":[''],
                             "Current Contract Amount (CPA)":['0'],
                             "Contract Amendment Currency (CPA)":[''],
                             "Contract Amendment Amount (CPA)":['0'],
                             "Contract Amendment Exchange Rate (CPA)":['0'],
                             "Contract Amendment Amount in dollar (CPA)":['0'],
                             "Currency: Current Contract Amount + Contract Amendment Amount (CPA)":[''],
                             "Current Contract Amount + Contract Amendment Amount (CPA)":['0'],
                             "CCA":[False],
                             "Current Contract Currency (CCA)":[''],
                             "Current Contract Amount (CCA)":['0'],
                             "Contract Amendment Currency (CCA)":[''],
                             "Contract Amendment Amount (CCA)":['0'],
                             "Contract Amendment Exchange Rate (CCA)":['0'],
                             "Contract Amendment Amount in dollar (CCA)":['0'],
                             "Currency: Current Contract + Contract Amendment (CCA)":[''],
                             "Current Contract Amount + Contract Amendment Amount (CCA)":['0'],
                             "Amendment_id":[str(contract_id)+"0"]}

    def get_base_amendment_dict(self) -> dict:
        return {"Contract_id":[],
                             "Has Amendments":[],
                             "Nº Amendment":[],
                             "SOS":[],
                             "VCO":[],
                             "OTH":[],
                             "MSS":[],
                             "CTP":[],
                             "Contract Duration at No Objection":[],
                             "Time unit":[],
                             "Duration Revised":[],
                             "Time unit duration":[],
                             "CTC":[],
                             "CPA":[],
                             "Current Contract Currency (CPA)":[],
                             "Current Contract Amount (CPA)":[],
                             "Contract Amendment Currency (CPA)":[],
                             "Contract Amendment Amount (CPA)":[],
                             "Contract Amendment Exchange Rate (CPA)":[],
                             "Contract Amendment Amount in dollar (CPA)":[],
                             "Currency: Current Contract Amount + Contract Amendment Amount (CPA)":[],
                             "Current Contract Amount + Contract Amendment Amount (CPA)":[],
                             "CCA":[],
                             "Current Contract Currency (CCA)":[],
                             "Current Contract Amount (CCA)":[],
                             "Contract Amendment Currency (CCA)":[],
                             "Contract Amendment Amount (CCA)":[],
                             "Contract Amendment Exchange Rate (CCA)":[],
                             "Contract Amendment Amount in dollar (CCA)":[],
                             "Currency: Current Contract + Contract Amendment (CCA)":[],
                             "Current Contract Amount + Contract Amendment Amount (CCA)":[],
                             "Amendment_id":[]}

    def extract_amendments(self,df_all_contracts: DataFrame, df_all_currency_list: DataFrame) -> DataFrame:
        df_contract_termination_list = pd.DataFrame()
        for contract_data in df_all_contracts.itertuples():
            amendment = self.get_amendments_info(contract_data.projectId,contract_data.activityId,contract_data.contractId, df_all_currency_list)
            if type(amendment) == type({}) and amendment == {}:
                continue
            else:
                df_contract_termination_list = df_contract_termination_list.append(amendment,ignore_index=True )
        return df_contract_termination_list

    def save_on_database(self, formatedData: DataFrame, dbController: DBController) -> None:
        '''
        :param formatedData: Dataframe with the extracted data
        :param dbController: DBController
        :return: None
        '''
        import time
        st = time.time()
        for data in formatedData.itertuples():
            data = data._asdict()
            data["contractId"] = int(data["contractId"])
            data["amendmentId"] = int(data["amendmentId"])
            data["amendmentNumber"] = int(data["amendmentNumber"])
            del data['Index']
            packet = [{'field': key, 'value': (None if type(data[key]) == type(float()) and math.isnan(data[key]) else data[key])} for key in data.keys()]
            dbController.updateOrSave(ContractAmendments,{'amendmentId': int(data["amendmentId"]) }, packet)
        print("Amendments saved on database, time of execution ----%.9f----" % (time.time() - st))