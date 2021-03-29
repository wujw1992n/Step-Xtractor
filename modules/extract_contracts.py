import time

import pandas as pd
from controllers.DBController import DBController
from controllers.extractorController import ExtractorController
from controllers.manipulationController import ManipulationController

from pandas.core.frame import DataFrame
import math

from models.activity import Activities
from models.contracts import Contracts


class ExtractContracts(ManipulationController):
    def __init__(self, extractor: ExtractorController):
        self.extractor = extractor

    def filter_contract_list(self, data: dict) -> list:
        filtered_data = {key: data[key] for key in
                         data.keys() & {'activityId',
                                        "contractNo",
                                        "agencyId",
                                        "baseAmount",
                                        "baseCurrency",
                                        "baseExchangeRate",
                                        "baseAmountEquivalence",
                                        "contractDesc",
                                        "org",
                                        "ctrRefNo",
                                        "statusCode",
                                        "totalAmount",
                                        "totalCurrency",
                                        "totalAmountEquivalence",
                                        "projectid",
                                        "ctrSignDate",
                                        'duration',
                                        'ctrComDate'
                                        }}
        rename_fields = [{"contractNo": "contractId"},
                         {'contractDesc':'description'},
                         {'ctrRefNo':'referenceNo'},
                         {'statusCode':'status'},
                         {'projectid':'projectId'},
                         {'ctrSignDate':'signedDate'},
                         {'ctrComDate': 'completionDate'}]
        formated_data = self.rename_data_fields(filtered_data, rename_fields)
        return formated_data

    def format_contract_data(self, data: dict, agency: str):
        all_contracts = []
        currencyList = data["currencyList"]
        currencyTotalList = data["currencyTotalList"]
        suppliersList = data["supplierList"]
        for contract in data['contractList']:
            contract["agencyId"] = agency
            currency = self.first_element_that_matches_in_dict_list(source=currencyList,field="contractNo",matches=contract["contractNo"])
            currencyTotal = self.first_element_that_matches_in_dict_list(source=currencyTotalList, field="contractNo",
                                                                    matches=contract["contractNo"])

            suppliers = [element["supplierName"] for element in self.search_elements_that_matches_in_dict_list(source=suppliersList,field="contractNo",matches=contract["contractNo"])]
            contract["baseAmount"] = self.format_value_str_to_float(currency["amount"])
            contract["baseCurrency"] = currency["currencyDesc"]
            contract["baseExchangeRate"] = self.format_value_str_to_float(currency["exchangeRate"])
            contract["baseAmountEquivalence"] = self.format_value_str_to_float(currency["usdAmount"])
            contract["org"] = self.join_str_list(source=suppliers,separator=' | ')
            contract["totalAmount"] = self.format_value_str_to_float(currencyTotal["amount"])
            contract["totalCurrency"] = currencyTotal["currencyDesc"]
            contract["totalAmountEquivalence"] = currencyTotal["usdAmount"]
            contract['duration'] = str(contract["duration"]) + str(contract['durationType'])
            all_contracts.append(self.filter_contract_list(contract))
        return all_contracts,currencyList

    def get_contract_info(self, activity: int, project: str, agency: int, contract=None):
        try:
            url = "https://stepapi2.worldbank.org/secure/api/1.0/contract/" + str(project) + "/" + str(
                activity) + '?lang=EN&isArchive=N&projectId=' + str(project)
            print(url)
            data = self.extractor.get_data(url)
            if data != {}:
                data = data['data']
                formated_data,currencyList   = self.format_contract_data(data,agency)
                return formated_data,currencyList
            else:
                return {}, {}
        except Exception as error:
            print("|Error in contracts| {}".format(error))
            return {}, {}

    def extract_contract(self, df_raw_contracts_list: DataFrame):
        df_contract_list = pd.DataFrame()
        df_all_currency_list = pd.DataFrame()
        for data in df_raw_contracts_list.itertuples():
            contract_info, currencyList = self.get_contract_info(data.activityId, data.project, data.agency)
            if contract_info == {} or currencyList == {} :
                print("Erro com contratos {} | Projeto: {} | agencia: {} | URL: {}".format(data.activityId,
                                                                             data.project,
                                                                             data.agency,
                                                                             "https://stepapi2.worldbank.org/secure/api/1.0/contract/" + str(data.project) + "/" + str(
                data.activityId) + '?lang=EN&isArchive=N&projectId=' + str(data.project)))
                continue
            else:
                df_contract_list = df_contract_list.append(contract_info,ignore_index=True)
                df_all_currency_list = df_all_currency_list.append(currencyList,ignore_index=True)
        return df_contract_list, df_all_currency_list

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
            del data['Index']
            packet = [{'field': key, 'value': (None if type(data[key]) == type(float()) and math.isnan(data[key]) else data[key])} for key in data.keys()]
            dbController.updateOrSave(Contracts,{'contractId': int(data["contractId"]) }, packet)
        print("Contracts saved on database, time of execution ----%.9f----" % (time.time() - st))