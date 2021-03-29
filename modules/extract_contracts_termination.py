import time

import pandas as pd
from controllers.DBController import DBController
from controllers.extractorController import ExtractorController
from controllers.manipulationController import ManipulationController

from pandas.core.frame import DataFrame
import math
from models.contractsTermination import ContractsTerminations


class ExtractContractsTermination(ManipulationController):
    def __init__(self, extractor: ExtractorController):
        self.extractor = extractor

    def get_contract_termination_info(self,project_id: str ,activity_id: int, contract_id: int) -> dict:
        try:
            url = "https://stepapi2.worldbank.org/secure/api/1.0/activity/" + str(project_id) + "/" + str(
                activity_id) + "/1700?lang=EN&ctrNo=" + str(
                contract_id)
            data = self.extractor.get_data(url)
            if data != {}:
                data = data['data']
                contractTerminationReasons = data['contractTerminationReasons']
                isTerminated = data['displayTerminationDataSheet']
                base_termination_dict = {"Contract_id": str(contract_id),
                                         "CCPE": False,
                                         "CCSC": False,
                                         "DATA": False,
                                         "DBPCC": False,
                                         "DCPE": False,
                                         "MPROC": False,
                                         "OTH": False,
                                         "reason": "",
                                         'terminationDate': data['activityStepData']['activityDetail']['actualDate'] if (
                                             isTerminated) else "",
                                         'Is Terminated?': isTerminated,
                                         'contractTerminationReasons': contractTerminationReasons}
                for element in data['activityStepData'][
                    'contractAddTypeList']:  # for any termination reason checkbox checked
                    if (element['code'] in base_termination_dict.keys()):
                        base_termination_dict[element['code']] = True
                        # print(element['code'])
                        if (element['code'] == 'OTH'):
                            base_termination_dict['reason'] = element['reason']
                # print(base_termination_dict)
                formated_data = self.filter_dict_list(
                    request_fields={"Contract_id", "CCPE", "CCSC", "DATA", "DBPCC", "DCPE", "MPROC", "OTH", "reason",
                                    'terminationDate', 'Is Terminated?'},
                    new_fields=[{'Contract_id': 'contractId'}, {'CCPE': 'ccpe'},
                                {'CCSC': 'ccsc'}, {'DATA': 'data'},
                                {'DBPCC': 'dbpcc'},
                                {'DCPE': 'dcpe'},
                                {'MPROC': 'mproc'}, {'OTH': 'oth'},
                                {'reason': 'reason'}, {'terminationDate': 'terminationDate'},
                                {'Is Terminated?': 'isTerminated'}],
                    data=base_termination_dict)
                # print(formated_data)
                return formated_data
            else:
                return {}
        except Exception as erro:
            print("|Erro Termino de Contrato| {}".format(erro))
            print("Data: {}".format(data))
            return {}

    def extract_contract_termination(self,df_all_contracts: DataFrame) -> DataFrame:
        df_contract_termination_list = pd.DataFrame()
        for contract_data in df_all_contracts.itertuples():
            contract_termination = self.get_contract_termination_info(contract_data.projectId,contract_data.activityId,contract_data.contractId)
            if contract_termination == {}:
                pass
            else:
                df_contract_termination_list = df_contract_termination_list.append(contract_termination,
                                                                                   ignore_index=True)
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

            data["ccpe"] = bool(data["ccpe"])
            data["ccsc"] = bool(data["ccsc"])
            data["data"] = bool(data["data"])
            data["dbpcc"] = bool(data["dbpcc"])
            data["dcpe"] = bool(data["dcpe"])
            data["mproc"] = bool(data["mproc"])
            data["oth"] = bool(data["oth"])
            data["isTerminated"] = bool(data["isTerminated"])
            data["contractId"] = int(data["contractId"])
            data['terminationDate'] = None if data['terminationDate'] == '' else data['terminationDate']
            del data['Index']
            packet = [{'field': key, 'value': (None if type(data[key]) == type(float()) and math.isnan(data[key]) else data[key])} for key in data.keys()]
            dbController.updateOrSave(ContractsTerminations,{'contractId': int(data["contractId"]) }, packet)
        print("Contracts termination saved on database, time of execution ----%.9f----" % (time.time() - st))