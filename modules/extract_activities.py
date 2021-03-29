import time
import pandas as pd
from controllers.DBController import DBController
from controllers.extractorController import ExtractorController
from controllers.manipulationController import ManipulationController

from pandas.core.frame import DataFrame
import math

from models.activity import Activities


class ExtractActivities(ManipulationController):
    def __init__(self, extractor: ExtractorController, projects: list = []):
        self.extractor = extractor
        self.projects = projects

    def filter_activity_data(self, data):
        filtered_data = {key: data[key] for key in
                         data.keys() & {'projectId', "Link", "consPayCode", "referenceNo", "activityId",
                                        "activityStatus", "processStatus", "description", "procGroupCodeDesc",
                                        "twoStagFlag", "procMethodCodeDesc", "marketApproch", "estimatedBudgetAmount",
                                        "retroactiveFinancing", "bankFinPercentage","activityAgencyCode","reviewType"}}
        rename_fields = [{"consPayCode": "contractType"}, {"referenceNo": "referenceNo"},
                         {"activityId": "activityId"}, {"activityStatus": "status"},
                         {"processStatus": "processStatus"}, {"description": "description"},
                         {"procGroupCodeDesc": "procurementCategory"}, {"twoStagFlag": "procurementProcess"},
                         {"procMethodCodeDesc": "procurementMethod"}, {"marketApproch": "marketApproach"},
                         {"estimatedBudgetAmount": "estimatedAmount"},
                         {"retroactiveFinancing": "retroactiveFinancing"},
                         {"bankFinPercentage": "bankFinanced"}, {"activityAgencyCode": "agencyId"},{"Project_id":"projectId"}]
        formated_data = self.rename_data_fields(filtered_data, rename_fields)
        return formated_data

    def specific_filter(self,data: dict):
        if (str('retroactiveFinancing') in data.keys()):
            if(data["retroactiveFinancing"] == "X"):
                data["retroactiveFinancing"] = True
            else:
                data["retroactiveFinancing"] = False

        if (str('procurementProcess') in data.keys()):
            if(data["procurementProcess"] =="S"):
                data["procurementProcess"] = "Single Stage - One Envelope"
            elif(data["procurementProcess"] == "T"):
                data["procurementProcess"] = "Single Stage - Two Envelope"
            elif(data["procurementProcess"] == 'X'):
                data["procurementProcess"] = "Prequalification"
            elif (data["procurementProcess"] == 'I'):
                data["procurementProcess"] = "Initial Selection"
            elif (data["procurementProcess"] == 'M'):
                data["procurementProcess"] = "Multi Stage - One Envelope"
            elif (data["procurementProcess"] == 'Q'):
                data["procurementProcess"] = "Multi Stage - Two Envelope"

        if (str('marketApproach') in data.keys()):
            if(data["marketApproach"] == "O"):
                data["marketApproach"] = "Open"
            elif(data["marketApproach"] == "L"):
                data["marketApproach"] = "Limited"
            elif (data["marketApproach"] == "A"):
                data["marketApproach"] = 'Direct'
        if (str('marketApproach') in data.keys() and str('marketApprochReg') in data.keys()):
            if (data["marketApprochReg"] == "N"):
                data["marketApproach"] = str(data["marketApproach"]) + " | " + ''
            elif (data["marketApprochReg"] == "I"):
                data["marketApproach"] = str(data["marketApproach"]) + " | " + ''

        if (str('contractType') in data.keys()):
            if(data['contractType'] == 'LS'):
                data['contractType'] = 'Lump sum'
            elif(data['contractType'] == 'TB'):
                data['contractType'] = 'Time Based'
            elif(data['contractType'] == 'BH'):
                data['contractType'] = 'Lump sum' + " - " + 'Time Based'
        return data

    def get_agency_activities(self,activity: pd.DataFrame) :
        try:
            all_activity_data = None
            start_time1 = time.time()
            url = "https://stepapi2.worldbank.org/secure/api/1.0/activity/{}?isArchive=N&lang=EN&projectId={}".format(
                activity.activityId, activity.project)
            data = self.extractor.get_data(url=url)
            if data != {}:
                data = data["data"]
                all_activity_data = data["activityDetail"]
                raw_step_data = {"steps": data["procurementRoadMapList"], "activityId": activity.activityId,
                                 "project": activity.project}
                raw_contracts_list = [{"contractId": contract["ctrNumber"], "activityId": activity.activityId,
                                       "project": activity.project, "agency": activity.agency} for contract in
                                      data["contractList"]]
                print("---Execution time for the task: {} seconds, activity: {} --- Project: {}".format(
                    time.time() - start_time1, activity.activityId, activity.project))
                all_activity_data["reviewType"] = str(activity.reviewType) + " Review"
                all_activity_data = self.filter_activity_data(all_activity_data)
                return self.specific_filter(all_activity_data), raw_step_data, raw_contracts_list
            return {}, {}, {}
        except Exception as erro:
            print("|Error in Activities| {}".format(erro))
            return {}, {}, {}

    def extract_activities_agencies(self,activities_list):
        df_activities = pd.DataFrame()
        df_raw_step_activity_data = pd.DataFrame()
        df_raw_contracts_list = pd.DataFrame()
        for activity_raw in activities_list[activities_list['project'].isin(self.projects)].itertuples():
            activity, steps, contracts = self.get_agency_activities(activity_raw)
            if activity == {} or steps == {} or contracts =={}:
                print("Error in activity {} | project: {} | URL: {}".format(activity_raw.activityId,activity_raw.project,  "https://stepapi2.worldbank.org/secure/api/1.0/activity/{}?isArchive=N&lang=EN&projectId={}".format(activity_raw.activityId,activity_raw.project)) )
                continue
            else:
                df_activities = df_activities.append(activity, ignore_index=True)
                df_raw_step_activity_data = df_raw_step_activity_data.append(steps,ignore_index=True)
                df_raw_contracts_list = df_raw_contracts_list.append(contracts, ignore_index=True)

        print("Finished: ",df_raw_contracts_list)
        return df_activities, df_raw_step_activity_data, df_raw_contracts_list.drop_duplicates('contractId').sort_index()

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
            data["retroactiveFinancing"] = bool(data["retroactiveFinancing"])
            del data['Index']
            packet = [{'field': key, 'value': (None if type(data[key]) == type(float()) and math.isnan(data[key]) else data[key])} for key in data.keys()]
            dbController.updateOrSave(Activities,{'activityId': int(data["activityId"]),'projectId': data["projectId"]}, packet)
        print("Activities saved on database, time of execution ----%.9f----" % (time.time() - st))

