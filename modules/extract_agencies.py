import pandas as pd
from controllers.DBController import DBController
from controllers.extractorController import ExtractorController
from controllers.manipulationController import ManipulationController
from models.agency import Agencies
from pandas.core.frame import DataFrame
import math

class ExtractAgencies(ManipulationController):
    def __init__(self, extractor: ExtractorController, projects: list = []):
        self.extractor = extractor
        self.projects = projects

    def extract_activities_list(self, project, df_activities_list):
        activities_list_formated = []
        for activity in df_activities_list.itertuples():
            activities_list_formated.append(
                {"agency": activity.activityAgencyCode, "activityId": activity.activityId, "project": project,"reviewType":activity.reviewType})
        return activities_list_formated

    def filter_agency_data(self, data):
        filtered_data = {key: data[key] for key in
                         data.keys() & {'Project_id', "Link", "agencyName", "agencyCode", "planCurrentStatus"}}
        rename_fields = [{"planCurrentStatus": "status"}, {'agencyName': 'name'}, {"agencyCode": "agencyID"},{"Link": "link"}, {"Project_id":"projectId"}]
        formated_data = self.rename_data_fields(filtered_data, rename_fields)
        return formated_data

    def format_agency_data(self, data: dict, project: str, url: str) -> list:
        df_all_agency = []
        for agency_plan_summary in data["planSummary"]["agenciesList"]:
            agency_plan_summary['Project_id'] = project
            agency_plan_summary['Link'] = url
            df_all_agency.append(self.filter_agency_data(agency_plan_summary))
        return df_all_agency

    def get_agency_projects(self, project: str):
        url = "https://stepapi2.worldbank.org/secure/api/1.0/getPlanDetails?projectId=" + str(project) + "&lang=EN"
        original_data = self.extractor.get_data(url=url)["data"]
        formated_data = self.format_agency_data(original_data, project, url)
        agency_activities_list = self.extract_activities_list(project, pd.DataFrame(original_data["planSummary"]["activitiesList"]))  # get activities list from each agency
        return formated_data, agency_activities_list

    def extract_agencies(self):
        '''
        :description: extract all loans for the current project list
        :return: DataFrame
        '''
        df_all_agencies = pd.DataFrame()
        df_all_proj_agency_activities = pd.DataFrame()
        for project in self.projects:
            project_agency, agency_activities = self.get_agency_projects(project)
            df_all_proj_agency_activities = df_all_proj_agency_activities.append(agency_activities, ignore_index=True)
            df_all_agencies = df_all_agencies.append(project_agency, ignore_index=True)
        return df_all_agencies, df_all_proj_agency_activities

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
            dbController.updateOrSave(Agencies,{'agencyID': data["agencyID"] }, packet)
        print("Projects save on BD time of execution ----%.9f----" % (time.time() - st))