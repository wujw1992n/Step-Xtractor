import pandas as pd
from config.environment import  PROJECTS_LIST
from controllers.DBController import DBController
from controllers.extractorController import ExtractorController
from controllers.manipulationController import ManipulationController
from models.project import Projects
from pandas.core.frame import DataFrame
import math
from typing import List

class ExtractProjects(ManipulationController):

    def __init__(self, extractor: ExtractorController):
        self.extractor = extractor
        self.projects = PROJECTS_LIST

    def projects_filter(self,data: dict, proj_name: str) -> dict:

        if (str('project_abstract') in data[str(proj_name)].keys()):  # treatment for the case that there's no project_abstract field
            data[str(proj_name)]['project_abstract'] = data[str(proj_name)]['project_abstract']['cdata']

        if (str('totalcommamt') in data[str(proj_name)].keys()):
            data[str(proj_name)]['totalcommamt'] = '${:20,.2f}'.format(
                float(data[str(proj_name)]['totalcommamt'].replace(',', ''))).replace('$-', '-$').replace(' ','')  # PEP 378
        if (str('projectcost') in data[str(proj_name)].keys()):
            data[str(proj_name)]['projectcost'] = '${:20,.2f}'.format(
                float(data[str(proj_name)]['projectcost'].replace(',', ''))).replace('$-', '-$').replace(' ','')  # PEP 378
        return data

    def extract_project_info(self, data: dict, project: str) -> list:

        filtered_data = {key: data[project][key] for key in
                         data[project].keys() & {'closingdate', "id", 'project_abstract', 'boardapprovaldate',
                                                 'approvalfy', 'borrower', 'countryshortname_exact',
                                                 'envassesmentcategorycode', 'impagency', 'proj_last_upd_date',
                                                 'project_name', 'regionlongname_exact', 'status', 'teamleadname',
                                                 'totalcommamt', 'projectcost','sector'}}
        rename_fields = [{"closingdate": "closingDate"}, {"project_abstract": "abstract"}, {'id': "projectId"},
                         {'boardapprovaldate': "approvalDate"}, {'approvalfy': "approvalFy"},
                         {'borrower': 'borrower'},
                         {'countryshortname_exact': 'countryName'}, {'envassesmentcategorycode': 'environmentalCategory'},
                         {'impagency': 'implementingAgency'}, {'proj_last_upd_date': 'lastUpdateDate'},
                         {'project_name': 'projectName'}, {'regionlongname_exact': 'regionName'}, {'status': 'status'},
                         {'teamleadname': 'teamLeader'}, {'totalcommamt': 'commitmentAmount'},
                         {'projectcost': 'totalProjectCost'}, {'sector': 'sectors'}]
        formated_data = self.rename_data_fields(filtered_data, rename_fields)
        return formated_data

    def extract_projects(self) -> DataFrame:
        '''
        :description: return the projects data extracted from the World Bank
        :return: DataFrame
        '''
        df_all_projects = pd.DataFrame()
        for project in self.projects:
            print('https://search.worldbank.org/api/v2/projects?format=json&fl=*&id=' + project + '&apilang=en')
            data = self.extractor.get_data('https://search.worldbank.org/api/v2/projects?format=json&fl=*&id=' + project + '&apilang=en')["projects"]
            data = self.projects_filter(data, project)
            df_all_projects = df_all_projects.append(self.extract_project_info(data, project), ignore_index=True)
        return df_all_projects

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
            dbController.updateOrSave(Projects,{'projectId': data["projectId"] }, packet)
        print("Projects saved on database, time of execution ----%.9f----" % (time.time() - st))


