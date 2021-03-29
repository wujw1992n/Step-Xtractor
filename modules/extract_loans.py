import pandas as pd
from controllers.DBController import DBController
from controllers.extractorController import ExtractorController
from controllers.manipulationController import ManipulationController
from models.loan import Loans
from pandas.core.frame import DataFrame
import math

class ExtractLoans(ManipulationController):
    def __init__(self, extractor:ExtractorController, projects: list = []):
        self.extractor = extractor
        self.projects = projects

    def filter_loan_data(self,data):
        filtered_data = {key: data[key] for key in
                         data.keys() & {'Project_id', 'finLoan', 'loanUsdAmt', 'disbUsdAmt', 'loanPaidAmt',
                                        'loanApprvDate', 'loanReffDate', 'loanRclsDate', 'activitiesCount'}}
        rename_fields = [{"Project_id":"projectId"},{"finLoan": "agreementNo"}, {'loanUsdAmt': 'amount'}, {'disbUsdAmt': 'disbursedAmountPaid'},
                         {'loanPaidAmt': 'contractAmountPaid'}, {'loanApprvDate': 'approvalDate'},
                         {'loanReffDate': "effectivenessDate"}, {'loanRclsDate': 'closingDate'},
                         {'activitiesCount': 'relatedActivities'}]
        formated_data = self.rename_data_fields(filtered_data, rename_fields)
        return formated_data

    def format_loan_data(self,data, project):
        df_all_loans = []
        for loan in data:
            loan['Project_id'] = project
            df_all_loans.append(self.filter_loan_data(loan))
        return df_all_loans

    def get_loan_projects(self, project):
        url = 'https://stepapi2.worldbank.org/secure/api/1.0/projectLoan?lang=EN&projectId=' + str(project)
        try:
            formated_data = self.format_loan_data(self.extractor.get_data(url=url)['data'], project)
            return formated_data
        except Exception as erro:
            print("|Error in Loans extraction| {}".format(erro))
            return {}
    def extract_all_projects_loans(self):
        df_all_projects_loans = pd.DataFrame()
        for project in self.projects:
            project_loan = self.get_loan_projects(project)
            if project_loan == {}:
                continue
            else:
                df_all_projects_loans = df_all_projects_loans.append(project_loan, ignore_index=True)
                df_all_projects_loans = df_all_projects_loans[df_all_projects_loans['agreementNo'] != 'Total']
        return df_all_projects_loans

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
            data["contractAmountPaid"] = self.format_value_str_to_float(data["contractAmountPaid"])
            data["disbursedAmountPaid"] = self.format_value_str_to_float(data["disbursedAmountPaid"])
            data['effectivenessDate'] = None if data['effectivenessDate'] == '' else data['effectivenessDate']
            data['approvalDate'] = None if data['approvalDate'] == '' else data['approvalDate']
            data['closingDate'] = None if data['closingDate'] == '' else data['closingDate']
            packet = [{'field': key, 'value': (None if type(data[key]) == type(float()) and math.isnan(data[key]) else data[key])} for key in data.keys()]
            dbController.updateOrSave(Loans,{'agreementNo': data["agreementNo"] }, packet)
        print("Loans saved on database, time of execution ----%.9f----" % (time.time() - st))