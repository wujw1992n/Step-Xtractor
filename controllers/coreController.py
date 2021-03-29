import time

from config.environment import database_url,database_name
import sys
from pandas.core.frame import DataFrame
from controllers.DBController import DBController
from controllers.extractorController import ExtractorController
from modules.extract_amendments import ExtractAmendments
from modules.extract_contracts import ExtractContracts
from modules.extract_activities import ExtractActivities
from modules.extract_activity_steps import ExtractActivitySteps
from modules.extract_agencies import ExtractAgencies
from modules.extract_contracts_termination import ExtractContractsTermination
from modules.extract_loans import ExtractLoans
from modules.extract_projects import ExtractProjects


class Core:
    def __init__(self):
        self.data_base_controller = DBController()
        self.extraction_controller = ExtractorController()

    def make_database_connection(self)->None:
        '''
        :description: This method start a connection with the database
        :return: None
        '''
        try:
            conn = self.data_base_controller.create_connection(database_name, database_url)
        except Exception as error:
            print(error)
            sys.exit(1)

    def initialize_extraction(self):
        '''
        :description: This method initialize the extraction by calling each respective phase
        :return: None
        '''
        self.make_database_connection()
        df_raw_contracts_list = self.extraction_first_phase()
        self.extraction_second_phase(df_raw_contracts_list)

    def extraction_second_phase(self,df_raw_contracts_list:DataFrame ):
        '''
        :description: This method makes the data extraction for Contracts, Contracts Termination and Amendments
        :param df_raw_contracts_list: Dataframe object containing the list of contracts
        :return: None
        '''
        print("Start contracts extraction....")
        st = time.time()
        contractsExtractor = ExtractContracts(extractor=self.extraction_controller)
        df_all_contracts, df_all_currency_list = contractsExtractor.extract_contract(df_raw_contracts_list)
        contractsExtractor.save_on_database(df_all_contracts,self.data_base_controller)
        print("Finished Contracts extraction, time of execution ----%.9f----" % (time.time() - st))

        print("Start contracts termination extraction....")
        st = time.time()
        contractsTerminationExtractor = ExtractContractsTermination(extractor=self.extraction_controller)
        df_all_contract_termination = contractsTerminationExtractor.extract_contract_termination(df_all_contracts)
        print(df_all_contract_termination.head(2))
        contractsTerminationExtractor.save_on_database(df_all_contract_termination,self.data_base_controller)
        print("Finished Contracts Termination extraction, time of execution ----%.9f----" % (time.time() - st))

        print("Start Amendment extraction....")
        st = time.time()
        amendmentExtractor = ExtractAmendments(extractor=self.extraction_controller)
        df_contracts_amendments = amendmentExtractor.extract_amendments(df_all_contracts,df_all_currency_list)
        amendmentExtractor.save_on_database(df_contracts_amendments,self.data_base_controller)
        print(df_contracts_amendments.head(5))
        print("Finished Amendments extraction, time of execution ----%.9f----" % (time.time() - st))

    def extraction_first_phase(self)-> DataFrame:
        '''
        :description: This method makes the data extraction for Projects, Loans, Agencies, Activities and Step Activities
        :return: DataFrame
        '''
        try:
            print("Start projects extraction....")
            st = time.time()
            projectsExtractor = ExtractProjects(extractor=self.extraction_controller)
            df_all_projects = projectsExtractor.extract_projects()
            projectsExtractor.save_on_database(df_all_projects,self.data_base_controller)
            print("Finished Projects extraction, time of execution ----%.9f----"% (time.time() - st))

            print("Start loans extraction....")
            st = time.time()
            loansExtractor = ExtractLoans(extractor=self.extraction_controller, projects= projectsExtractor.projects)
            df_all_loans = loansExtractor.extract_all_projects_loans()
            loansExtractor.save_on_database(df_all_loans,self.data_base_controller)
            print("Finished Loans extraction, time of execution ----%.9f----" % (time.time() - st))

            print("Start agencies extraction....")
            st = time.time()
            agencyExtractor = ExtractAgencies(extractor=self.extraction_controller,projects= projectsExtractor.projects )
            df_all_agencies, df_all_proj_agency_activities = agencyExtractor.extract_agencies()
            agencyExtractor.save_on_database(df_all_agencies,self.data_base_controller)
            print("Finished Agencies extraction, time of execution ----%.9f----" % (time.time() - st))

            print("Start activities extraction....")
            st = time.time()
            activityAgency = ExtractActivities(extractor=self.extraction_controller, projects= projectsExtractor.projects)
            df_all_activities, df_raw_step_activity_data, df_raw_contracts_list = activityAgency.extract_activities_agencies(df_all_proj_agency_activities)
            activityAgency.save_on_database(df_all_activities,self.data_base_controller)
            print("Finished Activities extraction, time of execution ----%.9f----" % (time.time() - st))

            print("Start step activities extraction....")
            st = time.time()
            stepExtractor = ExtractActivitySteps(extractor=self.extraction_controller)
            df_step_activity_data = stepExtractor.extract_steps_activities(df_raw_step_activity_data)
            stepExtractor.save_on_database(df_step_activity_data, self.data_base_controller)
            print("Finished Step Activities extraction, time of execution ----%.9f----" % (time.time() - st))

            return df_raw_contracts_list.drop_duplicates('activityId').sort_index()
        except Exception as error:
            print("Error: ", error)


