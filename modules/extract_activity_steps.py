import time

import pandas as pd
from controllers.DBController import DBController
from controllers.extractorController import ExtractorController
from controllers.manipulationController import ManipulationController

from pandas.core.frame import DataFrame
import math

from models.activity import Activities
from models.activityStep import ActivitySteps


class ExtractActivitySteps(ManipulationController):
    def __init__(self, extractor: ExtractorController):
        self.extractor = extractor

    def filter_step_data(self, data: dict) -> list:

        filtered_data = {key: data[key] for key in
                         data.keys() & {"actualDate","stepName","originalDate","orgDays","revisedDate","revisedDays","stepCode","runningDate","runningDays","actualDate","active"}}
        rename_fields = [{"stepCode":"stepActivityId"},{"orgDays":"originalDateDays"},{"active":"inProgress"}]
        formated_data = self.rename_data_fields(filtered_data, rename_fields)
        return formated_data

    def get_activities_steps(self,raw_steps: DataFrame) -> list:
        all_steps = []
        for step in raw_steps.steps:
            step_filtered = self.filter_step_data(step)
            step_filtered["activityId"] = raw_steps.activityId
            step_filtered["stepActivityId"] = int(str(step_filtered["stepActivityId"]) + str(raw_steps.activityId))
            all_steps.append(step_filtered)

        return all_steps

    def extract_steps_activities(self,df_raw_step_activity_data: DataFrame) -> DataFrame:
        df_step_activity_data = pd.DataFrame()
        for step_raw in df_raw_step_activity_data.itertuples():
            steps = self.get_activities_steps(step_raw)
            df_step_activity_data = df_step_activity_data.append(steps,ignore_index=True)

        return df_step_activity_data

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
            data["inProgress"] = bool(data["inProgress"])
            del data['Index']
            packet = [{'field': key, 'value': (None if type(data[key]) == type(float()) and math.isnan(data[key]) else data[key])} for key in data.keys()]
            dbController.updateOrSave(ActivitySteps,{'stepActivityId': data["stepActivityId"] },packet)
        print("Projects save on BD time of execution ----%.9f----" % (time.time() - st))