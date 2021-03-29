

class ManipulationController:
    def rename_data_fields(self,data:list, fields:list):
        '''
        :param data:
        :param fields:
        :return:
        '''
        for field in fields:
            key, value = self.get_dict_items(field)
            if (str(key) in data.keys()):
                data[str(value)] = data.pop(str(key))
        return data

    def get_dict_items(self,field:list):
        for key, value in field.items():
            return key, value
    def format_value_str_to_float(self,value:str)-> float:
        return float(value.replace(',',''))
    def merge_dicts(self,dicts:list) -> dict:
        '''

        :param dicts: list
        :return: dict
        '''
        d = {}
        for dict in dicts:
            for key in dict:
                if (key not in d.keys()):
                    d[key] = dict[key]

    def first_element_that_matches_in_dict_list(self,source: list,field:str, matches:str)-> dict:
        return next(item for item in source if item[field] == matches)

    def search_elements_that_matches_in_dict_list(self,source: list,field:str, matches:str)-> dict:
        return [item for item in source if item[field] == matches]

    def join_str_list(self, source:list, separator:str)-> str:
        '''
        :param source:
        :param separator:
        :return:
        '''
        return ' - '.join(source) if(len(source) > 1) else source[0]

    def filter_dict_list(self, request_fields:dict, new_fields:list, data):
        '''
        :param request_fields: {'activityId',"contractNo"}
        :param new_fields: [{"contractNo": "contractId"},{'contractDesc':'description'}]
        :param data:
        :return:
        '''
        filtered_data = {key: data[key] for key in
                         data.keys() & request_fields}
        formated_data = self.rename_data_fields(filtered_data, new_fields)
        return formated_data