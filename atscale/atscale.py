import logging
import os

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
import re
import json
import uuid
import getpass
from datetime import datetime, timedelta
from db.database import Database
from utils import Aggs
from errors import UserError

agg = Aggs() #used for faster aggregation entry for create_aggregate_feature

class AtScale:
    """Acts as an interface to a cube on the server.

    :var str `~AtScale.server`: The server to connect to.
    :var str `~AtScale.organization`: The name of the organization.
    :var str `~AtScale.project_id`: The id of the project.
    :var str `~AtScale.model_id`: The id of the model.
    :var str `~AtScale.token`: The token used for authentication
    :var str `~AtScale.username`: The AtScale username to log in with.
    :var str `~AtScale.password`: The password for the user. Defaults to 'None' to enter via prompt.
    :var str `~AtScale.design_center_server_port`: The port the design center is listening on. Defaults to '10500'.
    :var str `~AtScale.engine_port`: The port the engine is listening on. Defaults to '10502.
    """

    __version__ = '0.3.1'

    def __init__(self, server, organization, project_id, model_id, token=None,
                 username=None, password=None, design_center_server_port='10500', engine_port='10502'):

        self.server = server
        self.design_center_server_port = design_center_server_port
        self.engine_port = engine_port
        self.organization = organization
        self.project_id = project_id
        self.model_id = model_id
        self.project_name = None
        self.model_name = None
        self.project_json = None
        self.token = token
        self.username = username
        if self.token is None: #only prompt password and use temp token if token not given
            if self.username is None:
                raise UserError('You must pass in a token or alternatively a username to log in with')
            elif password is None:
                self.password = getpass.getpass(prompt='Password: ')
            else:
                self.password = password
            self.refresh_token()
        self.headers = {'Content-type': 'application/json', 'Authorization': f'Bearer {self.token}'}

        self.database = None

        self._time_steps = {
            'TimeSeconds': [1, 60],
            'TimeMinutes': [1, 60],
            'TimeHours': [1, 12, 24],
            'TimeDays': [1, 7, 28],
            'TimeWeeks': [1, 4],
            'TimeMonths': [1, 3, 6, 12],
            'TimeQuarters': [1, 4],
            'TimeTrimesters': [1, 3],
            'TimeHalf-years': [1, 2],
            'TimeYears': [1, 2]
        }

        self._dimension_dict = {}

        self._measure_dict = {}

        self._hierarchy_dict = {}

        self._level_type_dict = {
            '0': 'Standard',
            '20': 'TimeYears',
            '36': 'TimeHalfYears',
            '68': 'TimeQuarters',
            '4722': 'TimeTrimesters',
            '132': 'TimeMonths',
            '260': 'TimeWeeks',
            '516': 'TimeDays',
            '772': 'TimeHours',
            '1028': 'TimeMinutes',
            '2052': 'TimeSeconds',
            '4100': 'Undefined'
        }

        self.refresh_project()
        logging.debug('AtScale project created, refreshing')

    # Update, Refresh, Publish, Export, and Clone

    def refresh_token(self):
        """ Refreshes the API token.
        """
        if self.username is None:
            logging.info('You can not refresh the token if you logged in with a token, log in with username and '
                            'password or get a new token online')
        elif self.password is None:
            self.password = getpass.getpass(prompt=f'AtScale Password for username {self.username}: ')
        logging.debug('Refreshing API token')
        header = {'Content-type': 'application/json'}
        url = f'{self.server}:{self.design_center_server_port}/{self.organization}/auth'
        response = requests.get(url, headers=header, auth=HTTPBasicAuth(self.username, self.password))
        if response.ok:
            self.token = response.content.decode()
            self.headers = {'Content-type': 'application/json', 'Authorization': f'Bearer {self.token}'}
        elif response.status_code == 401:
            raise UserError(response.text)
        else:
            resp = json.loads(response.text)
            raise Exception(resp['response']['error'])

    def update_project_tables(self, tables=None):
        """ Updates the project's tables.
        :param list of str tables: The tables to update info for. Defaults to None for all tables in the project

        """
        logging.debug('ATSCALE.py: updating project tables')
        project_json = self.project_json
        datasets = [x for x in project_json['datasets']['data-set']]
        #requires_update = False
        for dataset in datasets:
            data_set_id = dataset['id']
            conn = dataset['physical']['connection']['id']
            if 'tables' in dataset['physical']:
                project_tables = [x for x in dataset['physical']['tables']]
                url = f'{self.server}:{self.engine_port}/data-sources/orgId/{self.organization}/conn/{conn}/tables/cacheRefresh'
                response = requests.post(url, data='', headers=self.headers)
                if response.status_code != 200:
                    resp = json.loads(response.text)
                    raise Exception(resp['response']['error'])
                for table in project_tables:
                    if tables is None or table['name'] in tables:
                        info = ''
                        if 'database' in table:
                            info = '?database=' + table['database']
                        if 'schema' in table:
                            if info == '':
                                info = '?schema=' + table['schema']
                            else:
                                info = f'{info}&schema={table["schema"]}'
                        url = f'{self.server}:{self.engine_port}/data-sources/orgId/{self.organization}/conn/{conn}/table/{table["name"]}/info{info}'
                        response = requests.get(url, headers=self.headers)
                        if response.status_code != 200:
                            resp = json.loads(response.text)
                            raise Exception(resp['response']['error'])
                        server_columns = [(x['name'], x['column-type']['data-type']) for x in
                                          json.loads(response.content)['response']['columns']]
                        project_columns = [(x['name'], x['type']['data-type']) for x in dataset['physical']['columns'] if
                                           'sqls' not in x]
                        project_sql_columns = [x for x in dataset['physical']['columns'] if 'sqls' in x]
                        if set(server_columns) != set(project_columns):
                            columns = project_sql_columns
                            for column in server_columns:
                                uid = str(uuid.uuid4())
                                column_json = {'id': uid, 'name': column[0], 'type': {'data-type': column[1]}}
                                columns.append(column_json)
                            dataset['physical']['columns'] = columns
                            #requires_update = True
        #if requires_update:
        #    self._update_project(project_json, publish)

    def refresh_project(self):
        """ Refreshes the project to pick up any changes from the server.
        """
        url = f'{self.server}:{self.design_center_server_port}/api/1.0/org/{self.organization}/project/{self.project_id}'
        response = requests.get(url, headers=self.headers)
        if response.status_code == 401:
            logging.info('Invalid authentication, if you created this instance with a token, you need a new one')
            self.refresh_token()
            response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            self.project_json = json.loads(response.content)['response']
            self.model_name = [x['name'] for x in self.project_json['cubes']['cube'] if x['id'] == self.model_id][0]
            self.project_name = self._get_project_name()
            self._parse_json()
        else:
            resp = json.loads(response.text)
            raise Exception(resp['response']['error'])
        #self.update_project_tables()

    def _get_project_name(self):
        """ Returns the name of the project.

        :return: The project name.
        :rtype: str
        """
        url = f'{self.server}:{self.engine_port}/projects/published/orgId/{self.organization}'
        response = requests.get(url, headers=self.headers)
        if response.status_code == 401:
            logging.info('Invalid authentication, if you created this instance with a token, you need a new one')
            self.refresh_token()
            response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            projects = json.loads(response.content)['response']
            for project in projects:
                if project['publishType'] == 'normal_publish':
                    for cube in project['cubes']:
                        if cube['id'] == self.model_id:
                            return project['name']
            return None
        else:
            resp = json.loads(response.text)
            raise Exception(resp['response']['error'])

    def _update_project(self, project_json, publish=True):
        """ Updates the project.

        :param json project_json: The local version of the project JSON being pushed to the server.
        :param bool publish: Whether or not the updated project should be published. Defaults to True.
        """
        snap = self.create_snapshot(f'Python snapshot {datetime.now()}')
        url = f'{self.server}:{self.design_center_server_port}/api/1.0/org/{self.organization}/project/{self.project_id}'
        response = requests.put(url, data=json.dumps(project_json), headers=self.headers)

        if response.status_code == 401:
            logging.info('Invalid authentication, if you created this instance with a token, you need a new one')
            self.refresh_token()
            response = requests.put(url, data=json.dumps(project_json), headers=self.headers)
        try:
            if response.status_code != 200:
                resp = json.loads(response.text)
                raise Exception(resp['response']['error'])
            if publish is True:
                self.publish_project()

        except Exception:
            self.restore_snapshot(snap)
            self.delete_snapshot(snap)
            self.refresh_project()
            raise
        self.delete_snapshot(snap)

    def publish_project(self):
        """ Publishes the project to make changes available to other tools.
        """
        data = {}
        json_data = json.dumps(data)
        url = f'{self.server}:{self.design_center_server_port}/api/1.0/org/{self.organization}/project/{self.project_id}'
        response = requests.post(f'{url}/publish', data=json_data, headers=self.headers)
        if response.status_code != 200:
            resp = json.loads(response.text)
            raise Exception(resp['response']['error'])
        self.refresh_project()

    def export_project(self, filename):
        """ Writes the project JSON to a file.

        :param str filename: What the name of the file should be.
        """
        if filename[-5:] != '.json':
            filename += '.json'

        self.refresh_project()
        f = open(filename, 'w+')
        f.write(json.dumps(self.project_json))
        f.close()

    def clone_project(self, name):
        """ Clones the current project.

        :param str name: The new name of the cloned project.
        """
        url = f'{self.server}:{self.design_center_server_port}/api/1.0/org/{self.organization}/project/{self.project_id}'
        response = requests.get(f'{url}/clone', headers=self.headers)
        if response.status_code == 401:
            logging.info('Invalid authentication, if you created this instance with a token, you need a new one')
            self.refresh_token()
            response = requests.get(f'{url}/clone', headers=self.headers)
        if response.status_code == 200:
            copy_json = json.loads(response.content)['response']
            copy_json['name'] = name
            copy_json['properties']['caption'] = name
            original_datasets = [x for x in self.project_json['datasets']['data-set']]
            data_list = []
            for dataset in original_datasets:
                data_list.append(dataset['physical']['connection']['id'])
            for copy_data in copy_json['datasets']['data-set']:
                copy_data['physical']['connection']['id'] = data_list.pop(0)
            for cube in copy_json['cubes']['cube']:
                if cube['name'] == self.model_name:
                    self.model_id = cube['id']
            self.project_id = self.create_new_project(copy_json)
            self.project_name = name
            self.publish_project()
        else:
            resp = json.loads(response.text)
            raise Exception(resp['response']['error'])

    def create_snapshot(self, name):
        """ Creates a snapshot of the current project.

        :param str name: The name of the snapshot.
        :return: The snapshot ID.
        :rtype: str
        """
        url = f'{self.server}:{self.design_center_server_port}/api/1.0/org/{self.organization}/project/{self.project_id}/snapshots'
        tag = {'tag': name}
        response = requests.post(url, data=json.dumps(tag), headers=self.headers)
        if response.status_code != 200:
            resp = json.loads(response.text)
            raise Exception(resp['response']['error'])
        return json.loads(response.content)['response']['snapshot_id']

    def delete_snapshot(self, snapshot_id):
        """ Deletes a snapshot.

        :param str snapshot_id: The ID of the snapshot to be deleted.
        """
        url = f'{self.server}:{self.design_center_server_port}/api/1.0/org/{self.organization}' \
              f'/project/{self.project_id}/snapshots/{snapshot_id}'
        response = requests.delete(url, headers=self.headers)
        if response.status_code != 200:
            resp = json.loads(response.text)
            raise Exception(resp['response']['error'])

    def restore_snapshot(self, snapshot_id):
        """ Restores a project to a snapshot.

        :param str snapshot_id: The ID of the snapshot to be restored from.
        """
        url = f'{self.server}:{self.design_center_server_port}/api/1.0/org/{self.organization}/project/{self.project_id}/snapshots/{snapshot_id}/restore'
        response = requests.get(url, headers=self.headers)  # in API documentation, says to use put, but doesn't work
        if response.status_code != 200:
            resp = json.loads(response.text)
            raise Exception(resp['response']['error'])

    def return_snapshot_id(self, name=''):
        """ Returns the IDs of snapshots.

        param str name: The name of the snapshot for which the ID is requested.
        :return: The ID of a snapshot, a list of snapshot IDs, or a dict of all snapshot names with corresponding IDs
        :rtype: str or lst of str or dict str:str
        """
        url = f'{self.server}:{self.design_center_server_port}/api/1.0/org/{self.organization}/project/{self.project_id}/snapshots'

        response = json.loads(requests.get(url, headers=self.headers).text)['response']
        response.reverse()

        if name:
            return_id = [x['snapshot_id'] for x in response if x['name'] == name]

            if not return_id:
                raise UserError(f'Snapshot: \'{name}\' not found. Make sure all snapshots are spelled correctly')
            else:
                if len(return_id) == 1:
                    return return_id[0]
                else:
                    return return_id
        else:
            id_dict = {}
            for i in response:
                id_dict[i['snapshot_id']] = i['name']

            return id_dict

    def create_new_project(self, json_data):
        """ Creates a new project using the JSON data provided.

        :param json json_data: The JSON file to be sent to AtScale.
        :return: The ID of the new project.
        :rtype: str
        """
        url = f'{self.server}:{self.design_center_server_port}/api/1.0/org/{self.organization}/project'
        response = requests.post(url, data=json.dumps(json_data), headers=self.headers)
        if response.status_code != 200:
            resp = json.loads(response.text)
            raise Exception(resp['response']['error'])
        return json.loads(response.content)['response']['id']

    # List features
    
    def list_all_categorical_features(self, folder=''):
        """ Gets all available categorical features and denormalized categorical features.

        :param str folder: The folder to display the features from. Defaults to '' to show all categorical features and denormalized categorical features.
        :return: A list of all available categorical features and denormalized categorical features.
        :rtype: list of str
        """
        return [name for (name, info) in self._dimension_dict.items() if info['visible'] and (folder == '' or info['folder'] == folder)]

    def _list_aggregate_features(self, folder=''):
        """ Gets all available aggregate features.

        :param str folder: The folder to display the features from. Defaults to '' to show all aggregate features.
        :return: A list of all available aggregate features.
        :rtype: list of str
        """
        return [name for (name, info) in self._measure_dict.items() if info['visible'] and info['type'] == 'Aggregate' and (folder == '' or info['folder'] == folder)]

    def _list_calculated_features(self, folder=''):
        """ Gets all available calculated features.

        :param str folder: The folder to display the features from. Defaults to '' to show all calculated features.
        :return: A list of all available calculated features.
        :rtype: list of str
        """
        return [name for (name, info) in self._measure_dict.items() if info['visible'] and info['type'] == 'Calculated' and (folder == '' or info['folder'] == folder)]

    def list_all_numeric_features(self, folder=''):
        """ Gets all available aggregate and calculated features.

        :param str folder: The folder to display the features from. Defaults to '' to show all aggregate features and calculated features.
        :return: A list of all available aggregate features and calculated features.
        :rtype: list of str
        """
        return [name for (name, info) in self._measure_dict.items() if info['visible'] and (folder == '' or info['folder'] == folder)]

    def list_all_features(self, folder=''):
        """ Gets all available features.

        :param str folder: The folder to display the features from. Defaults to '' to show all features.
        :return: A list of all available features.
        :rtype: list of str
        """
        return self.list_all_numeric_features(folder) + self.list_all_categorical_features(folder)

    # Get Feature Descriptions

    def get_feature_description(self, feature_name):
        """ Gets the description for the given feature.

        :param str feature_name: The name of the feature to pull the description from.
        :return: The description of the feature.
        :rtype: str
        """
        if feature_name in self._measure_dict:
            return_descrip = self._measure_dict[feature_name]['description']
        elif feature_name in self._dimension_dict:
            return_descrip = self._dimension_dict[feature_name]['description']
        else:
            raise UserError(f'Feature: \'{feature_name}\' not in model. Make sure the feature has been published and is'
            ' correctly spelled')
            
        if return_descrip == '':
            return f'No Description for Feature: \'{feature_name}\''
        else:
            return return_descrip
            
    # List hierarchies

    def list_all_hierarchies(self, folder=''):
        """ Lists all hierarchies.

        :param str folder: The name of the folder to return hierarchies from. Defaults to '' show all hierarchies.
        :return: A list of hierarchies.
        :rtype: list of str
        """
        return [name for (name, info) in self._hierarchy_dict.items() if info['visible'] and (folder == '' or info['folder'] == folder)]

    def list_hierarchy_levels(self, hierarchy_name):
        """ Lists the levels of a given hierarchy from lowest to highest
        
        :param str hierarchy_name: The name of the hierarchy.
        :return: A list of levels from lowest to highest
        :rtype: list of str
        """        

        if hierarchy_name in self._hierarchy_dict:
            lst = [name for (level_number, name, time_step) in sorted(self._hierarchy_dict[hierarchy_name]['levels'])]
            return lst
        else:
            raise UserError(f'Hierarchy: \'{hierarchy_name}\' not in model.'
                            f' Make sure the model has been published and it is correctly spelled')

    def _get_hierarchy_level_time_step(self, hierarchy_name, level_name):
        """ Gets the time step for the level in the hierarchy.
       
        :param str hierarchy_name: The name of the hierarchy.
        :param str level_name: The name of the level in the hierarchy.
        :return: A time step.
        :rtype: str
        """
        if hierarchy_name in self._hierarchy_dict:
            hierarchy = self._hierarchy_dict[hierarchy_name]
            if hierarchy['type'] == 'Time':
                if level_name in self._dimension_dict and self._dimension_dict[level_name]['hierarchy']==hierarchy_name:
                    return self._dimension_dict[level_name]['level_type']
                else:
                    raise UserError(f'Level: {level_name} not in Hierarchy: {hierarchy_name}. '
                                    f'Make sure the model has been published and it is correctly spelled')
            else:
                raise UserError(f'Level: {level_name} is not a time dimension')
        else:
            raise UserError(f'Hierarchy: \'{hierarchy_name}\' not in model. Make sure the model has been published '
                            f'and it is correctly spelled')

    def get_hierarchy_description(self, hierarchy_name):
        """ Gets the description for a given hierarchy.

        :param str hierarchy_name: The name of the hierarchy.
        :return: The description of the hierarchy.
        :rtype: str
        """
        hierarchies = self._hierarchy_dict.keys()
        if hierarchy_name not in hierarchies:
            raise UserError(f'Hierarchy: \'{hierarchy_name}\' not in model. Make sure the hierarchy has been published and is'
                            ' correctly spelled')

        this_dict = self._hierarchy_dict[hierarchy_name]
        return_descrip = this_dict['description']
        if return_descrip == '':
            return f'No Description for Hierarchy: \'{hierarchy_name}\''
        else:
            return return_descrip

    def _hierarchy_dimension(self, hierarchy_name):
        """ Finds the dimension a hierarchy is in.

        :param str hierarchy_name: The name of the hierarchy.
        :return: The dimension's name.
        :rtype: str
        """
        if hierarchy_name in self._hierarchy_dict:
            return self._hierarchy_dict[hierarchy_name]['dimension']
        else:
            raise UserError(f'Hierarchy: \'{hierarchy_name}\' not in model. Make sure the hierarchy has been published and is'
            ' correctly spelled')
            
    def list_all_folders(self):
        """ Lists the folders in the model
        
        :return: A list of folders
        :rtype: list of str
        """
        hierarchy_folders  = [info['folder'] for (name, info) in self._hierarchy_dict.items()]
        measure_folders  = [info['folder'] for (name, info) in self._measure_dict.items()]
        dimension_folders  = [info['folder'] for (name, info) in self._dimension_dict.items()]
        folders = list(set(hierarchy_folders + measure_folders + dimension_folders))
        if '' in folders:
            folders.remove('')
        return folders

    # Querying and Describing

    def get_data(self, features, filter_equals=None, filter_greater=None, filter_less=None, filter_greater_or_equal=None, filter_less_or_equal=None, 
                 filter_not_equal=None, filter_in=None, filter_between=None, filter_like=None, filter_rlike=None, filter_null=None, filter_not_null=None,
                 limit=None, comment=None, useAggs=True, genAggs=False, fakeResults=False, dryRun=False, useLocalCache=True, useAggregateCache=True, timeout=2):
        """ Submits a query using the supplied information and returns the results in a pandas DataFrame.

        :param list of str features: The list of features to query.
        :param dict of str/str filter_equals: Filters results based on the feature equaling the value. Defaults to None
        :param dict of str/str filter_greater: Filters results based on the feature being greater than the value. Defaults to None
        :param dict of str/str filter_less: Filters results based on the feature being less than the value. Defaults to None
        :param dict of str/str filter_greater_or_equal: Filters results based on the feature being greater or equaling the value. Defaults to None
        :param dict of str/str filter_less_or_equal: Filters results based on the feature being less or equaling the value. Defaults to None
        :param dict of str/str filter_not_equal: Filters results based on the feature not equaling the value. Defaults to None
        :param dict of str/list of str filter_in: Filters results based on the feature being contained in the values. Defaults to None
        :param dict of str/tuple of (str,str) filter_between: Filters results based on the feature being between the values. Defaults to None
        :param dict of str/str filter_like: Filters results based on the feature being like the clause. Defaults to None
        :param dict of str/str filter_rlike: Filters results based on the feature being matched by the regular expression. Defaults to None
        :param list of str filter_null: Filters results to show null values of the specified features. Defaults to None
        :param list of str filter_not_null: Filters results to exclude null values of the specified features. Defaults to None
        :param int limit: Limit the number of results. Defaults to None for no limit.
        :param str comment: A comment string to build into the query. Defaults to None for no comment.
        :param bool useAggs: Whether to allow the query to use aggs. Defaults to True.
        :param bool genAggs: Whether to allow the query to generate aggs. Defaults to False.
        :param bool fakeResults: Whether to use fake results. Defaults to False.
        :param bool dryRun: Whether the query is a dry run. Defaults to False.
        :param bool useLocalCache: Whether to allow the query to use the local cache. Defaults to True.
        :param bool useAggregateCache: Whether to allow the query to use the aggregate cache. Defaults to True.
        :param int timeout: The number of minutes to wait for a response before timing out. Defaults to 2.
        :return: A pandas DataFrame containing the query results.
        :rtype: pandas.DataFrame
        """
        if filter_equals is None:
            filter_equals = {}
        if filter_greater is None:
            filter_greater = {}
        if filter_less is None:
            filter_less = {}
        if filter_greater_or_equal is None:
            filter_greater_or_equal = {}
        if filter_less_or_equal is None:
            filter_less_or_equal = {}
        if filter_not_equal is None:
            filter_not_equal = {}
        if filter_in is None:
            filter_in = {}
        if filter_between is None:
            filter_between = {}
        if filter_like is None:
            filter_like = {}
        if filter_rlike is None:
            filter_rlike = {}
        if filter_null is None:
            filter_null = []
        if filter_not_null is None:
            filter_not_null = []
        if type(filter_null) != list:
            filter_null = [filter_null]
        if type(filter_not_null) != list:
            filter_not_null = [filter_not_null]

        if type(features) != list:
            features = [features]

        list_all = self.list_all_features()
        self._check_multiple_features(features, list_all)
        self._check_multiple_features(filter_equals, list_all)
        self._check_multiple_features(filter_greater, list_all)
        self._check_multiple_features(filter_less, list_all)
        self._check_multiple_features(filter_greater_or_equal, list_all)
        self._check_multiple_features(filter_less_or_equal, list_all)
        self._check_multiple_features(filter_not_equal, list_all)
        self._check_multiple_features(filter_in, list_all)
        self._check_multiple_features(filter_between, list_all)
        self._check_multiple_features(filter_like, list_all)
        self._check_multiple_features(filter_rlike, list_all)
        self._check_multiple_features(filter_null, list_all)
        self._check_multiple_features(filter_not_null, list_all)
        
        categorical_features = []
        numeric_features = []

        all_categorical_features = self.list_all_categorical_features()
        for feature in features:
            if feature in all_categorical_features:
                categorical_features.append(feature)
            else:
                numeric_features.append(feature)

        if categorical_features:
            categorical_columns_string = ' ' + ', '.join(f'`{self.model_name}`.`{x}`' for x in categorical_features)
            if numeric_features:
                categorical_columns_string += ','
        else:
            categorical_columns_string = ''
        if numeric_features:
            numeric_columns_string = ' ' + ', '.join(f'`{self.model_name}`.`{x}`' for x in numeric_features)
        else:
            numeric_columns_string = ''
        
        if filter_equals or filter_greater or filter_less or filter_greater_or_equal or filter_less_or_equal or filter_not_equal \
                or filter_in or filter_between or filter_null or filter_not_null or filter_like or filter_rlike:
            filter_string = ' WHERE ('
            for key, value in filter_equals.items():
                if filter_string != ' WHERE (':
                    filter_string += ' and '
                if not isinstance(value, (int, float, bool)):
                    filter_string += f'(`{self.model_name}`.`{key}` = \'{value}\')'
                else:
                    filter_string += f'(`{self.model_name}`.`{key}` = {value})'
            for key, value in filter_greater.items():
                if filter_string != ' WHERE (':
                    filter_string += ' and '
                if not isinstance(value, (int, float, bool)):
                    filter_string += f'(`{self.model_name}`.`{key}` > \'{value}\')'
                else:
                    filter_string += f'(`{self.model_name}`.`{key}` > {value})'
            for key, value in filter_less.items():
                if filter_string != ' WHERE (':
                    filter_string += ' and '
                if not isinstance(value, (int, float, bool)):
                    filter_string += f'(`{self.model_name}`.`{key}` < \'{value}\')'
                else:
                    filter_string += f'(`{self.model_name}`.`{key}` < {value})'
            for key, value in filter_greater_or_equal.items():
                if filter_string != ' WHERE (':
                    filter_string += ' and '
                if not isinstance(value, (int, float, bool)):
                    filter_string += f'(`{self.model_name}`.`{key}` >= \'{value}\')'
                else:
                    filter_string += f'(`{self.model_name}`.`{key}` >= {value})'
            for key, value in filter_less_or_equal.items():
                if filter_string != ' WHERE (':
                    filter_string += ' and '
                if not isinstance(value, (int, float, bool)):
                    filter_string += f'(`{self.model_name}`.`{key}` <= \'{value}\')'
                else:
                    filter_string += f'(`{self.model_name}`.`{key}` <= {value})'
            for key, value in filter_not_equal.items():
                if filter_string != ' WHERE (':
                    filter_string += ' and '
                if not isinstance(value, (int, float, bool)):
                    filter_string += f'(`{self.model_name}`.`{key}` <> \'{value}\')'
                else:
                    filter_string += f'(`{self.model_name}`.`{key}` <> {value})'
            for key, value in filter_like.items():
                if filter_string != ' WHERE (':
                    filter_string += ' and '
                if not isinstance(value, (int, float, bool)):
                    filter_string += f'(`{self.model_name}`.`{key}` LIKE \'{value}\')'
                else:
                    filter_string += f'(`{self.model_name}`.`{key}` LIKE {value})'
            for key, value in filter_rlike.items():
                if filter_string != ' WHERE (':
                    filter_string += ' and '
                filter_string += f'(`{self.model_name}`.`{key}` RLIKE \'{value}\')'
            for key, value in filter_in.items():
                str_values = [str(x) for x in value]
                if filter_string != ' WHERE (':
                    filter_string += ' and '
                if not isinstance(value[0], (int, float, bool)):
                    filter_string += f'(`{self.model_name}`.`{key}` IN (\''
                    filter_string += '\', \''.join(str_values)
                    filter_string += '\'))'
                else:
                    filter_string += f'(`{self.model_name}`.`{key}` IN ('
                    filter_string += ', '.join(str_values)
                    filter_string += '))'
            for key, value in filter_between.items():
                if filter_string != ' WHERE (':
                    filter_string += ' and '
                if not isinstance(value[0], (int, float, bool)):
                    filter_string += f'(`{self.model_name}`.`{key}` BETWEEN \'{value[0]}\' and \'{value[1]}\')'
                else:
                    filter_string += f'(`{self.model_name}`.`{key}` BETWEEN {value[0]} and {value[1]})'
            for key in filter_null:
                if filter_string != ' WHERE (':
                    filter_string += ' and '
                filter_string += f'(`{self.model_name}`.`{key}` IS NULL)'
            for key in filter_not_null:
                if filter_string != ' WHERE (':
                    filter_string += ' and '
                filter_string += f'(`{self.model_name}`.`{key}` IS NOT NULL)'
            filter_string += ') GROUP BY 1'
        else:
            filter_string = ''
                
        if limit is None:
            limit_string = ''
        else:
            limit_string = f' LIMIT {limit}'

        if comment is None:
            comment_string = ''
        else:
            comment_string = f' /* {comment} */'
        
        version_comment = f' /* Python library version: {self.__version__} */'

        query = f'SELECT{categorical_columns_string}{numeric_columns_string}' \
                f' FROM `{self.project_name}`.`{self.model_name}` `{self.model_name}`' \
                f'{filter_string}{limit_string}{comment_string}{version_comment}'

        df = self.custom_query(query, 'SQL', useAggs, genAggs, fakeResults, dryRun, useLocalCache, useAggregateCache, timeout)
        if categorical_features:
            df.sort_values(categorical_features, inplace=True)
        return df

    def describe(self, categorical_features, numeric_features):
        """ Gets a description of all measures for the cube.

        :param list of str categorical_features: The categorical features to describe the numeric features over.
        :param list of str numeric_features: The numeric features to describe.
        :return: A pandas DataFrame with the description of all the categorical features in the cube over the given numeric features.
        :rtype: pandas.DataFrame
        """
        all_categorical_features = self.list_all_categorical_features()
        all_numeric_features = self.list_all_numeric_features()

        self._check_multiple_features(categorical_features, all_categorical_features,
                                      errmsg='Make sure all items in categorical_features are '
                                             'categorical features')

        self._check_multiple_features(numeric_features, all_numeric_features,
                                      errmsg='Make sure all items in numeric_features are '
                                             'numeric features')

        df = self.get_data(categorical_features + numeric_features)
        df.dropna(how='all', axis='columns', inplace=True)
        return df.describe(include='all')

    def _parse_query_response(self, response):
        """ Parses a query response.

        :param requests.Response response: The response used to formulate the dataframe that the function returns.
        :return: A pandas DataFrame.
        :rtype: pandas.DataFrame
        """
        content = str(response.content)

        if re.search('<succeeded>(.*?)</succeeded>', content).group(1) == 'false':
            raise Exception(re.search('<error-message>(.*?)</error-message>', ' '.join(content.split('\n'))).group(1))
        column_names = re.findall('<name>(.*?)</name>', content)
        row_text = re.findall('<row>(.*?)</row>', content)
        rows = []
        for row in row_text:
            row = row.replace('<column null="true"/>', '<column></column>')
            cells = re.findall('<column>(.*?)</column>', row)
            rows.append(cells)
        df = pd.DataFrame(data=rows, columns=column_names)
        for column in df.columns:
            df[column] = pd.to_numeric(df[column].values, errors='ignore')
        return df

    def custom_query(self, query, language='SQL', useAggs=True, genAggs=False, fakeResults=False, dryRun=False,
                     useLocalCache=True, useAggregateCache=True, timeout=2):
        """ Submits the given query and returns the results in a pandas dataframe.

        :param str query: The query to submit.
        :param str language: The language of the query. Valid options are 'SQL' or 'MDX'. Defaults to 'SQL'.
        :param bool useAggs: Whether to allow the query to use aggs. Defaults to True.
        :param bool genAggs: Whether to allow the query to generate aggs. Defaults to False.
        :param bool fakeResults: Whether to use fake results. Defaults to False.
        :param bool dryRun: Whether the query is a dry run. Defaults to False.
        :param bool useLocalCache: Whether to allow the query to use the local cache. Defaults to True.
        :param bool useAggregateCache: Whether to allow the query to use the aggregate cache. Defaults to True.
        :param int timeout: The number of minutes to wait for a response before timing out. Defaults to 2.
        :return: A DataFrame containing the query results.
        :rtype: pandas.DataFrame
        """
        language = language.upper()
        valid_languages = ['SQL', 'MDX']
        if language not in valid_languages:
            raise Exception(f'Invalid language: {language}. Valid options are: {valid_languages}.')
        data = {
            'language': language,
            'query': query,
            'context': {
                'organization': {
                    'id': self.organization
                },
                'environment': {
                    'id': self.organization
                },
                'project': {
                    'name': self.project_name
                }
            },
            'aggregation': {
                'useAggregates': useAggs,
                'genAggregates': genAggs
            },
            'fakeResults': fakeResults,
            'dryRun': dryRun,
            'useLocalCache': useLocalCache,
            'useAggregateCache': useAggregateCache,
            f'timeout': '{timeout}.minutes'
        }
        json_data = json.dumps(data)
        response = requests.post(f'{self.server}:{self.engine_port}/query/orgId/{self.organization}/submit',
                                 data=json_data, headers=self.headers)
        if response.status_code == 401 or response.status_code == 403:
            logging.info('Invalid authentication, if you created this instance with a token, you need a new one')
            self.refresh_token()
            response = requests.post(
                f'{self.server}:{self.engine_port}/query/orgId/{self.organization}/submit', data=json_data,
                headers=self.headers)
        if response.status_code != 200:
            resp = json.loads(response.text)
            raise Exception(resp['response']['error'])
        return self._parse_query_response(response)

    # Parsing project JSON

    def _parse_json(self):
        """ Loads _measure_dict, _dimension_dict and _hierarchy_dict.
        """
        self._parse_dimensions()
        # hierarchies need to be parsed after dimensions so they can set dimension folders
        self._parse_hierarchies()
        self._parse_measures()
        
    def _parse_dimensions(self):
        level_rows = self._submit_dmv_query(f"""<?xml version="1.0" encoding="UTF-8"?>
        <Envelope xmlns="http://schemas.xmlsoap.org/soap/envelope/">
         <Body>
          <Execute xmlns="urn:schemas-microsoft-com:xml-analysis">
           <Command>
            <Statement>select [DIMENSION_UNIQUE_NAME], [HIERARCHY_UNIQUE_NAME], [LEVEL_UNIQUE_NAME], [LEVEL_NUMBER], [LEVEL_CAPTION], [LEVEL_NAME], [LEVEL_IS_VISIBLE], [LEVEL_TYPE], [DESCRIPTION] from $system.mdschema_levels where [CUBE_NAME] = @CubeName and [LEVEL_NAME] &lt;&gt; '(All)' and [DIMENSION_UNIQUE_NAME] &lt;&gt; '[Measures]'</Statement>
           </Command>
           <Properties>
            <PropertyList>
             <LocaleIdentifier>1033</LocaleIdentifier>
             <SspropInitAppName>Power BI Desktop</SspropInitAppName>
             <ClientProcessID>3628</ClientProcessID>
             <DataSourceInfo />
             <Catalog>{self.project_name}</Catalog>
             <Format>Tabular</Format>
             <Content>SchemaData</Content>
             <DbpropMsmdActivityID>8dff0ce3-6815-4b16-980e-ca414dd9402b</DbpropMsmdActivityID>
             <DbpropMsmdRequestID>da46398b-ad69-43bc-a80b-8ba0a9e6751e</DbpropMsmdRequestID>
            </PropertyList>
           </Properties>
           <Parameters xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
            <Parameter>
             <Name>CubeName</Name>
             <Value xsi:type="xsd:string">{self.model_name}</Value>
            </Parameter>
           </Parameters>
          </Execute>
         </Body>
        </Envelope>""")

        for level in level_rows:
            name = re.search('<LEVEL_NAME>(.*?)</LEVEL_NAME>', level)[1]

            this_dict = {}

            description = re.search('<DESCRIPTION>(.*?)</DESCRIPTION>', level)
            if description:
                this_dict['description'] = description[1]
            else:
                this_dict['description'] = ''

            caption = re.search('<LEVEL_CAPTION>(.*?)</LEVEL_CAPTION>', level)
            if caption:
                this_dict['caption'] = caption[1]
            else:
                this_dict['caption'] = ''

            visible = re.search('<LEVEL_IS_VISIBLE>(.*?)</LEVEL_IS_VISIBLE>', level)[1]
            this_dict['visible'] = visible
            
            level_number = re.search('<LEVEL_NUMBER>(.*?)</LEVEL_NUMBER>', level)[1]
            this_dict['level_number'] = int(level_number)
            
            level_type = re.search('<LEVEL_TYPE>(.*?)</LEVEL_TYPE>', level)[1]
            this_dict['level_type'] = self._level_type_dict[level_type]

            
            hierarchy_unique_name = re.search('<HIERARCHY_UNIQUE_NAME>(.*?)</HIERARCHY_UNIQUE_NAME>', level)[1]
            this_dict['hierarchy'] = hierarchy_unique_name.split('].[')[1][:-1]
            this_dict['dimension'] = hierarchy_unique_name.split('].[')[0][1:]

            self._dimension_dict[name] = this_dict

    def _parse_measures(self):
        measure_rows = self._submit_dmv_query(f"""<?xml version="1.0" encoding="UTF-8"?>
                <Envelope xmlns="http://schemas.xmlsoap.org/soap/envelope/">
                   <Body>
                      <Execute xmlns="urn:schemas-microsoft-com:xml-analysis">
                         <Command>
                            <Statement>SELECT [CATALOG_NAME], [SCHEMA_NAME], [CUBE_NAME], [MEASURE_NAME], [MEASURE_UNIQUE_NAME], [MEASURE_GUID], [MEASURE_CAPTION], [MEASURE_AGGREGATOR], [DATA_TYPE], [NUMERIC_PRECISION], [NUMERIC_SCALE], [MEASURE_UNITS], [DESCRIPTION], [EXPRESSION], [MEASURE_IS_VISIBLE], [MEASURE_IS_VISIBLE], [MEASURE_NAME_SQL_COLUMN_NAME], [MEASURE_UNQUALIFIED_CAPTION], [MEASUREGROUP_NAME], [MEASURE_DISPLAY_FOLDER], [DEFAULT_FORMAT_STRING] FROM $system.MDSCHEMA_MEASURES</Statement>
                         </Command>
                         <Properties>
                            <PropertyList>
                               <LocaleIdentifier>1033</LocaleIdentifier>
                               <SspropInitAppName>Power BI Desktop</SspropInitAppName>
                               <ClientProcessID>3628</ClientProcessID>
                               <DataSourceInfo />
                               <Catalog>{self.project_name}</Catalog>
                               <Format>Tabular</Format>
                               <Content>SchemaData</Content>
                               <DbpropMsmdActivityID>8dff0ce3-6815-4b16-980e-ca414dd9402b</DbpropMsmdActivityID>
                               <DbpropMsmdRequestID>da46398b-ad69-43bc-a80b-8ba0a9e6751e</DbpropMsmdRequestID>
                            </PropertyList>
                         </Properties>
                         <Parameters xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                            <Parameter>
                               <Name>CubeName</Name>
                               <Value xsi:type="xsd:string">{self.model_name}</Value>
                            </Parameter>
                         </Parameters>
                      </Execute>
                   </Body>
                </Envelope>""")

        for measure in measure_rows:
            name = re.search('<MEASURE_NAME>(.*?)</MEASURE_NAME>', measure)[1]

            this_dict = {}

            description = re.search('<DESCRIPTION>(.*?)</DESCRIPTION>', measure)
            if description:
                this_dict['description'] = description[1]
            else:
                this_dict['description'] = ''

            caption = re.search('<MEASURE_CAPTION>(.*?)</MEASURE_CAPTION>', measure)
            if caption:
                this_dict['caption'] = caption[1]
            else:
                this_dict['caption'] = ''

            folder = re.search('<MEASURE_DISPLAY_FOLDER>(.*?)</MEASURE_DISPLAY_FOLDER>', measure)
            if folder:
                this_dict['folder'] = folder[1]
            else:
                this_dict['folder'] = ''

            visible = re.search('<MEASURE_IS_VISIBLE>(.*?)</MEASURE_IS_VISIBLE>', measure)[1]
            this_dict['visible'] = visible

            aggregator = re.search('<MEASURE_AGGREGATOR>(.*?)</MEASURE_AGGREGATOR>', measure)[1]

            if aggregator == '9':
                this_dict['type'] = 'Calculated'
            else:
                this_dict['type'] = 'Aggregate'

            self._measure_dict[name] = this_dict

    def _parse_hierarchies(self):
        hierarchy_rows = self._submit_dmv_query(f"""<?xml version="1.0" encoding="UTF-8"?>
                    <Envelope xmlns="http://schemas.xmlsoap.org/soap/envelope/">
                       <Body>
                          <Execute xmlns="urn:schemas-microsoft-com:xml-analysis">
                             <Command>
                                <Statement>SELECT [CATALOG_NAME], [SCHEMA_NAME], [CUBE_NAME], [DIMENSION_UNIQUE_NAME], [HIERARCHY_NAME], [HIERARCHY_UNIQUE_NAME], [HIERARCHY_GUID], [HIERARCHY_CAPTION], [DIMENSION_TYPE], [HIERARCHY_CARDINALITY], [DEFAULT_MEMBER], [ALL_MEMBER], [DESCRIPTION], [STRUCTURE], [IS_VIRTUAL], [IS_READWRITE], [DIMENSION_UNIQUE_SETTINGS], [DIMENSION_MASTER_UNIQUE_NAME], [DIMENSION_IS_VISIBLE], [HIERARCHY_ORIGIN], [HIERARCHY_DISPLAY_FOLDER], [INSTANCE_SELECTION], [GROUPING_BEHAVIOR], [STRUCTURE_TYPE] FROM $system.MDSCHEMA_HIERARCHIES</Statement>
                                \n
                             </Command>
                             \n
                             <Properties>
                                \n
                                <PropertyList>
                                   \n
                                   <LocaleIdentifier>1033</LocaleIdentifier>
                                   \n
                                   <SspropInitAppName>Power BI Desktop</SspropInitAppName>
                                   \n
                                   <ClientProcessID>3628</ClientProcessID>
                                   \n
                                   <DataSourceInfo />
                                   \n
                                   <Catalog>{self.project_name}</Catalog>
                                   \n
                                   <Format>Tabular</Format>
                                   \n
                                   <Content>SchemaData</Content>
                                   \n
                                   <DbpropMsmdActivityID>8dff0ce3-6815-4b16-980e-ca414dd9402b</DbpropMsmdActivityID>
                                   \n
                                   <DbpropMsmdRequestID>da46398b-ad69-43bc-a80b-8ba0a9e6751e</DbpropMsmdRequestID>
                                   \n
                                </PropertyList>
                                \n
                             </Properties>
                             \n
                             <Parameters xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                                <Parameter>
                                   <Name>CubeName</Name>
                                   <Value xsi:type="xsd:string">{self.model_name}</Value>
                                </Parameter>
                             </Parameters>
                          </Execute>
                       </Body>
                    </Envelope>""")

        for hierarchy in hierarchy_rows:

            structure = re.search('<STRUCTURE>(.*?)</STRUCTURE>', hierarchy)[1]

            name = re.search('<HIERARCHY_NAME>(.*?)</HIERARCHY_NAME>', hierarchy)[1]

            this_dict = {}

            dimension = re.search('<DIMENSION_UNIQUE_NAME>(.*?)</DIMENSION_UNIQUE_NAME>', hierarchy)
            if dimension:
                this_dict['dimension'] = dimension[1][1:-1]
            else:
                this_dict['dimension'] = ''

            description = re.search('<DESCRIPTION>(.*?)</DESCRIPTION>', hierarchy)
            if description:
                this_dict['description'] = description[1]
            else:
                this_dict['description'] = ''

            caption = re.search('<HIERARCHY_CAPTION>(.*?)</HIERARCHY_CAPTION>', hierarchy)
            if caption:
                this_dict['caption'] = caption[1]
            else:
                this_dict['caption'] = ''

            folder = re.search('<HIERARCHY_DISPLAY_FOLDER>(.*?)</HIERARCHY_DISPLAY_FOLDER>', hierarchy)
            if folder:
                hierarchy_folder = folder[1]
            else:
                hierarchy_folder = ''
            this_dict['folder'] = hierarchy_folder

            visible = re.search('<DIMENSION_IS_VISIBLE>(.*?)</DIMENSION_IS_VISIBLE>', hierarchy)[1]
            this_dict['visible'] = visible
            
            type = re.search('<DIMENSION_TYPE>(.*?)</DIMENSION_TYPE>', hierarchy)[1]
            if type == '1':
                this_dict['type'] = 'Time'
            elif type == '3':
                this_dict['type'] = 'Standard'
            else:
                this_dict['type'] = None

            levels = []
            for level in self.list_all_categorical_features():
                if self._dimension_dict[level]['hierarchy'] == name:
                    levels.append((self._dimension_dict[level]['level_number'], level, self._dimension_dict[level]['level_type']))
                    # push the folder to each level
                    self._dimension_dict[level]['folder'] = hierarchy_folder

            this_dict['levels'] = levels
            
            if structure == '1': # Seems to remove secondary attributes
                self._hierarchy_dict[name] = this_dict

    def _submit_dmv_query(self, query_body):
        """ Submit DMV Query.
        """

        url = f'{self.server}:{self.engine_port}/xmla/{self.organization}'
        headers = {'Content-type': 'application/xml', 'Authorization': f'Bearer {self.token}'}
        response = requests.post(url, data=query_body, headers=headers)

        xml_text = str(response.content)

        rows = re.findall('<row>(.*?)</row>', xml_text)

        return rows

    # Exception Handling

    def _check_single_element(self, feature, check_list, errmsg=None):
        """ Checks that a given feature exists within a given list of features.

        :param str feature: The feature being checked.
        :param list or dict of str check_list: The list of features against which the feature is being checked.
        :param str errmsg: A custom error message displayed if feature isn't in check_list. If not specified otherwise, the standard message will be displayed.
        """
        if feature not in check_list:
            if errmsg:
                raise UserError(errmsg)
            else:
                raise UserError(f'Feature: \'{feature}\' not in model.'
                                ' Make sure each feature has been published and is correctly spelled')

    def _check_single_dataset(self, dataset):
        """ Checks that a given dataset exists.

        :param str dataset: The dataset being checked.
        """
        dataset_names = [x['name'] for x in self.project_json['datasets']['data-set']]

        if dataset not in dataset_names:
            raise UserError(f'Dataset: \'{dataset}\' not in model.'
                            f' Make sure the model has been published and the dataset is correctly spelled')

    def _check_single_column(self, dataset_name, column):
        """ Checks that a given column exists within a given dataset.

        :param str dataset_name: The dataset containing the column being checked.
        :param str column: The column being checked.
        """
        self._check_single_dataset(dataset_name)

        dataset = [x for x in self.project_json['datasets']['data-set'] if x['name'] == dataset_name][0]
        columns = [x['name'] for x in dataset['physical']['columns'] if 'physical' in dataset and 'columns' in dataset['physical']]
                        
        if column not in columns:
            raise UserError(f'Column: \'{column}\' not in Dataset: \'{dataset_name}\'. Make sure the project has'
                            f' been published, everything is correctly spelled, and/or is in the dataset')

    def _check_multiple_features(self, features, check_list, catch_duplicates=False, errmsg=None):
        """ Checks that the given feature(s) exist(s) within a specified list of features.

        :param list of str features: The features being checked.
        :param list of str check_list: The list of features against which the features are being checked.
        :param bool catch_duplicates: Whether the function should alert the user if duplicate features exist. Defaults to False.
        :param str errmsg: A custom error message displayed if the function finds an error. If not specified otherwise, the standard message will be displayed.
        """
        if catch_duplicates:
            check_dict = {}

            for item in check_list:
                check_dict[item] = 0

            error = ''

            try:
                for f in features:
                    error = f
                    check_dict[f] += 1
            except KeyError:
                if errmsg:
                    raise UserError(errmsg)
                else:
                    raise UserError(f'Feature: \'{error}\' not in model. Make sure each feature has been published and is '
                                    'correctly spelled')
            for item in check_dict:
                if check_dict[item] > 1:
                    raise UserError(f'Feature: \'{item}\' occurs multiple times. Please remove duplicates or check that '
                                    'features are spelled correctly')
        else:
            for f in features:
                error = f
                if f not in check_list:
                    if errmsg:
                        raise UserError(errmsg)
                    else:
                        raise UserError(f'Feature: \'{error}\' not in model. Make sure each feature has been published and is'
                                        ' correctly spelled')

    def _check_single_connection(self, connection):
        """ Checks that the given connection is valid.

        :param str connection: The name of the connection being checked.
        """
        data = {}
        url = f'{self.server}:{self.engine_port}/connection-groups/orgId/{self.organization}'
        response = requests.get(url, data=json.dumps(data), headers=self.headers)

        check_list = [x['connectionId'] for x in json.loads(response.content)['response']['results']['values']]

        self._check_single_element(connection, check_list,
                                   errmsg=f'Connection: \'{connection}\' does not exist.'
                                          f' Make sure the atscale_connection_id argument is spelled correctly')

    # WRITE BACK

    # Check function for time hierarchies; to be lumped in with other check functions when writeback is rolled out

    def _check_time_hierarchy(self, hierarchy, level=''):
        """ Checks that the hierarchy given is a valid time hierarchy.

        :param str hierarchy: The name of the hierarchy to be checked.
        """
        if hierarchy in self._hierarchy_dict:
            if self._hierarchy_dict[hierarchy]['type'] != 'Time':
                raise UserError(f'Make sure Hierarchy: \'{hierarchy}\' is a time hierarchy')
            if level:
                if level not in [x[1] for x in self._hierarchy_dict[hierarchy]['levels']]:
                    raise UserError(f'Level: \'{level}\' not a valid level. Make sure the level argument exists in Hierarchy: \'{hierarchy}\'')
        else:
            raise UserError(f'Hierarchy: \'{hierarchy}\' not in model. Make sure the model has been published and that it is correctly spelled')

    # Creating/Adding/Deleting Columns, Features, etc.

    def create_calculated_column(self, dataset_name, name, expression, publish=True):
        """ Creates a new calculated column.
        
        :param str dataset_name: The dataset the calculated column will be derived in.
        :param str name: The name of the column.
        :param str expression: The SQL expression for the column.
        :param bool publish: Whether or not the updated project should be published. Defaults to True.
        """

        self.refresh_project()
        project_json = self.project_json
        
        self._check_single_dataset(dataset_name)
        
        data_set = [x for x in project_json['datasets']['data-set'] if x['name'] == dataset_name][0]
        conn = data_set['physical']['connection']['id']
        table = data_set['physical']['tables'][0]
        table_name = table['name']
        database = table['database']
        schema = table['schema']
        
        url = f'{self.server}:{self.engine_port}/expression-evaluator/evaluate/orgId/{self.organization}/conn/{conn}/table/{table_name}'
        data = {'dbschema': schema,
        'expression': expression,
        'database': database}
        headers = {'Content-type': 'application/x-www-form-urlencoded', 'Authorization': 'Bearer ' + self.token}
        response = requests.post(url, data=data, headers=headers)

        if response.status_code != 200:
            resp = json.loads(response.text)
            raise Exception(resp['response']['error'])
        else:
            resp = json.loads(response.text)
            data_type = resp['response']['data-type']

        new_column = {'name': name,
                      'sqls': [{'expression': expression}],
                      'type': {'data-type': data_type}
                      }
        data_set['physical']['columns'].append(new_column)

        self._update_project(project_json, publish)
        
    def create_mapped_columns(self, dataset_name, column_name, names, data_types, key_terminator, field_terminator, map_key_type, map_value_type, first_char_delimited=False, publish=True):
        """ Creates a new mapped column.
        
        :param str dataset_name: The dataset the mapped column will be derived in.
        :param str column_name: The name of the column.
        :param list str names: The names of the mapped columns.
        :param list str data_types: The types of the mapped columns.
        :param str key_terminator: The key terminator. Valid values are ':', '=', and '^'
        :param str field_terminator: The field terminator. Valid values are ',', ';', and '|'
        :param str map_key_type: The mapped key type.
        :param str map_value_type: The mapped value type.
        :param bool first_char_delimited: Whether the first character is delimited. Defaults to False.
        :param bool publish: Whether or not the updated project should be published. Defaults to True.
        """
        
        valid_key_terminators = [':', '=', '^']
        if key_terminator not in valid_key_terminators:
            raise Exception(f'Invalid key_terminator: `{key_terminator}` valid values are `:`, `=`, and `^`')
        valid_field_terminators = [',', ';', '|']
        if field_terminator not in valid_field_terminators:
            raise Exception(f'Invalid field_terminator: `{field_terminator}` valid values are `,`, `;`, and `|`')
            
        valid_types = ['Int', 'Long', 'Boolean', 'String', 'Float', 'Double', 'Integer', 'Decimal', 'DateTime', 'Date']
        for type in data_types:
            if type not in valid_types:
                raise Exception(f'Invalid data_type: `{type}` valid values are `Int`, `Long`, `Boolean`, `String`, '
                                f'`Float`, `Double`, `Integer`, `Decimal`, `DateTime`, `Date`')
        
        self.refresh_project()
        project_json = self.project_json
        
        self._check_single_column(dataset_name, column_name)
        
        project_dataset = [x for x in project_json['datasets']['data-set'] if x['name'] == dataset_name][0]
        if 'map-column' not in project_dataset['physical']:
            project_dataset['physical']['map-column'] = []
            
        cols = []
        for (column, type) in tuple(zip(names, data_types)):
            uid = str(uuid.uuid4())
            col = {
                'id': uid,
                'name': column,
                'type': {
                    'data-type': type
                }
            }
            cols.append(col)
        new_map = {
            'columns': {
                'columns': cols
            },
            'delimited': {
                'field-terminator': field_terminator,
                'key-terminator': key_terminator,
                'prefixed': first_char_delimited
            },
            'map-key': {
                'type': map_key_type
            },
            'map-value': {
                'type': map_value_type
            },
            'name': column_name
        }
        project_dataset['physical']['map-column'].append(new_map)

        self._update_project(project_json, publish)
        
    def add_column_mapping(self, dataset_name, column_name, name, data_type, publish=True):
        """ Adds a mapping to a previously created mapped column.
        
        :param str dataset_name: The dataset the calculated column will be derived in.
        :param str column_name: The name of the column.
        :param str name: The names of the mapped columns.
        :param str data_type: The types of the mapped columns.
        :param bool publish: Whether or not the updated project should be published. Defaults to True.
        """
        
        valid_types = ['Int', 'Long', 'Boolean', 'String', 'Float', 'Double', 'Integer', 'Decimal', 'DateTime', 'Date']
        if data_type not in valid_types:
            raise Exception(f'Invalid data_type: `{data_type}` valid values are `Int`, `Long`, `Boolean`, '
                            f'`String`, `Float`, `Double`, `Integer`, `Decimal`, `DateTime`, `Date`')
            
        self.refresh_project()
        
        self._check_single_column(dataset_name, column_name)

        project_json = self.project_json
        
        project_dataset = [x for x in project_json['datasets']['data-set'] if x['name'] == dataset_name][0]
        if 'map-column' not in project_dataset['physical']:
            raise Exception(f'No mapped column exists for column: {name}. Use create_mapped_columns to create one')

        uid = str(uuid.uuid4())
        col = {
            'id': uid,
            'name': name,
            'type': {
                'data-type': data_type
            }
        }

        mapped_cols = [x for x in project_dataset['physical']['map-column'] if x['name'] == column_name]
        if len(mapped_cols) < 1:
            raise Exception(f'No mapped column exists for column: {name}. Use create_mapped_columns to create one')

        col_map = mapped_cols[0]
        col_map['columns']['columns'].append(col)

        self._update_project(project_json, publish)

    def create_aggregate_feature(self, dataset_name, column, name, aggregation_type, description='', caption='',
                                 folder='', format_string='General Number', publish=True):
        """ Creates a new aggregate feature.

        :param str dataset_name: The dataset containing the column that the feature will use.
        :param str column: The column that the feature will use.
        :param str name: What the feature will be called.
        :param str aggregation_type: What aggregation method to use for the feature. Example: agg.MAX
                                     Valid options include 'SUM', 'AVG', 'MAX', 'MIN', 'DC',
                                     'DCE', 'NDC', 'STDDEV_SAMP', 'STDDEV_POP', 'VAR_SAMP',
                                     and 'VAR_POP' or any field in atscale.utils.Aggs
        :param str description: The description for the feature. Defaults to ''.
        :param str caption: The caption for the feature. Defaults to ''.
        :param str folder: The folder to put the feature in. Defaults to ''.
        :param str format_string: The format string for the feature. Defaults to 'General Number'.
        :param bool publish: Whether or not the updated project should be published. Defaults to True.
        """
        if name in self.list_all_features():
            raise Exception(f'Invalid name: \'{name}\'. A feature already exists with that name')
            
        self._check_single_column(dataset_name, column)

        aggregation_type_caps = aggregation_type.upper()
        valid_agg_types = ['SUM', 'AVG', 'MAX', 'MIN', 'DC', 'DCE', 'NDC', 'STDDEV_SAMP', 'STDDEV_POP', 'VAR_SAMP',
                           'VAR_POP']
        if aggregation_type_caps not in valid_agg_types:
            raise Exception(f'Invalid aggregation_type: \'{aggregation_type}\'. Valid options are: {valid_agg_types}.')
                
        valid_formatting_strings = ['None', 'General Number', 'Standard', 'Scientific']
        if format_string in valid_formatting_strings:
            formatting = {'named-format': format_string}
        else:
            formatting = {'format-string': format_string}
        
        self.refresh_project()
        project_json = self.project_json
        uid = str(uuid.uuid4())
        cube = [x for x in project_json['cubes']['cube'] if x['name'] == self.model_name][0]
        new_measure = {'id': uid,
                       'name': name,
                       'properties': {'caption': caption,
                                      'description': description,
                                      'folder': folder,
                                      'formatting': formatting,
                                      'type': {'measure': {'default-aggregation': aggregation_type_caps}},
                                      'visible': True}}
        if 'attributes' not in cube:
            cube['attributes'] = {}
        if 'attribute' not in cube['attributes']:
            cube['attributes']['attribute'] = []
        cube['attributes']['attribute'].append(new_measure)
        new_ref = {'column': [column], 'complete': 'true', 'id': uid}
        data_set_id = [x['id'] for x in project_json['datasets']['data-set'] if x['name'] == dataset_name][0]
        dataset = [x for x in cube['data-sets']['data-set-ref'] if x['id'] == data_set_id][0]
        if 'attribute-ref' not in dataset['logical']:
            dataset['logical']['attribute-ref'] = []
        dataset['logical']['attribute-ref'].append(new_ref)

        self._update_project(project_json, publish)
        
    def update_aggregate_feature_metadata(self, name, description=None, caption=None,
                             folder=None, format_string=None, publish=True):
        """ Update the metadata for an aggregate feature.

        :param str name: The name of the feature to update.
        :param str description: The description for the feature. Defaults to None to leave unchanged.
        :param str caption: The caption for the feature. Defaults to None to leave unchanged.
        :param str folder: The folder to put the feature in. Defaults to None to leave unchanged.
        :param str format_string: The format string for the feature. Defaults to None to leave unchanged.
        :param bool publish: Whether or not the updated project should be published. Defaults to True.
        """

        self.refresh_project()
        
        if name not in self._list_aggregate_features():
            raise Exception(f'Feature: {name} does not exist.')

        project_json = self.project_json
        cube = [x for x in project_json['cubes']['cube'] if x['name'] == self.model_name][0]
        measure = [x for x in cube['attributes']['attribute'] if x['name'] == name][0]
        if description is not None:
            measure['properties']['description'] = description
        if caption is not None:
            measure['properties']['caption'] = caption
        if folder is not None:
            measure['properties']['folder'] = folder
        if format_string is not None:
            valid_formatting_strings = ['None', 'General Number', 'Standard', 'Scientific']
            if format_string in valid_formatting_strings:
                formatting = {'named-format': format_string}
            else:
                formatting = {'format-string': format_string}
            measure['properties']['formatting'] = formatting
            
        self._update_project(project_json, publish)

    def create_calculated_feature(self, name, expression, description='', caption='', folder='',
                                  format_string='General Number', publish=True):
        """ Creates a new calculated feature.

        :param str name: What the feature will be called.
        :param str expression: The MDX expression for the feature.
        :param str description: The description for the feature. Defaults to ''.
        :param str caption: The caption for the feature. Defaults to ''.
        :param str folder: The folder to put the feature in. Defaults to ''.
        :param str format_string: The format string for the feature. Defaults to 'General Number'.
        :param bool publish: Whether or not the updated project should be published. Defaults to True.
        """
        self.refresh_project()
        
        if name in self.list_all_features():
            raise Exception(f'Invalid name: \'{name}\'. A feature already exists with that name')
            
        valid_formatting_strings = ['None', 'General Number', 'Standard', 'Scientific', 'Fixed', 'Percent']
        if format_string in valid_formatting_strings:
            formatting = {'named-format': format_string}
        else:
            formatting = {'format-string': format_string}
        
        project_json = self.project_json
        uid = str(uuid.uuid4())
        cube = [x for x in project_json['cubes']['cube'] if x['name'] == self.model_name][0]
        if 'calculated-members' not in project_json:
            project_json['calculated-members'] = {}
        if 'calculated-member' not in project_json['calculated-members']:
            project_json['calculated-members']['calculated-member'] = []
        new_calculated_measure = {'id': uid,
                                  'name': name,
                                  'expression': expression,
                                  'properties': {'caption': caption,
                                                 'description': description,
                                                 'folder': folder,
                                                 'formatting': formatting,
                                                 'visible': True}}
        project_json['calculated-members']['calculated-member'].append(new_calculated_measure)
        if 'calculated-members' not in cube:
            cube['calculated-members'] = {}
        if 'calculated-member-ref' not in cube['calculated-members']:
            cube['calculated-members']['calculated-member-ref'] = []
        new_ref = {'id': uid,
                   'XMLName': {
                       'Local': 'calculated-member-ref',
                       'Space': 'http://www.atscale.com/xsd/project_2_0'
                   }}
        cube['calculated-members']['calculated-member-ref'].append(new_ref)

        self._update_project(project_json, publish)
        
    def update_calculated_feature_metadata(self, name, description=None, caption=None, folder=None,
                              format_string=None, publish=True):
        """ Update the metadata for a calculated feature.

        :param str name: The name of the feature to update.
        :param str description: The description for the feature. Defaults to None to leave unchanged.
        :param str caption: The caption for the feature. Defaults to None to leave unchanged.
        :param str folder: The folder to put the feature in. Defaults to None to leave unchanged.
        :param str format_string: The format string for the feature. Defaults to None to leave unchanged.
        :param bool publish: Whether or not the updated project should be published. Defaults to True.
        """
        self.refresh_project()
        
        if name not in self._list_calculated_features():
            raise Exception(f'Feature: {name} does not exist.')
        
        project_json = self.project_json
        measure = [x for x in project_json['calculated-members']['calculated-member'] if x['name'] == name][0]
        if description is not None:
            measure['properties']['description'] = description
        if caption is not None:
            measure['properties']['caption'] = caption
        if folder is not None:
            measure['properties']['folder'] = folder
        if format_string is not None:
            valid_formatting_strings = ['None', 'General Number', 'Standard', 'Scientific', 'Fixed', 'Percent']
            if format_string in valid_formatting_strings:
                formatting = {'named-format': format_string}
            else:
                formatting = {'format-string': format_string}
            measure['properties']['formatting'] = formatting

        self._update_project(project_json, publish)

    def create_denormalized_categorical_feature(self, dataset_name, column, name, description='', caption='', folder='', publish=True):
        """ Creates a new denormalized categorical feature.

        :param str dataset_name: The dataset containing the column that the feature will use.
        :param str column: The column that the feature will use.
        :param str name: what the feature will be called.
        :param str description: the description for the feature. Defaults to ''.
        :param str caption: the caption for the feature. Defaults to ''.
        :param str folder: the folder to put the feature in. Defaults to ''.
        :param bool publish: Whether or not the updated project should be published. Defaults to True.
        """
        self.refresh_project()
        
        self._check_single_column(dataset_name, column)
        
        if caption == '':
            caption = name
        
        project_json = self.project_json
        hierarchy_id = str(uuid.uuid4())
        level_id = str(uuid.uuid4())
        dimension_id = str(uuid.uuid4())
        attribute_id = str(uuid.uuid4())
        ref_id = str(uuid.uuid4())
        cube = [x for x in project_json['cubes']['cube'] if x['name'] == self.model_name][0]
        new_dimension = {
            'hierarchy': [
                {
                    'id': hierarchy_id,
                    'level': [
                        {
                            'id': level_id,
                            'primary-attribute': attribute_id,
                            'properties': {
                                'unique-in-parent': False,
                                'visible': True
                            }
                        }
                    ],
                    'name': name,
                    'properties': {
                        'folder': folder,
                        'caption': caption,
                        'description': description,
                        'default-member': {
                            'all-member': {
                            }
                        },
                        'filter-empty': 'Always',
                        'visible': True
                    }
                }
            ],
            'id': dimension_id,
            'name': caption,
            'properties': {
                'visible': True
            }
        }
        if 'dimension' not in cube['dimensions']:
            cube['dimensions']['dimension'] = []
        cube['dimensions']['dimension'].append(new_dimension)
        new_ref = {
            'column': [
                column
            ],
            'complete': True,
            'id': attribute_id
        }
        data_set_id = [x['id'] for x in project_json['datasets']['data-set'] if x['name'] == dataset_name][0]
        dataset = [x for x in cube['data-sets']['data-set-ref'] if x['id'] == data_set_id][0]
        dataset['logical']['attribute-ref'].append(new_ref)
        new_keyed_attribute = {
            'id': attribute_id,
            'key-ref': ref_id,
            'name': name,
            'properties': {
                'description': description,
                'caption': caption,
                'type': {
                    'enum': {

                    }
                },
                'visible': True
            }
        }
        if 'keyed-attribute' not in cube['attributes']:
            cube['attributes']['keyed-attribute'] = []
        cube['attributes']['keyed-attribute'].append(new_keyed_attribute)
        new_attribute_key = {
            'id': ref_id,
            'properties': {
                'columns': 1,
                'visible': True
            }
        }
        if 'attribute-key' not in cube['attributes']:
            cube['attributes']['attribute-key'] = []
        cube['attributes']['attribute-key'].append(new_attribute_key)
        new_key_ref = {
            'column': [
                column
            ],
            'complete': 'true',
            'id': ref_id,
            'unique': False
        }
        if 'key-ref' not in dataset['logical']:
            dataset['logical']['key-ref'] = []
        dataset['logical']['key-ref'].append(new_key_ref)

        self._update_project(project_json, publish)
        
    def create_secondary_attribute(self, dataset_name, column, name, hierarchy, level, description='', caption='', folder='', publish=True):
        """ Creates a new secondary attribute.

        :param str dataset_name: The dataset containing the column that the feature will use.
        :param str column: The column that the feature will use.
        :param str name: what the feature will be called.
        :param str hierarchy: what hierarchy to add the attribute to.
        :param str level: what level of the hierarchy the attribute should be added to.
        :param str description: the description for the feature. Defaults to ''.
        :param str caption: the caption for the feature. Defaults to ''.
        :param str folder: the folder to put the feature in. Defaults to ''.
        :param bool publish: Whether or not the updated project should be published. Defaults to True.
        """
        self.refresh_project()
        
        self._check_single_column(dataset_name, column)
        
        if caption == '':
            caption = name
            
        project_json = self.project_json
        cube = [x for x in project_json['cubes']['cube'] if x['name'] == self.model_name][0]

        attribute_id = str(uuid.uuid4())
        ref_id = str(uuid.uuid4())
        
        degen = True
        if 'attributes' in project_json and 'keyed-attribute' in project_json['attributes']:
            for attr in project_json['attributes']['keyed-attribute']:
                if attr['name'] == level:
                    level_id = attr['id']
                    degen = False
                    break
        if 'attributes' in cube and 'keyed-attribute' in cube['attributes']:
            for attr in cube['attributes']['keyed-attribute']:
                if attr['name'] == level:
                    level_id = attr['id']
                    break
                    
        new_attribute = {
            'attribute-id': attribute_id,
            'properties': {
                'multiplicity': {}
            }
        }
        
        if degen:
            if 'dimensions' in cube and 'dimension' in cube['dimensions']:
                for dimension in cube['dimensions']['dimension']:
                    if 'hierarchy' in dimension:
                        for hier in dimension['hierarchy']:
                            if hier['name'] == hierarchy:
                                if 'level' in hier:
                                    for l in hier['level']:
                                        if l['primary-attribute'] == level_id:
                                            if 'keyed-attribute-ref' not in l:
                                                l['keyed-attribute-ref'] = []
                                            l['keyed-attribute-ref'].append(new_attribute)
        else:
            if 'dimensions' in project_json and 'dimension' in project_json['dimensions']:
                for dimension in project_json['dimensions']['dimension']:
                    if 'hierarchy' in dimension:
                        for hier in dimension['hierarchy']:
                            if hier['name'] == hierarchy:
                                if 'level' in hier:
                                    for l in hier['level']:
                                        if l['primary-attribute'] == level_id:
                                            if 'keyed-attribute-ref' not in l:
                                                l['keyed-attribute-ref'] = []
                                            l['keyed-attribute-ref'].append(new_attribute)
                                            
        new_ref = {
            'column': [
                column
            ],
            'complete': True,
            'id': attribute_id
        }
        data_set = [x for x in project_json['datasets']['data-set'] if x['name'] == dataset_name][0]
        data_set_id = data_set['id']
        data_set['logical']['attribute-ref'].append(new_ref)
        new_keyed_attribute = {
            'id': attribute_id,
            'key-ref': ref_id,
            'name': name,
            'properties': {
                'folder': folder,
                'description': description,
                'caption': caption,
                'type': {
                    'enum': {

                    }
                },
                'visible': True
            }
        }
        if 'keyed-attribute' not in project_json['attributes']:
            project_json['attributes']['keyed-attribute'] = []
        project_json['attributes']['keyed-attribute'].append(new_keyed_attribute)
        new_attribute_key = {
            'id': ref_id,
            'properties': {
                'columns': 1,
                'visible': True
            }
        }
        if 'attribute-key' not in project_json['attributes']:
            project_json['attributes']['attribute-key'] = []
        project_json['attributes']['attribute-key'].append(new_attribute_key)
        new_key_ref = {
            'column': [
                column
            ],
            'complete': 'true',
            'id': ref_id,
            'unique': False
        }
        data_set['logical']['key-ref'].append(new_key_ref)

        self._update_project(project_json, publish)
    
    def update_secondary_attribute_metadata(self, name, description=None, caption=None, folder=None, publish=True):
        """ Updates the metadata for a secondary attribute.

        :param str name: The name of the feature to update.
        :param str description: The description for the feature. Defaults to None to leave unchanged.
        :param str caption: The caption for the feature. Defaults to None to leave unchanged.
        :param str folder: The folder to put the feature in. Defaults to None to leave unchanged.
        :param bool publish: Whether or not the updated project should be published. Defaults to True.
        """
        self.refresh_project()
            
        project_json = self.project_json
            
        attributes = [x for x in project_json['attributes']['keyed-attribute'] if x['name'] == name]
        if len(attributes) < 1:
            raise Exception(f'Secondary Attribute: {name} does not exist.')
        attribute = attributes[0]
        if description is not None:
            attribute['properties']['description'] = description
        if caption is not None:
            attribute['properties']['caption'] = caption
        if folder is not None:
            attribute['properties']['folder'] = folder

        self._update_project(project_json, publish)

    def _create_rolling_helper(self, prefix, name, numeric_feature, length, time_hierarchy, level, description,
                               caption, folder, format_string, publish):
        """ Factors out common code from several of the following functions that create calculated features.

        :param str prefix: The prefix to the query specifying what sort of feature is being created.
        :param str name: What the feature will be called.
        :param str numeric_feature: The numeric feature to use for the calculation.
        :param int length: The length the feature should be calculated over.
        :param str time_hierarchy: The time hierarchy used in the calculation.
        :param str level: The level within the time hierarchy
        :param str description: The description for the feature. Defaults to ''.
        :param str caption: The caption for the feature. Defaults to ''.
        :param str folder: The folder to put the feature in. Defaults to ''.
        :param str format_string: The format string for the feature. Defaults to 'General Number'.
        :param bool publish: Whether or not the updated project should be published. Defaults to True.
        """
        self._check_single_element(numeric_feature, self.list_all_numeric_features(),
                                   f'Make sure \'{numeric_feature}\' is a numeric feature')

        if not (type(length) == int) or length < 0:
            raise UserError(f'Make sure Argument: \'{length}\' is an integer greater than zero')

        self._check_time_hierarchy(time_hierarchy, level=level)

        time_dimension = self._hierarchy_dimension(time_hierarchy)

        expression = prefix + f'(' \
                              f'ParallelPeriod([{time_dimension}].[{time_hierarchy}].[{level}]' \
                              f', {length - 1}, [{time_dimension}].[{time_hierarchy}].CurrentMember)' \
                              f':[{time_dimension}].[{time_hierarchy}].CurrentMember, [Measures].[{numeric_feature}])'
        self.create_calculated_feature(name, expression, description=description, caption=caption, folder=folder,
                                       format_string=format_string, publish=publish)

    def create_rolling_mean(self, name, numeric_feature, length, time_hierarchy, level, description='',
                            caption='', folder='', format_string='General Number', publish=True):
        """ Creates a rolling mean calculated numeric feature.

        :param str name: What the feature will be called.
        :param str numeric_feature: The numeric feature to use for the calculation.
        :param int length: The length the mean should be calculated over.
        :param str time_hierarchy: The time hierarchy used in the calculation.
        :param str level: The level within the time hierarchy
        :param str description: The description for the feature. Defaults to ''.
        :param str caption: The caption for the feature. Defaults to ''.
        :param str folder: The folder to put the feature in. Defaults to ''.
        :param str format_string: The format string for the feature. Defaults to 'General Number'.
        :param bool publish: Whether or not the updated project should be published. Defaults to True.
        """
        self._create_rolling_helper('Avg', name, numeric_feature, length, time_hierarchy, level, description,
                                    caption, folder, format_string, publish)

    def create_rolling_sum(self, name, numeric_feature, length, time_hierarchy, level, description='',
                           caption='', folder='', format_string='General Number', publish=True):
        """ Creates a rolling sum calculated numeric feature.

        :param str name: What the feature will be called.
        :param str numeric_feature: The numeric feature to use for the calculation.
        :param int length: The length the sum should be calculated over.
        :param str time_hierarchy: The time hierarchy used in the calculation.
        :param str level: The level within the time hierarchy
        :param str description: The description for the feature. Defaults to ''.
        :param str caption: The caption for the feature. Defaults to ''.
        :param str folder: The folder to put the feature in. Defaults to ''.
        :param str format_string: The format string for the feature. Defaults to 'General Number'.
        :param bool publish: Whether or not the updated project should be published. Defaults to True.
        """
        self._create_rolling_helper('Sum', name, numeric_feature, length, time_hierarchy, level, description,
                                    caption, folder, format_string, publish)

    def create_rolling_max(self, name, numeric_feature, length, time_hierarchy, level, description='',
                           caption='', folder='', format_string='General Number', publish=True):
        """ Creates a rolling max calculated numeric feature.

        :param str name: What the feature will be called.
        :param str numeric_feature: The numeric feature to use for the calculation.
        :param int length: The length the max should be calculated over.
        :param str time_hierarchy: The time hierarchy used in the calculation.
        :param str level: The level within the time hierarchy
        :param str description: The description for the feature. Defaults to ''.
        :param str caption: The caption for the feature. Defaults to ''.
        :param str folder: The folder to put the feature in. Defaults to ''.
        :param str format_string: The format string for the feature. Defaults to 'General Number'.
        :param bool publish: Whether or not the updated project should be published. Defaults to True.
        """
        self._create_rolling_helper('Max', name, numeric_feature, length, time_hierarchy, level, description,
                                    caption, folder, format_string, publish)

    def create_rolling_min(self, name, numeric_feature, length, time_hierarchy, level, description='',
                           caption='', folder='', format_string='General Number', publish=True):
        """ Creates a rolling min calculated numeric feature.

        :param str name: What the feature will be called.
        :param str numeric_feature: The numeric feature to use for the calculation.
        :param int length: The length the min should be calculated over.
        :param str time_hierarchy: The time hierarchy used in the calculation.
        :param str level: The level within the time hierarchy
        :param str description: The description for the feature. Defaults to ''.
        :param str caption: The caption for the feature. Defaults to ''.
        :param str folder: The folder to put the feature in. Defaults to ''.
        :param str format_string: The format string for the feature. Defaults to 'General Number'.
        :param bool publish: Whether or not the updated project should be published. Defaults to True.
        """
        self._create_rolling_helper('Min', name, numeric_feature, length, time_hierarchy, level, description,
                                    caption, folder, format_string, publish)

    def create_rolling_stdev(self, name, numeric_feature, length, time_hierarchy, level, description='',
                             caption='', folder='', format_string='General Number', publish=True):
        """ Creates a rolling standard deviation calculated numeric feature.

         :param str name: What the feature will be called.
         :param str numeric_feature: The numeric feature to use for the calculation.
         :param int length: The length the standard deviation should be calculated over.
         :param str time_hierarchy: The time hierarchy used in the calculation.
         :param str level: The level within the time hierarchy
         :param str description: The description for the feature. Defaults to ''.
         :param str caption: The caption for the feature. Defaults to ''.
         :param str folder: The folder to put the feature in. Defaults to ''.
         :param str format_string: The format string for the feature. Defaults to 'General Number'.
         :param bool publish: Whether or not the updated project should be published. Defaults to True.
         """
        self._create_rolling_helper('Stdev', name, numeric_feature, length, time_hierarchy, level, description,
                                    caption, folder, format_string, publish)

    def create_lag(self, name, numeric_feature, length, time_hierarchy, level, description='', caption='',
                   folder='', format_string='General Number', publish=True):
        """ Creates a lag calculated numeric feature.

        :param str name: What the feature will be called.
        :param str numeric_feature: The numeric feature to use for the calculation.
        :param int length: The length of the lag.
        :param str time_hierarchy: The time hierarchy used in the calculation.
        :param str level: The level within the time hierarchy
        :param str description: The description for the feature. Defaults to ''.
        :param str caption: The caption for the feature. Defaults to ''.
        :param str folder: The folder to put the feature in. Defaults to ''.
        :param str format_string: The format string for the feature. Defaults to 'General Number'.
        :param bool publish: Whether or not the updated project should be published. Defaults to True.
        """
        self._check_single_element(numeric_feature, self.list_all_numeric_features(),
                                   f'Make sure \'{numeric_feature}\' is a numeric feature')

        if not (type(length) == int) or length <= 0:
            raise UserError(f'Make sure Argument: \'{length}\' is an integer greater than zero')

        self._check_time_hierarchy(time_hierarchy, level=level)

        time_dimension = self._hierarchy_dimension(time_hierarchy)

        expression = f'(ParallelPeriod([{time_dimension}].[{time_hierarchy}].[{level}], {length}' \
                     f', [{time_dimension}].[{time_hierarchy}].CurrentMember),[Measures].[{numeric_feature}])'

        self.create_calculated_feature(name, expression, description=description, caption=caption, folder=folder,
                                       format_string=format_string, publish=publish)

    def create_rolling_stats(self, numeric_features, time_hierarchy, level, intervals=None,
                             description='', caption='', folder='', format_string='General Number', publish=True):
        """ Creates a rolling min, max, mean, sum, stddev, and lag of numeric features.

         :param str lst numeric_features: The numeric features to use for the calculation.
         :param str time_hierarchy: The hierarchy that the time level belongs to.
         :param str level: The time level to use for the calculation.
         :param int lst intervals: Custom list of intervals to create features over. Defaults to None to use default intervals based off of level time step
         :param str description: The description for the feature. Defaults to ''.
         :param str caption: The caption for the feature. Defaults to ''.
         :param str folder: The folder to put the feature in. Defaults to ''.
         :param str format_string: The format string for the feature. Defaults to 'General Number'.
         :param bool publish: Whether or not the updated project should be published. Defaults to True.
         :return: A list of the new features created.
         :rtype: list of str
         """
        self._check_time_hierarchy(time_hierarchy, level=level)
        
        if type(numeric_features) != list:
            numeric_features = [numeric_features]

        self._check_multiple_features(numeric_features, self.list_all_numeric_features(),
                                      errmsg='Make sure all items in numeric_features are'
                                             'numeric features')

        time_numeric = self._get_hierarchy_level_time_step(time_hierarchy, level)
        time_name = str(time_numeric)[4:-1].lower()  # takes out the Time and 's' at the end and in lowercase

        if intervals:
            if type(intervals) != list:
                intervals = [intervals]
        else:
            intervals = self._time_steps[time_numeric]

        name_list = []
        for feature in numeric_features:
            for interval in intervals:
                interval = int(interval)
                name = feature + f'_{interval}_{time_name}_'
                if interval > 1:
                    self.create_rolling_min(f'{name}min', feature, interval, time_hierarchy, level, description, caption, folder, format_string, False)
                    name_list.append(f'{name}min')

                    self.create_rolling_max(f'{name}max', feature, interval, time_hierarchy, level, description, caption, folder, format_string, False)
                    name_list.append(f'{name}max')

                    self.create_rolling_mean(f'{name}avg', feature, interval, time_hierarchy, level, description, caption, folder, format_string, False)
                    name_list.append(f'{name}avg')

                    self.create_rolling_sum(f'{name}sum', feature, interval, time_hierarchy, level, description, caption, folder, format_string, False)
                    name_list.append(f'{name}sum')

                    self.create_rolling_stdev(f'{name}stddev', feature, interval, time_hierarchy, level, description, caption, folder, format_string, False)
                    name_list.append(f'{name}stddev')

                self.create_lag(f'{name}lag', feature, interval, time_hierarchy, level, description, caption, folder, format_string, False)
                name_list.append(f'{name}lag')
        if publish:
            self._update_project(self.project_json, publish=True)
        return name_list
    
    def create_diff(self, name, numeric_feature, length, time_hierarchy, level, description='', caption='', folder='',
                    format_string='General Number', publish=True):
        """ Creates a time over time calculation.

        :param str name: What the feature will be called.
        :param str numeric_feature: The numeric feature to use for the calculation.
        :param int length: The length of the lag.
        :param str time_hierarchy: The time hierarchy used in the calculation.
        :param str level: The level within the time hierarchy
        :param str description: The description for the feature. Defaults to ''.
        :param str caption: The caption for the feature. Defaults to ''.
        :param str folder: The folder to put the feature in. Defaults to ''.
        :param str format_string: The format string for the feature. Defaults to 'General Number'.
        :param bool publish: Whether or not the updated project should be published. Defaults to True.
        """

        self._check_single_element(numeric_feature, self.list_all_numeric_features(),
                                   f'Make sure Argument: \'{numeric_feature}\' is a numeric feature')
                                   
        if not (type(length) == int) or length < 0:
            raise UserError(f'Make sure Argument: \'{length}\' is an integer greater than zero')

        self._check_time_hierarchy(time_hierarchy, level=level)

        time_dimension = self._hierarchy_dimension(time_hierarchy)

        expression = f'CASE WHEN IsEmpty((ParallelPeriod([{time_dimension}].[{time_hierarchy}].[{level}], {length}' \
                     f', [{time_dimension}].[{time_hierarchy}].CurrentMember), [Measures].[{numeric_feature}])) ' \
                     f'THEN 0 ELSE ([Measures].[{numeric_feature}]' \
                     f'-(ParallelPeriod([{time_dimension}].[{time_hierarchy}].[{level}], {length}' \
                     f', [{time_dimension}].[{time_hierarchy}].CurrentMember), [Measures].[{numeric_feature}])) END'
        self.create_calculated_feature(name, expression, description=description, caption=caption, folder=folder,
                                       format_string=format_string, publish=publish)
    
    def create_percent_change(self, name, numeric_feature, length, time_hierarchy, level, description='', caption='', folder='',
                              format_string='General Number', publish=True):
        """ Creates a time over time calculation.

        :param str name: What the feature will be called.
        :param str numeric_feature: The numeric feature to use for the calculation.
        :param int length: The length of the lag.
        :param str time_hierarchy: The time hierarchy used in the calculation.
        :param str level: The level within the time hierarchy
        :param str description: The description for the feature. Defaults to ''.
        :param str caption: The caption for the feature. Defaults to ''.
        :param str folder: The folder to put the feature in. Defaults to ''.
        :param str format_string: The format string for the feature. Defaults to 'General Number'.
        :param bool publish: Whether or not the updated project should be published. Defaults to True.
        """

        self._check_single_element(numeric_feature, self.list_all_numeric_features(),
                                   f'Make sure Argument: \'{numeric_feature}\' is a numeric feature')
                                   
        if not (type(length) == int) or length < 0:
            raise UserError(f'Make sure Argument: \'{length}\' is an integer greater than zero')

        self._check_time_hierarchy(time_hierarchy, level=level)

        time_dimension = self._hierarchy_dimension(time_hierarchy)

        expression = f'CASE WHEN IsEmpty((ParallelPeriod([{time_dimension}].[{time_hierarchy}].[{level}], {length}' \
                     f', [{time_dimension}].[{time_hierarchy}].CurrentMember), [Measures].[{numeric_feature}])) ' \
                     f'THEN 0 ELSE ([Measures].[{numeric_feature}]' \
                     f'/(ParallelPeriod([{time_dimension}].[{time_hierarchy}].[{level}], {length}' \
                     f', [{time_dimension}].[{time_hierarchy}].CurrentMember), [Measures].[{numeric_feature}]) - 1) END'
        self.create_calculated_feature(name, expression, description=description, caption=caption, folder=folder,
                                       format_string=format_string, publish=publish)
                
    def create_period_to_date(self, name, numeric_feature, time_hierarchy, level, description='', caption='',
                              folder='', format_string='General Number', publish=True):
        """ Creates a period-to-date calculation.
        
        :param str name: What the feature will be called.
        :param str numeric_feature: The numeric feature to use for the calculation.
        :param str time_hierarchy: The time hierarchy used in the calculation.
        :param str level: The level within the time hierarchy
        :param str description: The description for the feature. Defaults to ''.
        :param str caption: The caption for the feature. Defaults to ''.
        :param str folder: The folder to put the feature in. Defaults to ''.
        :param str format_string: The format string for the feature. Defaults to 'General Number'.
        :param bool publish: Whether or not the updated project should be published. Defaults to True.
        """
        self._check_time_hierarchy(time_hierarchy, level=level)

        time_dimension = self._hierarchy_dimension(time_hierarchy)

        expression = f'CASE WHEN IsEmpty([Measures].[{numeric_feature}]) THEN NULL ELSE ' \
                     f'Sum(PeriodsToDate([{time_dimension}].[{time_hierarchy}].[{level}], ' \
                     f'[{time_dimension}].[{time_hierarchy}].CurrentMember), [Measures].[{numeric_feature}]) END'
        self.create_calculated_feature(name, expression, description=description, caption=caption,
                                       folder=folder, format_string=format_string, publish=publish)

    def create_periods_to_date(self, numeric_feature, time_hierarchy, description='', caption='',
                               folder='', format_string='General Number', publish=True):
        """ Creates a period-to-date calculation.

        :param str numeric_feature: The numeric feature to use for the calculation.
        :param str time_hierarchy: The time hierarchy used in the calculation.
        :param str description: The description for the feature. Defaults to ''.
        :param str caption: The caption for the feature. Defaults to ''.
        :param str folder: The folder to put the feature in. Defaults to ''.
        :param str format_string: The format string for the feature. Defaults to 'General Number'.
        :param bool publish: Whether or not the updated project should be published. Defaults to True.
        """
        self._check_time_hierarchy(time_hierarchy)

        base = self.list_hierarchy_levels(time_hierarchy)[-1]
        for level in self.list_hierarchy_levels(time_hierarchy):
            if level != base:
                name = f'{numeric_feature}_{level.capitalize()}_To_{base.capitalize()}'
                self.create_period_to_date(name, numeric_feature, time_hierarchy, level, description=description,
                                           caption=caption, folder=folder, format_string=format_string, publish=False)
        if publish:
            self._update_project(self.project_json, publish=True)
                                           
    def create_percentage(self, numeric_feature, hierarchy, description='', caption='',
                          folder='', format_string='General Number', publish=True):
        """ Creates a percentage comparing figures at one level of a hierarchy to the levels above it.

        :param str numeric_feature: The numeric feature to use for the calculation.
        :param str hierarchy: The hierarchy that the feature belongs to.
        :param str description: The description for the feature. Defaults to ''.
        :param str caption: The caption for the feature. Defaults to ''.
        :param str folder: The folder to put the feature in. Defaults to ''.
        :param str format_string: The format string for the feature. Defaults to 'General Number'.
        :param bool publish: Whether or not the updated project should be published. Defaults to True.
        """
        if hierarchy not in self.list_all_hierarchies():
            raise UserError(f'Hierarchy: \'{hierarchy}\' not in model.'
                            f' Make sure the model has been published and it is correctly spelled')

        dimension_name = self._hierarchy_dimension(hierarchy)

        name_list = self.list_hierarchy_levels(hierarchy)

        self._check_single_element(numeric_feature, self.list_all_numeric_features(),
                                   f'Make sure Argument: \'{numeric_feature}\' is a numeric feature')

        for lower in range(len(name_list)):
            for higher in range(len(name_list)):
                if higher > lower:
                    diff = higher - lower
                    current = 'CurrentMember'
                    for _ in range(diff):
                        current += '.Parent'
                    name_diff = f'{name_list[lower]}%{name_list[higher]}'

                    expression = f'IIF( ([Measures].[{numeric_feature}], [{dimension_name}].[{hierarchy}].{current})' \
                                 f' = 0, NULL, [Measures].[{numeric_feature}] / ([Measures].[{numeric_feature}]' \
                                 f', [{dimension_name}].[{hierarchy}].{current}) )'
                    self.create_calculated_feature(name_diff, expression, description=description, caption=caption,
                                                   folder=folder, format_string=format_string, publish=False)
        if publish:
            self._update_project(self.project_json, publish=True)
            
    def create_minmax_scaled_feature(self, numeric_feature, name, min, max, feature_min=0, feature_max=1, description='', caption='', folder='',
                                 format_string='General Number', publish=True):
        """ Creates a new feature that is minmax scaled.
        
        :param str numeric_feature: The name of the feature to scale.
        :param str name: The name of the new feature.
        :param float min: The min from the base feature.
        :param float max: The max from the base feature.
        :param float feature_min: The min for the scaled feature.
        :param float feature_max: The max for the scaled feature.
        :param str description: The description for the feature. Defaults to ''.
        :param str caption: The caption for the feature. Defaults to ''.
        :param str folder: The folder to put the feature in. Defaults to ''.
        :param str format_string: The format string for the feature. Defaults to 'General Number'.
        :param bool publish: Whether or not the updated project should be published. Defaults to True.
        """
        expression = f'(([Measures].[{numeric_feature}] - {min})/({max}-{min}))*({feature_max}-{feature_min}) ' \
                     f'+{feature_min}'
        
        self.create_calculated_feature(name, expression, description=description, caption=caption, folder=folder, format_string=format_string, publish=publish)
        
    def create_standard_scaled_feature(self, numeric_feature, name, mean=0, standard_deviation=1, description='', caption='', folder='',
                                 format_string='General Number', publish=True):
        """ Creates a new feature that is standard scaled.
        
        :param str numeric_feature: The name of the feature to scale.
        :param str name: The name of the new feature.
        :param float mean: The mean from the base feature.
        :param float standard_deviation: The standard deviation from the base feature.
        :param str description: The description for the feature. Defaults to ''.
        :param str caption: The caption for the feature. Defaults to ''.
        :param str folder: The folder to put the feature in. Defaults to ''.
        :param str format_string: The format string for the feature. Defaults to 'General Number'.
        :param bool publish: Whether or not the updated project should be published. Defaults to True.
        """
        expression = f'([Measures].[{numeric_feature}] - {mean}) / {standard_deviation}'
        
        self.create_calculated_feature(name, expression, description=description, caption=caption, folder=folder, format_string=format_string, publish=publish)

    def create_maxabs_scaled_feature(self, numeric_feature, name, max_abs, description='', caption='', folder='',
                                 format_string='General Number', publish=True):
        """ Creates a new feature that is maxabs scaled.
        
        :param str numeric_feature: The name of the feature to scale.
        :param str name: The name of the new feature.
        :param float max_abs: The max absolute from the base feature.
        :param str description: The description for the feature. Defaults to ''.
        :param str caption: The caption for the feature. Defaults to ''.
        :param str folder: The folder to put the feature in. Defaults to ''.
        :param str format_string: The format string for the feature. Defaults to 'General Number'.
        :param bool publish: Whether or not the updated project should be published. Defaults to True.
        """
        max_abs = abs(max_abs)
        expression = f'[Measures].[{numeric_feature}] / {max_abs}'
        
        self.create_calculated_feature(name, expression, description=description, caption=caption, folder=folder, format_string=format_string, publish=publish)

    def create_robust_scaled_feature(self, numeric_feature, name, median=0, interquartile_range=1, description='', caption='', folder='',
                                 format_string='General Number', publish=True):
        """ Creates a new feature that is robust scaled.
        
        :param str numeric_feature: The name of the feature to scale.
        :param str name: The name of the new feature.
        :param float median: The median from the base feature.
        :param float interquartile_range: The interquartile range from the base feature.
        :param str description: The description for the feature. Defaults to ''.
        :param str caption: The caption for the feature. Defaults to ''.
        :param str folder: The folder to put the feature in. Defaults to ''.
        :param str format_string: The format string for the feature. Defaults to 'General Number'.
        :param bool publish: Whether or not the updated project should be published. Defaults to True.
        """
        expression = f'([Measures].[{numeric_feature}] - {median}) / {interquartile_range}'
        
        self.create_calculated_feature(name, expression, description=description, caption=caption, folder=folder, format_string=format_string, publish=publish)

    def create_log_transformed_feature(self, numeric_feature, name, description='', caption='', folder='',
                                 format_string='General Number', publish=True):
        """ Creates a new feature that is log transformed.
        
        :param str numeric_feature: The name of the feature to scale.
        :param str name: The name of the new feature.
        :param str description: The description for the feature. Defaults to ''.
        :param str caption: The caption for the feature. Defaults to ''.
        :param str folder: The folder to put the feature in. Defaults to ''.
        :param str format_string: The format string for the feature. Defaults to 'General Number'.
        :param bool publish: Whether or not the updated project should be published. Defaults to True.
        """
        expression = f'log([Measures].[{numeric_feature}])'
        
        self.create_calculated_feature(name, expression, description=description, caption=caption, folder=folder, format_string=format_string, publish=publish)
        
    def create_unit_vector_normalized_feature(self, numeric_feature, name, magnitude, description='', caption='', folder='',
                             format_string='General Number', publish=True):
        """ Creates a new feature that is unit vector normalized.
        
        :param str numeric_feature: The name of the feature to scale.
        :param str name: The name of the new feature.
        :param float magnitude: The magnitude of the base feature.
        :param str description: The description for the feature. Defaults to ''.
        :param str caption: The caption for the feature. Defaults to ''.
        :param str folder: The folder to put the feature in. Defaults to ''.
        :param str format_string: The format string for the feature. Defaults to 'General Number'.
        :param bool publish: Whether or not the updated project should be published. Defaults to True.
        """
        expression = f'[Measures].[{numeric_feature}]/{magnitude}'
        
        self.create_calculated_feature(name, expression, description=description, caption=caption, folder=folder, format_string=format_string, publish=publish)

    def create_power_transformed_feature(self, numeric_feature, name, power, method='yeo-johnson', description='', caption='', folder='',
                                 format_string='General Number', publish=True):
        """ Creates a new feature that is power transformed.
        
        :param str numeric_feature: The name of the feature to scale.
        :param str name: The name of the new feature.
        :param float power: The name of the new feature.
        :param str method: Which method to use. Valid values are yeo-johnson and box-cox.
        :param str description: The description for the feature. Defaults to ''.
        :param str caption: The caption for the feature. Defaults to ''.
        :param str folder: The folder to put the feature in. Defaults to ''.
        :param str format_string: The format string for the feature. Defaults to 'General Number'.
        :param bool publish: Whether or not the updated project should be published. Defaults to True.
        """
        if method.lower() == 'yeo-johnson':
            if power == 0:
                expression = f'IIF([Measures].[{numeric_feature}]<0' \
                             f', (-1*((((-1*[Measures].[{numeric_feature}])+1)^(2-{power}))-1))/(2-{power})' \
                             f', log([Measures].[{numeric_feature}]+1))'
            elif power == 2:
                expression = f'IIF([Measures].[{numeric_feature}]<0,(-1*log((-1*[Measures].[{numeric_feature}])+1))' \
                             f', ((([Measures].[{numeric_feature}]+1)^{power})-1)/{power})'
            else:
                expression = f'IIF([Measures].[{numeric_feature}]<0' \
                             f', (-1*((((-1*[Measures].[{numeric_feature}])+1)^(2-{power}))-1))/(2-{power})' \
                             f', ((([Measures].[{numeric_feature}]+1)^{power})-1)/{power})'
        elif method.lower() == 'box-cox':
            if power == 0:
                expression = f'log([Measures].[{numeric_feature}])'
            else:
                expression = f'(([Measures].[{numeric_feature}]^{power})-1)/{power}'
        else:
            raise Exception('Invalid type: Valid values are yeo-johnson and box-cox')
        
        self.create_calculated_feature(name, expression, description=description, caption=caption, folder=folder, format_string=format_string, publish=publish)
        
    def create_binned_feature(self, numeric_feature, name, bin_edges, description='', caption='', folder='',
                             format_string='General Number', publish=True):
        """ Creates a new feature that is binned.
        
        :param str numeric_feature: The name of the feature to scale.
        :param str name: The name of the new feature.
        :param list float bin_edges: The edges to use to compute the bins. Left inclusive
        :param str description: The description for the feature. Defaults to ''.
        :param str caption: The caption for the feature. Defaults to ''.
        :param str folder: The folder to put the feature in. Defaults to ''.
        :param str format_string: The format string for the feature. Defaults to 'General Number'.
        :param bool publish: Whether or not the updated project should be published. Defaults to True.
        """
        expression = f'CASE [Measures].[{numeric_feature}]'
        bin = 0
        for edge in bin_edges:
            expression += f' WHEN [Measures].[{numeric_feature}] < {edge} THEN {bin}'
            bin += 1
        expression += f' ELSE {bin} END'
        
        self.create_calculated_feature(name, expression, description=description, caption=caption, folder=folder, format_string=format_string, publish=publish)

    def generate_time_series_features(self, dataframe, numeric_features, time_hierarchy, level,  group_features=None, intervals=None, shift_amount=0):
        """ Adds calculated measures to a pandas.DataFrame.

        :param pandas.DataFrame dataframe: The DataFrame to be changed.
        :param str lst numeric_features: The numeric features to use for the calculation.
        :param str time_hierarchy: The hierarchy that the level belongs to.
        :param str level: The level within the time hierarchy.
        :param str lst group_features: The features to be grouped by. Defaults to None.
        :param int lst intervals: Custom list of intervals to create features over. Defaults to None to use default intervals based off of level time step
        :param int shift_amount: Allows the user to specify the lag used to generate the calculated measures. Defaults to 0.
        :return: The changed pandas.DataFrame.
        :rtype: pandas.DataFrame
        """
        self._check_time_hierarchy(time_hierarchy, level=level)

        if group_features:
            if type(group_features) != list:
                group_features = [group_features]
            self._check_multiple_features(group_features, self.list_all_features())

        if type(numeric_features) != list:
            numeric_features = [numeric_features]
        self._check_multiple_features(numeric_features, self.list_all_numeric_features(),
                                      errmsg='Make sure all items in numeric_features are numeric features')

        time_numeric = self._get_hierarchy_level_time_step(time_hierarchy, level)
        time_name = str(time_numeric)[4:-1].lower()  # takes out the Time and 's' at the end and in lowercase
        
        if intervals:
            if type(intervals) != list:
                intervals = [intervals]
        else:
            intervals = self._time_steps[time_numeric]

        levels = [x for x in self.list_hierarchy_levels(time_hierarchy) if x in dataframe.columns]

        if group_features:
            dataframe = dataframe.sort_values(by=group_features + levels).reset_index(drop=True)
        else:
            dataframe = dataframe.sort_values(by=levels).reset_index(drop=True)
            
        for feature in numeric_features:
            for interval in intervals:
                interval = int(interval)
                name = feature + f'_{interval}_{time_name}_'
                if group_features:
                    if interval > 1:
                        dataframe[f'{name}sum'] = dataframe.groupby(group_features)[feature].rolling(interval).sum().shift(shift_amount).reset_index(drop=True)

                        dataframe[f'{name}avg'] = dataframe.groupby(group_features)[feature].rolling(interval).mean().shift(shift_amount).reset_index(drop=True)

                        dataframe[f'{name}stddev'] = dataframe.groupby(group_features)[feature].rolling(interval).std().shift(shift_amount).reset_index(drop=True)

                        dataframe[f'{name}min'] = dataframe.groupby(group_features)[feature].rolling(interval).min().shift(shift_amount).reset_index(drop=True)

                        dataframe[f'{name}max'] = dataframe.groupby(group_features)[feature].rolling(interval).max().shift(shift_amount).reset_index(drop=True)

                    dataframe[f'{name}lag'] = dataframe.groupby(group_features)[feature].shift(interval).reset_index(drop=True)
                else:
                    if interval > 1:
                        dataframe[f'{name}sum'] = dataframe[feature].rolling(interval).sum().shift(shift_amount).reset_index(drop=True)

                        dataframe[f'{name}avg'] = dataframe[feature].rolling(interval).mean().shift(shift_amount).reset_index(drop=True)

                        dataframe[f'{name}stddev'] = dataframe[feature].rolling(interval).std().shift(shift_amount).reset_index(drop=True)

                        dataframe[f'{name}min'] = dataframe[feature].rolling(interval).min().shift(shift_amount).reset_index(drop=True)

                        dataframe[f'{name}max'] = dataframe[feature].rolling(interval).max().shift(shift_amount).reset_index(drop=True)

                    dataframe[f'{name}lag'] = dataframe[feature].shift(interval).shift(shift_amount).reset_index(drop=True)
                    
            found = False
            for heir_level in reversed(levels):
                if found and heir_level in dataframe.columns:
                    name = f'{feature}_{heir_level}_to_date'
                    if group_features:
                        dataframe[name] = dataframe.groupby(group_features+[heir_level])[feature].cumsum().shift(1).reset_index(drop=True)
                    else:
                        dataframe[name] = dataframe.groupby([heir_level])[feature].cumsum().shift(1).reset_index(drop=True)
                if heir_level == level:
                    found = True

        return dataframe

    # Connecting to Databases

    def create_db_connection(self, db: Database):
        """ Links the given Database object to this AtScale project"""
        self._check_single_connection(db.get_atscale_connection_id())

        self.database = db

    def add_table(self, table_name, dataframe, join_features, join_columns=None, chunksize=None, publish=True):
        """ Creates a table, inserts a DataFrame into the table, and then joins the table to the cube.

        :param str table_name: What the table should be named.
        :param pandas.DataFrame dataframe: The DataFrame to upload to the table.
        :param list of str join_features: The features that join to the cube dimensions.
        :param list of str join_columns: The columns in the dataframe to join to the join_features.
        List must be either None or the same length and order as join_features. Defaults to None to use identical
        names to the join_features
        :param int chunksize: the number of rows to insert at a time. Defaults to 10,000.
        :param bool publish: Whether or not the updated project should be published. Defaults to True.
        """
        if not isinstance(self.database, Database):
            raise Exception(
                'No database connection set up. Please add a connection using one of'
                ' the create_db_connection functions.')
        
        if join_columns is None:
            join_columns = join_features
            
        if len(join_features) != len(join_columns):
            raise Exception(f'join_features and join_columns lengths must match. join_features is'
                            f' length {len(join_features)} while join_columns is length {len(join_columns)}')

        self._check_multiple_features(join_features, self.list_all_categorical_features(),
                                      errmsg='Make sure all items in join_features are categorical features')

        self._check_multiple_features(join_columns, dataframe.columns,
                                      errmsg='Make sure all items in join_features are in the dataframe')

        self.database.add_table(table_name, dataframe, chunksize, 'fail')
        # TO-DO: if write_df_to_db is deprecated, check if chunksize is None and don't pass None so default
        # is used

        table_name = self.database.fix_table_name(table_name=table_name)

        self.join_table(table_name, join_features, join_columns = join_columns, publish=publish)

    def join_table(self, table_name, join_features, join_columns=None,
                   connection_id='', database='', schema='', publish=True):
        """ Joins the table to the model.

        :param str table_name: What the table should be named.
        :param list of str join_features: The features in the model to use for the joins.
        :param list of str join_columns: The columns in the table to join to the join_features.
            List must be either None or the same length and order as join_features. Defaults to None to use
            identical names to the join_features
        :param str connection_id: The connection name for the warehouse in AtScale.
        Defaults to '' to use value set by creating a db connection
        :param str database: The database name. Defaults to '' to use value set by creating a db connection
        :param str schema: The database schema. Defaults to '' to use value set by creating a db connection
        :param bool publish: Whether or not the updated project should be published. Defaults to True.
        """
        self.refresh_project()
        project_json = self.project_json
        
        if join_columns is None:
            join_columns = join_features
        
        if len(join_features) != len(join_columns):
            raise Exception(f'join_features and join_columns lengths must match. join_features is length '
                            f'{len(join_features)} while join_columns is length {len(join_columns)}')
        
        if not connection_id:
            if isinstance(self.database, Database):
                connection_id = self.database.get_atscale_connection_id()
        else:
            raise Exception(
                'No database connection set up. Either pass in a connection_id, schema, and database '
                'or add a connection using one of the create_db_connection functions.')
        if database == '':
            database = self.database.get_database_name()
        if schema == '':
            schema = self.database.get_schema()

        self._check_multiple_features(join_features, self.list_all_categorical_features(),
                                      errmsg='Make sure all items in join_features are categorical features')

        url = f'{self.server}:{self.engine_port}/data-sources/orgId/{self.organization}' \
              f'/conn/{connection_id}/tables/cacheRefresh'
        response = requests.post(url, data='', headers=self.headers)

        if response.status_code != 200:
            resp = json.loads(response.text)
            raise Exception(resp['response']['error'])
        url = f'{self.server}:{self.engine_port}/data-sources/orgId/{self.organization}' \
              f'/conn/{connection_id}/table/{table_name}/info'
        if database:
            url += f'?database={database}'
            if schema:
                url += f'&schema={schema}'
        elif schema:
            url += f'?schema={schema}'
        response = requests.get(url, headers=self.headers)

        if response.status_code != 200:
            resp = json.loads(response.text)
            raise Exception(resp['response']['error'])
        table_columns = [(x['name'], x['column-type']['data-type']) for x in
                         json.loads(response.content)['response']['columns']]

        columns = []
        for (name, d_type) in table_columns:
            column = {
                'id': str(uuid.uuid4()),
                'name': name,
                'type': {
                    'data-type': d_type
                }
            }
            columns.append(column)
        dataset_id = str(uuid.uuid4())
        dataset = {
            'id': dataset_id,
            'name': table_name,
            'properties': {
                'allow-aggregates': True,
                'aggregate-locality': None,
                'aggregate-destinations': None
            },
            'physical': {
                'connection': {
                    'id': connection_id
                },
                'tables': [{
                    'schema': schema,
                    'name': table_name
                }],
                'immutable': False,
                'columns': columns
            },
            'logical': {}
        }
        if database:
            dataset['physical']['tables'][0]['database'] = database
        project_json['datasets']['data-set'].append(dataset)

        cube = [x for x in project_json['cubes']['cube'] if x['name'] == self.model_name][0]

        key_refs = []
        attribute_refs = []

        column_names = []
        for (name, d_type) in table_columns:
            column_names.append(name)
        column_name = None
        
        joins = tuple(zip(join_features, join_columns))
        
        for join_feature, join_column in joins:
            if join_column in column_names:
                column_name = join_column
            elif join_column.upper() in column_names:
                column_name = join_column.upper()
            elif join_column.lower() in column_names:
                column_name = join_column.lower()
            
            if 'attributes' in project_json and 'keyed-attribute' in project_json['attributes']:
                dimension = [x for x in project_json['attributes']['keyed-attribute'] if x['name'] == join_feature]
                if dimension:
                    ref = dimension[0]['key-ref']
                    key_ref = {
                        'id': ref,
                        'unique': False,
                        'complete': 'false',
                        'column': [column_name]
                    }
                    key_refs.append(key_ref)
            if not dimension:
                if 'attributes' in cube and 'keyed-attribute' in cube['attributes']:
                    dimension = [x for x in cube['attributes']['keyed-attribute'] if x['name'] == join_feature]
                    if dimension:
                        ref = dimension[0]['key-ref']
                        key_ref = {
                            'id': ref,
                            'unique': False,
                            'complete': 'partial',
                            'column': [column_name]
                        }
                        key_refs.append(key_ref)
                        uid = dimension[0]['id']
                        attr = {
                            'id': uid,
                            'complete': 'partial',
                            'column': [column_name]
                        }
                        attribute_refs.append(attr)
        dataset = {
            'id': dataset_id,
            'properties': {
                'allow-aggregates': True,
                'create-hinted-aggregate': False,
                'aggregate-destinations': None
            },
            'logical': {
                'key-ref': key_refs,
                'attribute-ref': attribute_refs
            }
        }
        cube['data-sets']['data-set-ref'].append(dataset)

        self._update_project(project_json, publish)

    #could deprecate
    def write_dataframe_to_db(self, table_name, dataframe, chunksize=None, if_exists='fail'):
        """ Inserts a DataFrame into table.

        :param str table_name: The table to insert into.
        :param pandas.DataFrame dataframe: The DataFrame to upload to the table.
        :param int chunksize: the number of rows to insert at a time. Defaults to None to use default value for database.
        :param string if_exists: what to do if the table exists. Valid inputs are 'append', 'replace', and 'fail'. Defaults to 'fail'.
        """
            
        if not isinstance(self.database, Database):
            raise Exception(
                'No database connection set up. Please add a connection using one of'
                ' the create_db_connection functions.')

        if int(chunksize) < 1:
            raise UserError('Chunksize must be greater than 0 or \'None\' to use default value')
        else:
            self.database.add_table(dataframe=dataframe, table_name=table_name, chunksize=chunksize, if_exists=if_exists)
        logging.warning('ATSCALE.py will deprecate write_dataframe_to_db in the future, '
                        'use self.database.add_table() instead')

    # SKUNKWORKS

    # direct db query

    def generate_atscale_query(self, features, filter_equals=None, filter_greater=None, filter_less=None,
                               filter_greater_or_equal=None, filter_less_or_equal=None, filter_not_equal=None,
                               filter_in=None, filter_between=None, filter_like=None, filter_rlike=None,
                               filter_null=None, filter_not_null=None, limit=None, comment=None):
        """ Generates an AtScale query to get the given features.

        :param list of str features: The list of features to query.
        :param dict of str/str filter_equals: Filters results based on the feature equaling the value. Defaults to None
        :param dict of str/str filter_greater: Filters results based on the feature being greater than the value. Defaults to None
        :param dict of str/str filter_less: Filters results based on the feature being less than the value. Defaults to None
        :param dict of str/str filter_greater_or_equal: Filters results based on the feature being greater or equaling the value. Defaults to None
        :param dict of str/str filter_less_or_equal: Filters results based on the feature being less or equaling the value. Defaults to None
        :param dict of str/str filter_not_equal: Filters results based on the feature not equaling the value. Defaults to None
        :param dict of str/list of str filter_in: Filters results based on the feature being contained in the values. Defaults to None
        :param dict of str/tuple of (str,str) filter_between: Filters results based on the feature being between the values. Defaults to None
        :param dict of str/str filter_like: Filters results based on the feature being like the clause. Defaults to None
        :param dict of str/str filter_rlike: Filters results based on the feature being matched by the regular expression. Defaults to None
        :param list of str filter_null: Filters results to show null values of the specified features. Defaults to None
        :param list of str filter_not_null: Filters results to exclude null values of the specified features. Defaults to None
        :param int limit: Limit the number of results. Defaults to None for no limit.
        :param str comment: A comment string to build into the query. Defaults to None for no comment.
        :return: An AtScale query string.
        :rtype: str
        """

        if filter_equals is None:
            filter_equals = {}
        if filter_greater is None:
            filter_greater = {}
        if filter_less is None:
            filter_less = {}
        if filter_greater_or_equal is None:
            filter_greater_or_equal = {}
        if filter_less_or_equal is None:
            filter_less_or_equal = {}
        if filter_not_equal is None:
            filter_not_equal = {}
        if filter_in is None:
            filter_in = {}
        if filter_between is None:
            filter_between = {}
        if filter_like is None:
            filter_like = {}
        if filter_rlike is None:
            filter_rlike = {}
        if filter_null is None:
            filter_null = []
        if filter_not_null is None:
            filter_not_null = []
        if type(filter_null) != list:
            filter_null = [filter_null]
        if type(filter_not_null) != list:
            filter_not_null = [filter_not_null]

        if type(features) != list:
            raise UserError(f'Make sure that Argument: \'{features}\' is a list')

        list_all = self.list_all_features()
        self._check_multiple_features(features, list_all)
        self._check_multiple_features(filter_equals, list_all)
        self._check_multiple_features(filter_greater, list_all)
        self._check_multiple_features(filter_less, list_all)
        self._check_multiple_features(filter_greater_or_equal, list_all)
        self._check_multiple_features(filter_less_or_equal, list_all)
        self._check_multiple_features(filter_not_equal, list_all)
        self._check_multiple_features(filter_in, list_all)
        self._check_multiple_features(filter_between, list_all)
        self._check_multiple_features(filter_like, list_all)
        self._check_multiple_features(filter_rlike, list_all)
        self._check_multiple_features(filter_null, list_all)
        self._check_multiple_features(filter_not_null, list_all)
        
        categorical_features = []
        numeric_features = []
        for feature in features:
            if feature in self.list_all_categorical_features():
                categorical_features.append(feature)
            else:
                numeric_features.append(feature)
        
        if categorical_features:
            categorical_columns_string = ' ' + ', '.join(f'`{self.model_name}`.`{x}`' for x in categorical_features)
            if numeric_features:
                categorical_columns_string += ','
        else:
            categorical_columns_string = ''
        if numeric_features:
            numeric_columns_string = ' ' + ', '.join(f'`{self.model_name}`.`{x}`' for x in numeric_features)
        else:
            numeric_columns_string = ''

        if filter_equals or filter_greater or filter_less or filter_greater_or_equal or filter_less_or_equal \
                or filter_not_equal or filter_in or filter_between or filter_null or filter_not_null or filter_like \
                or filter_rlike:
            filter_string = ' WHERE ('
            for key, value in filter_equals.items():
                if filter_string != ' WHERE (':
                    filter_string += ' and '
                if not isinstance(value, (int, float, bool)):
                    filter_string += f'(`{self.model_name}`.`{key}` = \'{value}\')'
                else:
                    filter_string += f'(`{self.model_name}`.`{key}` = {value})'
            for key, value in filter_greater.items():
                if filter_string != ' WHERE (':
                    filter_string += ' and '
                if not isinstance(value, (int, float, bool)):
                    filter_string += f'(`{self.model_name}`.`{key}` > \'{value}\')'
                else:
                    filter_string += f'(`{self.model_name}`.`{key}` > {value})'
            for key, value in filter_less.items():
                if filter_string != ' WHERE (':
                    filter_string += ' and '
                if not isinstance(value, (int, float, bool)):
                    filter_string += f'(`{self.model_name}`.`{key}` < \'{value}\')'
                else:
                    filter_string += f'(`{self.model_name}`.`{key}` < {value})'
            for key, value in filter_greater_or_equal.items():
                if filter_string != ' WHERE (':
                    filter_string += ' and '
                if not isinstance(value, (int, float, bool)):
                    filter_string += f'(`{self.model_name}`.`{key}` >= \'{value}\')'
                else:
                    filter_string += f'(`{self.model_name}`.`{key}` >= {value})'
            for key, value in filter_less_or_equal.items():
                if filter_string != ' WHERE (':
                    filter_string += ' and '
                if not isinstance(value, (int, float, bool)):
                    filter_string += f'(`{self.model_name}`.`{key}` <= \'{value}\')'
                else:
                    filter_string += f'(`{self.model_name}`.`{key}` <= {value})'
            for key, value in filter_not_equal.items():
                if filter_string != ' WHERE (':
                    filter_string += ' and '
                if not isinstance(value, (int, float, bool)):
                    filter_string += f'(`{self.model_name}`.`{key}` <> \'{value}\')'
                else:
                    filter_string += f'(`{self.model_name}`.`{key}` <> {value})'
            for key, value in filter_like.items():
                if filter_string != ' WHERE (':
                    filter_string += ' and '
                if not isinstance(value, (int, float, bool)):
                    filter_string += f'(`{self.model_name}`.`{key}` LIKE \'{value}\')'
                else:
                    filter_string += f'(`{self.model_name}`.`{key}` LIKE {value})'
            for key, value in filter_rlike.items():
                if filter_string != ' WHERE (':
                    filter_string += ' and '
                filter_string += f'(`{self.model_name}`.`{key}` RLIKE \'{value}\')'
            for key, value in filter_in.items():
                str_values = [str(x) for x in value]
                if filter_string != ' WHERE (':
                    filter_string += ' and '
                if not isinstance(value[0], (int, float, bool)):
                    filter_string += f'(`{self.model_name}`.`{key}` IN (\''
                    filter_string += '\', \''.join(str_values)
                    filter_string += '\'))'
                else:
                    filter_string += f'(`{self.model_name}`.`{key}` IN ('
                    filter_string += ', '.join(str_values)
                    filter_string += '))'
            for key, value in filter_between.items():
                if filter_string != ' WHERE (':
                    filter_string += ' and '
                if not isinstance(value[0], (int, float, bool)):
                    filter_string += f'(`{self.model_name}`.`{key}` BETWEEN \'{value[0]}\' and \'{value[1]}\')'
                else:
                    filter_string += f'(`{self.model_name}`.`{key}` BETWEEN {value[0]} and {value[1]})'
            for key in filter_null:
                if filter_string != ' WHERE (':
                    filter_string += ' and '
                filter_string += f'(`{self.model_name}`.`{key}` IS NULL)'
            for key in filter_not_null:
                if filter_string != ' WHERE (':
                    filter_string += ' and '
                filter_string += f'(`{self.model_name}`.`{key}` IS NOT NULL)'
            filter_string += ') GROUP BY 1'
        else:
            filter_string = ''
        
        if limit is None:
            limit_string = ''
        else:
            limit_string = f' LIMIT {limit}'

        if comment is None:
            comment_string = ''
        else:
            comment_string = f' /* {comment} */'

        query = f'SELECT {categorical_columns_string}{numeric_columns_string} FROM `{self.project_name}`' \
                f'.`{self.model_name}` `{self.model_name}`{filter_string}{limit_string}{comment_string}'
        return query

    def generate_db_query(self, atscale_query):
        """ Submits an AtScale query to the query planner to generate a query for Snowflake.

        :param str atscale_query: The AtScale query to convert to a database query.
        :return: A database query string.
        :rtype: str
        """
        limit_match = re.search(r"LIMIT [0-9]+", atscale_query)
        if limit_match:
            inbound_query = atscale_query.replace(limit_match.group(0), 'LIMIT 1')
        else:
            inbound_query = f'{atscale_query} LIMIT 1'
            
        comment_match = re.findall(r"/\*.+?\*/", atscale_query)
            
        now = datetime.utcnow()  # current date and time
        now = now - timedelta(minutes=5)

        date_time = now.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        
        self.custom_query(inbound_query)

        url = f'{self.server}:{self.engine_port}/queries/orgId/{self.organization}'\
              f'?limit=21&querySource=user&queryStarted=5m&queryDateTimeStart={date_time}'

        response = requests.get(url, headers=self.headers)
        if response.status_code == 401:
            logging.info('Invalid authentication, if you created this instance with a token, you need a new one')
            self.refresh_token()
            response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            json_data = json.loads(response.content)['response']
        else:
            resp = json.loads(response.text)
            raise Exception(resp['response']['error'])
        db_query = ''

        for query_info in json_data['data']:
            if db_query != '':
                break
            if query_info['query_text'] == inbound_query:
                for event in query_info['timeline_events']:
                    if event['type'] == 'SubqueriesWall':
                        outbound_query = event['children'][0]['query_text']
                        if limit_match:
                            db_query = outbound_query.replace('LIMIT 1', limit_match.group(0))
                        else:
                            db_query = outbound_query.replace('LIMIT 1', '')
                        if comment_match:
                            for comment in comment_match:
                                db_query += ' '
                                db_query += comment
                            
                        break

        return db_query

    #could deprecate
    def submit_db_query(self, db_query):
        """ Submits a query to the database and returns the result.

        :param str db_query: The query to submit to the database.
        :return: The queried data.
        :rtype: pandas.DataFrame
        """
        if not self.database.get_atscale_connection_id():
            raise Exception(
                'No database connection set up. Please add a connection using one of the create_db_connection functions.')
        return self.database.submit_query(db_query=db_query)

    def get_data_direct(self, features, filter_equals=None, filter_greater=None, filter_less=None, filter_greater_or_equal=None, filter_less_or_equal=None,
                        filter_not_equal=None, filter_in=None, filter_between=None, filter_like=None, filter_rlike=None, filter_null=None,
                        filter_not_null=None, limit=None, comment=None):
        """ Generates an AtScale query to get the given features, translates it to a database query, and submits it directly to the database.

        :param list of str features: The list of features to query.
        :param dict of str/str filter_equals: Filters results based on the feature equaling the value. Defaults to None
        :param dict of str/str filter_greater: Filters results based on the feature being greater than the value. Defaults to None
        :param dict of str/str filter_less: Filters results based on the feature being less than the value. Defaults to None
        :param dict of str/str filter_greater_or_equal: Filters results based on the feature being greater or equaling the value. Defaults to None
        :param dict of str/str filter_less_or_equal: Filters results based on the feature being less or equaling the value. Defaults to None
        :param dict of str/str filter_not_equal: Filters results based on the feature not equaling the value. Defaults to None
        :param dict of str/list of str filter_in: Filters results based on the feature being contained in the values. Defaults to None
        :param dict of str/tuple of (str,str) filter_between: Filters results based on the feature being between the values. Defaults to None
        :param dict of str/str filter_like: Filters results based on the feature being like the clause. Defaults to None
        :param dict of str/str filter_rlike: Filters results based on the feature being matched by the regular expression. Defaults to None
        :param list of str filter_null: Filters results to show null values of the specified features. Defaults to None
        :param list of str filter_not_null: Filters results to exclude null values of the specified features. Defaults to None
        :param int limit: Limit the number of results. Defaults to None for no limit.
        :param str comment: A comment string to build into the query. Defaults to None for no comment.
        :return: the queried data
        :rtype: pandas.DataFrame
        """

        return self.database.submit_query(
            self.generate_db_query(self.generate_atscale_query(features, filter_equals, filter_greater, filter_less,
                                                                 filter_greater_or_equal, filter_less_or_equal,
                                                                 filter_not_equal, filter_in,
                                                                 filter_between, filter_like, filter_rlike, filter_null,
                                                                 filter_not_null, limit, comment)))

    # FUNCTION TO BE DEPRECATED

    def create_db_connection_snowflake(self, atscale_connection_id, username, account, warehouse, database, schema):
        """ Creates a database connection to allow for writeback to a Snowflake warehouse.

        :param str atscale_connection_id: The connection name for the warehouse in AtScale.
        :param str username: The database username.
        :param str account: The database account.
        :param str warehouse: The database warehouse.
        :param str database: The database name.
        :param str schema: The database schema.
        """
        self._check_single_connection(atscale_connection_id)

        from atscale.db.snowflake import Snowflake
        self.database = Snowflake(atscale_connection_id=atscale_connection_id, username=username, account=account,
                                  warehouse=warehouse, database=database, schema=schema)
        logging.warning('Warning: create_db_connection_<db_name> functions will be deprecated in the future. '
                     'You can create a connection by creating a Database object of the respective cloud service and '
                     'setting the database field to it which can be done using create_db_connection')

    def create_db_connection_redshift(self, atscale_connection_id, username, host, database, schema, port='5439'):
        """ Creates a database connection to allow for writeback to a Redshift warehouse.

        :param str atscale_connection_id: The connection name for the warehouse in AtScale.
        :param str username: The database username.
        :param str host: The host.
        :param str database: The database name.
        :param str schema: The database schema.
        :param str port: The database port (defaults to 5439).
        """
        self._check_single_connection(atscale_connection_id)

        import atscale.db.redshift
        self.database = atscale.db.redshift.Redshift(atscale_connection_id=atscale_connection_id, username=username, host=host,
                                                     database=database, schema=schema, port=port)
        logging.warning('Warning: create_db_connection_<db_name> functions will be deprecated in the future. '
                     'You can create a connection by creating a Database object of the respective cloud service and '
                     'setting the database field to it which can be done using create_db_connection')

    def create_db_connection_bigquery(self, atscale_connection_id, credentials_path, project, dataset):
        """ Creates a database connection to allow for writeback to a BigQuery warehouse.

        :param str atscale_connection_id: The connection name for the warehouse in AtScale.
        :param str credentials_path: The path to the JSON file with the database credentials.
        :param str project: The database project.
        :param str dataset: The database dataset.
        """
        self._check_single_connection(atscale_connection_id)

        import atscale.db.bigquery
        self.database = atscale.db.bigquery.BigQuery(atscale_connection_id=atscale_connection_id,
                                                     credentials_path=credentials_path,
                                                     project=project, dataset=dataset)
        logging.warning('Warning: create_db_connection_<db_name> functions will be deprecated in the future. '
                     'You can create a connection by creating a Database object of the respective cloud service and '
                     'setting the database field to it which can be done using create_db_connection')

    def create_db_connection_synapse(self, atscale_connection_id, username, host, database, driver, schema, port=1433):
        """ Creates a database connection to allow for writeback to a Synapse warehouse.

        :param str atscale_connection_id: The connection name for the warehouse in AtScale.
        :param str username: The database username.
        :param str host: The host.
        :param str database: The database name.
        :param str driver: The SQL server driver to use.
        :param str schema: The database schema.
        :param str port: The database port (defaults to 1433).
        """
        self._check_single_connection(atscale_connection_id)

        import atscale.db.synapse
        self.database = atscale.db.synapse.Synapse(atscale_connection_id=atscale_connection_id, username=username, host=host,
                                                   database=database, driver=driver, schema=schema, port=port)
        logging.warning('Warning: create_db_connection_<db_name> functions will be deprecated in the future. '
                     'You can create a connection by creating a Database object of the respective cloud service and '
                     'setting the database field to it which can be done using create_db_connection')

    def create_db_connection_databricks(self, atscale_connection_id, token, host, database, http_path, driver,
                                        port=443):
        """ Creates a database connection to allow for writeback to a Databricks warehouse.

        :param str atscale_connection_id: The connection name for the warehouse in AtScale.
        :param str token: The database token.
        :param str host: The host.
        :param str database: The database name.
        :param str http_path: The database HTTP path.
        :param str driver: The Databricks driver to use.
        :param str port: The database port (defaults to 443).
        """
        self._check_single_connection(atscale_connection_id)

        import atscale.db.databricks
        self.database = atscale.db.databricks.Databricks(atscale_connection_id, token, host, database, http_path, driver,
                                                         port=port)
        logging.warning('Warning: create_db_connection_<db_name> functions will be deprecated in the future. '
                     'You can create a connection by creating a Database object of the respective cloud service and '
                     'setting the database field to it which can be done using create_db_connection')

    def create_db_connection_iris(self, atscale_connection_id, username, host, namespace, driver, schema, port=1972):
        """ Creates a database connection to allow for writeback to a IRIS warehouse.

        :param str atscale_connection_id: The connection name for the warehouse in AtScale.
        :param str username: The database username.
        :param str host: The host.
        :param str namespace: The namespace name.
        :param str driver: The IRIS driver to use.
        :param str schema: The database schema.
        :param str port: The database port (defaults to 1972).
        """
        logging.warning('Warning: create_db_connection_<db_name> functions will be deprecated in the future. '
                     'You can create a connection by creating a Database object of the respective cloud service and '
                     'setting the database field to it which can be done using create_db_connection')
        self._check_single_connection(atscale_connection_id)

        import atscale.db.iris
        self.database = atscale.db.iris.Iris(atscale_connection_id=atscale_connection_id, username=username, host=host,
                                             namespace=namespace, driver=driver, schema=schema, port=port)
