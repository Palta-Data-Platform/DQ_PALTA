from soda.common.json_helper import JsonHelper
from soda.scan import Scan
import json
import os
import yaml
from yaml.loader import SafeLoader
import ruamel.yaml
import textwrap
from ruamel.yaml.scalarstring import PreservedScalarString

class Run_soda:
    def __init__(self, path_to_table, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT, SCHEDULE,
                 SNOWFLAKE_WAREHOUSE,hooks, SODA_CLOUD='False', API_KEY_ID_SODA='', API_KEY_SECRET_SODA=''):
        self.path_to_table = path_to_table
        self.SODA_CLOUD = SODA_CLOUD
        self.SNOWFLAKE_USER = SNOWFLAKE_USER
        self.SNOWFLAKE_PASSWORD = SNOWFLAKE_PASSWORD
        self.SNOWFLAKE_ACCOUNT = SNOWFLAKE_ACCOUNT
        self.SNOWFLAKE_WAREHOUSE = SNOWFLAKE_WAREHOUSE
        self.API_KEY_ID_SODA = API_KEY_ID_SODA
        self.API_KEY_SECRET_SODA = API_KEY_SECRET_SODA
        self.SCHEDULE = SCHEDULE
        self.should_not_start={}
        self.ALWAYS_SEND='False'
        self.EMAILS=''
        self.hooks=hooks
        if SCHEDULE == 'Manual':
            if len(path_to_table.split(':')) > 1:
                self.list_tests = path_to_table.split(':')[1].split(',')
                self.path_to_table = path_to_table.split(':')[0]
            else:
                self.list_tests = ['all_tests']
            self._generation_yml_for_manual_test()
        else:
            self._generation_yml_for_test()
        self.test_passed, self.result_test=self._start_tests()

    def __wrapped(self, s, width=60):
        return PreservedScalarString('\n'.join(textwrap.wrap(s, width=width)))

    def _generation_yml_for_manual_test(self):
        with open('Tests_soda/list_tables.yml') as f:
            get_params = yaml.load(f, Loader=SafeLoader)[self.path_to_table]

            print(json.dumps(get_params))
        if 'EMAILS' in os.environ:
            self.EMAILS = os.environ['EMAILS']
        if os.environ["CHANNEL_CUSTOM"] != '1':
            self.hooks = os.environ["CHANNEL_CUSTOM"]


        self.ALWAYS_SEND = os.environ['ALWAYS_SEND']
        yml_for_soda = {}
        yml_for_soda['checks for ' + self.path_to_table] = []
        with open('Tests_soda/pattern_tests.yml') as f:
            get_shablons_tests = yaml.load(f, Loader=SafeLoader)
            for test in get_params:
                if test in get_shablons_tests:
                    print(get_params[test])
                    get_params[test]['path_to_table'] = self.path_to_table
                    for i in get_shablons_tests[test]:
                        print(get_shablons_tests[test][i].format(**get_params[test]))
                        if "name" == i:
                            get_shablons_tests[test][i] = ruamel.yaml.scalarstring.DoubleQuotedScalarString(
                                get_shablons_tests[test][i].format(**get_params[test]))
                        else:
                            get_shablons_tests[test][i] = get_shablons_tests[test][i].format(**get_params[test])
                            get_shablons_tests[test][i] = self.__wrapped(get_shablons_tests[test][i])
                    if test in self.list_tests or "all_tests" in self.list_tests:
                        yml_for_soda['checks for ' + self.path_to_table].append({test: get_shablons_tests[test]})
                    else:
                        self.should_not_start[get_shablons_tests[test]["name"].split('.')[0]] = test
                elif test == 'custom_selects':
                    for custom_test in get_params[test]:
                        for_custom_test = {}
                        for custom_test_param in get_params[test][custom_test]:

                            if "name" == custom_test_param:
                                for_custom_test['name'] = ruamel.yaml.scalarstring.DoubleQuotedScalarString(
                                    get_params[test][custom_test]['name'])
                            elif "schedule" == custom_test_param:
                                pass
                            else:
                                for_custom_test[custom_test_param] = self.__wrapped(
                                    get_params[test][custom_test][custom_test_param])
                        if custom_test in self.list_tests or "all_tests" in self.list_tests:
                            yml_for_soda['checks for ' + self.path_to_table].append({custom_test: for_custom_test})
                        else:
                            self.should_not_start[get_params[test][custom_test]["name"].split('.')[0]] = custom_test

        #     print(json.dumps(get_shablons_tests))
        # print(json.dumps(yml_for_soda))
        # print(json.dumps(self.should_not_start))

        yaml_wr = ruamel.yaml.YAML()
        with open("temp/" + self.path_to_table + ".yml", 'w') as outfile:
            yaml_wr.dump(yml_for_soda, outfile)

    def _generation_yml_for_test(self):
        with open('Tests_soda/list_tables.yml') as f:
            get_params = yaml.load(f, Loader=SafeLoader)[self.path_to_table]

            print(json.dumps(get_params))
        if 'EMAILS' in get_params:

            self.EMAILS = get_params['EMAILS']
        else:
            self.EMAILS = ''
        if 'schedule' in get_params:
            if get_params['schedule'] == self.SCHEDULE:
                flag_all_test = True
            else:
                flag_all_test = False

        if "CHANNEL_SLACK" in get_params:
            self.hooks = os.environ[get_params["CHANNEL_SLACK"]]
        else:
            self.hooks = os.environ["CHANNEL"]
        if "ALWAYS_SEND" in get_params:
            self.ALWAYS_SEND = get_params['ALWAYS_SEND']
        yml_for_soda = {}
        yml_for_soda['checks for ' + self.path_to_table] = []
        with open('Tests_soda/pattern_tests.yml') as f:
            get_shablons_tests = yaml.load(f, Loader=SafeLoader)
            for test in get_params:
                if test in get_shablons_tests:
                    print(get_params[test])
                    get_params[test]['path_to_table'] = self.path_to_table
                    for i in get_shablons_tests[test]:
                        print(get_shablons_tests[test][i].format(**get_params[test]))
                        if "name" == i:
                            get_shablons_tests[test][i] = ruamel.yaml.scalarstring.DoubleQuotedScalarString(
                                get_shablons_tests[test][i].format(**get_params[test]))
                        else:
                            get_shablons_tests[test][i] = get_shablons_tests[test][i].format(**get_params[test])
                            get_shablons_tests[test][i] = self.__wrapped(get_shablons_tests[test][i])
                    if flag_all_test and "schedule" not in get_params[test]:
                        yml_for_soda['checks for ' + self.path_to_table].append({test: get_shablons_tests[test]})
                    elif flag_all_test and "schedule" in get_params[test]:
                        if get_params[test]['schedule'] == self.SCHEDULE:
                            yml_for_soda['checks for ' + self.path_to_table].append({test: get_shablons_tests[test]})
                    elif not flag_all_test and "schedule" in get_params[test]:
                        if get_params[test]['schedule'] == self.SCHEDULE:
                            yml_for_soda['checks for ' + self.path_to_table].append({test: get_shablons_tests[test]})
                    else:
                        self.should_not_start[get_shablons_tests[test]["name"].split('.')[0]] = test
                    if "ALWAYS_SEND" in get_params[test]:
                        self.ALWAYS_SEND = get_params[test]['ALWAYS_SEND']
                elif test == 'custom_selects':
                    for custom_test in get_params[test]:
                        for_custom_test = {}
                        for custom_test_param in get_params[test][custom_test]:

                            if "name" == custom_test_param:
                                for_custom_test['name'] = ruamel.yaml.scalarstring.DoubleQuotedScalarString(
                                    get_params[test][custom_test]['name'])
                            elif "schedule" == custom_test_param:
                                pass
                            else:
                                for_custom_test[custom_test_param] = self.__wrapped(
                                    get_params[test][custom_test][custom_test_param])
                        if flag_all_test and "schedule" not in get_params[test][custom_test]:
                            yml_for_soda['checks for ' + self.path_to_table].append({custom_test: for_custom_test})
                        elif flag_all_test and "schedule" in get_params[test][custom_test]:
                            if get_params[test][custom_test]['schedule'] == self.SCHEDULE:
                                yml_for_soda['checks for ' + self.path_to_table].append({custom_test: for_custom_test})
                        elif not flag_all_test and "schedule" in get_params[test][custom_test]:
                            if get_params[test][custom_test][
                                'schedule'] == self.SCHEDULE:
                                yml_for_soda['checks for ' + self.path_to_table].append({custom_test: for_custom_test})
                        else:
                            self.should_not_start[get_params[test][custom_test]["name"].split('.')[0]] = custom_test

            # print(json.dumps(get_shablons_tests))

        yaml_wr = ruamel.yaml.YAML()
        with open("temp/" + self.path_to_table + ".yml", 'w') as outfile:
            yaml_wr.dump(yml_for_soda, outfile)

    def __build_scan_results(self, scan) -> dict:
        checks = [
            check.get_cloud_dict()
            for check in scan._checks
            if (check.outcome is not None or check.force_send_results_to_cloud == True) and check.archetype is None
        ]
        automated_monitoring_checks = [
            check.get_cloud_dict()
            for check in scan._checks
            if (check.outcome is not None or check.force_send_results_to_cloud == True) and check.archetype is not None
        ]
        profiling = [
            profile_table.get_cloud_dict()
            for profile_table in scan._profile_columns_result_tables + scan._sample_tables_result_tables
        ]

        return JsonHelper.to_jsonnable(  # type: ignore
            {
                "definitionName": scan._scan_definition_name,
                "defaultDataSource": scan._data_source_name,
                "dataTimestamp": scan._data_timestamp,
                "scanStartTimestamp": scan._scan_start_timestamp,
                "scanEndTimestamp": scan._scan_end_timestamp,
                "hasErrors": scan.has_error_logs(),
                "hasWarnings": scan.has_check_warns(),
                "hasFailures": scan.has_check_fails(),
                "metrics": [metric.get_cloud_dict() for metric in scan._metrics],
                # If archetype is not None, it means that check is automated monitoring
                "checks": checks,
                "queries": [query.get_cloud_dict() for query in scan._queries],
                "automatedMonitoringChecks": automated_monitoring_checks,
                "profiling": profiling,
                "metadata": [
                    discover_tables_result.get_cloud_dict()
                    for discover_tables_result in scan._discover_tables_result_tables
                ],
                "logs": [log.get_cloud_dict() for log in scan._logs.logs],
            }
        )

    def _start_tests(self):
        scan = Scan()
        scan.set_data_source_name("source")
        scan.set_scan_definition_name(self.path_to_table)

        if self.SODA_CLOUD == 'True':
            scan.add_configuration_yaml_str(
                f"""
                    data_source {"source"}:
                      type: 'snowflake'
                      connection:
                        username: {self.SNOWFLAKE_USER}
                        password: {self.SNOWFLAKE_PASSWORD}
                        account: {self.SNOWFLAKE_ACCOUNT}
                        warehouse: {self.SNOWFLAKE_WAREHOUSE}
                    soda_cloud:
                      host: cloud.soda.io
                      api_key_id: {self.API_KEY_ID_SODA}
                      api_key_secret: {self.API_KEY_SECRET_SODA}
                """
            )
        else:
            scan.add_configuration_yaml_str(
                f"""
                    data_source {"source"}:
                      type: 'snowflake'
                      connection:
                        username: {self.SNOWFLAKE_USER}
                        password: {self.SNOWFLAKE_PASSWORD}
                        account: {self.SNOWFLAKE_ACCOUNT}
                        warehouse: {self.SNOWFLAKE_WAREHOUSE}
                """
            )
        scan.add_sodacl_yaml_files("temp/{}.yml".format(self.path_to_table))
        test_passed = scan.execute()
        result_test = self.__build_scan_results(scan)
        # print(json.dumps(result_test))
        # print(test_passed)
        return test_passed, result_test
