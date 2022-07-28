from soda.scan import Scan
import json
import requests
from datetime import datetime
import os
from GetPutInfoAtlan import GetPutInfoAtlan
from soda.common.json_helper import JsonHelper
import zipfile, io
import yaml
from yaml.loader import SafeLoader
import ruamel.yaml
import traceback
from ruamel.yaml.scalarstring import PreservedScalarString
import textwrap
import snowflake.connector

ALWAYS_SEND = 'False'


def wrapped(s, width=60):
    return PreservedScalarString('\n'.join(textwrap.wrap(s, width=width)))


# TODO ручной запуск работает, только проблема откуда брать статус работы теста и результат
# TODO использова tempfile
# TODO Использовать шаблон для slack сообщений
# TODO добавить отправление по emails
# TODO продумать если несколько репозиториев с разными тестами, как выдавать общий статус по таблице
# TODO добавить логирование
# TODO Добавить тесты на аномалии


def send_messeg_to_slake(text_er, hooks_s):
    # print({"text": str(text_er)})
    response = requests.post(
        hooks_s, json={"text": str(text_er)}, headers={"Content-Type": "application/json"}
    )
    # print(response.status_code)
    return response.status_code


def build_scan_results(scan) -> dict:
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


def start_tests():
    scan = Scan()
    scan.set_data_source_name("source")
    scan.add_configuration_yaml_str(
        f"""
        data_source {"source"}:
          type: 'snowflake'
          connection:
            username: {SNOWFLAKE_USER}
            password: {SNOWFLAKE_PASSWORD}
            account: {SNOWFLAKE_ACCOUNT}
            warehouse: {SNOWFLAKE_WAREHOUSE}
    """
    )
    scan.add_sodacl_yaml_files("temp/{}.yml".format(path_to_table))
    test_passed = scan.execute()
    result_test = build_scan_results(scan)
    # print(json.dumps(result_test))
    # print(test_passed)
    return test_passed, result_test


def get_sf_connection(user_sf, password_sf, account_sf, use_db='', use_schema_meta=''):
    if use_db != '':
        entry = snowflake.connector.connect(
            user=user_sf, password=password_sf, account="%s" % (account_sf),
            database=use_db, schema=use_schema_meta)
    else:
        entry = snowflake.connector.connect(
            user=user_sf, password=password_sf, account="%s" % (account_sf)
        )
    return entry


def log_insert_snowflake(for_log_iter):
    sql_log = """insert into DQ_LOG_RESULT_{8} (NAME_TEST,PATH_TO_TABLE, STATUS,RESULT,START_TIME_TS,ERROR_TEXT,
            T_CHANGE_TS,LOGIC_QUERY,COMPANY,DURATION_REQUEST) values ('{0}','{1}','{2}','{3}','{4}','{5}',
            '{6}','{7}','{8}','{9}') """.format(for_log_iter['NAME_TEST'], for_log_iter['PATH_TO_TABLE'],
                                                for_log_iter['STATUS'], for_log_iter['RESULT']['value'],
                                                for_log_iter['START_TIME'], for_log_iter['ERROR_TEXT'],
                                                for_log_iter['INSERT_DATETIME'], for_log_iter['LOGIC_QUERY'],
                                                for_log_iter['COMPANY'], for_log_iter['DURATION_TESTS']
                                                )

    conction_for_meta = get_sf_connection(os.environ['SNOWFLAKE_USER_META'], os.environ['SNOWFLAKE_PASSWORD_META'], os.environ['SNOWFLAKE_ACCOUNT_META'],
                                          use_db='BI__META', use_schema_meta='DQ')
    try:
        sf_cursor = conction_for_meta.cursor()
        sf_cursor.execute(sql_log, timeout=900)
        conction_for_meta.commit()

    except  Exception:
        print(sql_log)
        print(traceback.format_exc())
        print("DQ_LOG_RESULT_{0} недоступен".format(for_log_iter['COMPANY'],))
    conction_for_meta.close()


def create_log_and_messages(result_test, now_int, now, test_passed):
    result_error = ''
    result_ok = ''
    count_result = {}
    for_log = []
    for ch in result_test['checks']:
        name_test = ch['name'].split('.')[0]
        for_log_iter = {"NAME_TEST": name_test,
                        "PATH_TO_TABLE": path_to_table,
                        "STATUS": ch["outcome"],
                        "RESULT": ch["diagnostics"],
                        "START_TIME": now_int,
                        "ERROR_TEXT": ch["name"],
                        "INSERT_DATETIME": int(datetime.utcnow().timestamp()),
                        "LOGIC_QUERY": ch['definition'],
                        "COMPANY": COMPANY,
                        "DURATION_TESTS": (datetime.now() - now).total_seconds(),

                        }
        for_log.append(
            for_log_iter
        )
        if ch['outcome'] != "pass":
            result_error = result_error + ch["name"] + " *value=" + str(ch["diagnostics"]['value']) + "*\n>"
            count_result[name_test] = 0
        else:
            result_ok = result_ok + "--" + ch['name'].split('.')[0] + "\n>"
            count_result[name_test] = 1
    if "FOR_LOG" in prev_for_log:
        for ch in prev_for_log["FOR_LOG"]:

            if ch["NAME_TEST"] in should_not_start:
                name_test = ch["NAME_TEST"]
                for_log.append(
                    {"NAME_TEST": ch["NAME_TEST"],
                     "PATH_TO_TABLE": ch["PATH_TO_TABLE"],
                     "STATUS": ch["STATUS"],
                     "RESULT": ch["RESULT"],
                     "START_TIME": ch["START_TIME"],
                     "ERROR_TEXT": ch["ERROR_TEXT"],
                     "INSERT_DATETIME": int(datetime.utcnow().timestamp()),
                     "LOGIC_QUERY": ch['LOGIC_QUERY'],
                     "COMPANY": COMPANY,
                     "DURATION_TESTS": ch['DURATION_TESTS'],

                     }
                )
                if ch["NAME_TEST"] in prev_for_log['RESULT_ERROR']:
                    count_result[name_test] = prev_for_log['RESULT_ERROR'][ch["NAME_TEST"]]

    table = "*Table:" + path_to_table + "*"
    if test_passed == 0:
        text_e = ":white_check_mark:" + table + " all checks passed " + "\n>" + result_ok
    elif test_passed == 1 or test_passed == 2:
        text_e = ":bangbang: " + table + ' Have_failure' + "\n>" + result_error
    else:
        log_erros = []
        for i in result_test['logs']:
            if i['level'] == "error":
                log_erros.append(i)
        text_e = ":fire: " + table + " test not start,Soda encountered a runtime issue " + "\n" + json.dumps(
            log_erros,
            ensure_ascii=False,
            indent=4)
        for_log.append(
            {"NAME_TEST": "",
             "PATH_TO_TABLE": path_to_table,
             "STATUS": "tests not start",
             "RESULT": "",
             "START_TIME": now_int,
             "ERROR_TEXT": "",
             "INSERT_DATETIME": int(datetime.utcnow().timestamp()),
             "LOGIC_QUERY": result_test['logs'],
             "COMPANY": COMPANY,
             "DURATION_TESTS": (datetime.now() - now).total_seconds(),

             }
        )
    link_to_git_workflow = "https://github.com/{0}/actions/runs/{1}".format(
        GITHUB_REPOSITORY,
        GITHUB_RUN_ID)

    link_to_git_workflow_slack = "<" + link_to_git_workflow + " |link to the test workflow>\n>"

    text_e = text_e + link_to_git_workflow_slack

    # Atlan
    get_info_atlan = GetPutInfoAtlan(API_KEY_ATLAN, DOMMEN_ATLAN)
    link_to_atlan = ''
    if get_info_atlan.flag_all_ok and test_passed != 3:
        guid, link_to_atlan, owners_experts = get_info_atlan.finde_table_in_atlan(path_to_table.replace('.', '/'))
        if guid != '':
            dependents = get_info_atlan.get_dependent_objects(guid)
        else:
            dependents = ''
        text_e = text_e + link_to_atlan
        text_e = text_e + owners_experts
        text_e = text_e + dependents
        get_info_atlan.put_atlan(guid, test_passed, result_error, for_log, link_to_git_workflow, should_not_start)
    else:
        if test_passed == 3:
            print("problems with tests")
        else:
            print("problems with atlan")
    # Atlan

    return text_e, for_log, count_result, link_to_atlan


def wirte_and_send_results(test_passed, text_e, for_log, count_result, link_to_atlan):
    global perv_how_long_status
    for_status = {}
    perv_how_long_status_local = perv_how_long_status

    if ALWAYS_SEND == 'True':
        send_messeg_to_slake(text_e, hooks)
    else:
        if prev_path_test != test_passed:

            perv_how_long_status_local = 0
            send_messeg_to_slake(text_e, hooks)
        elif (prev_len_res != len(
                for_log) or prev_count_result != count_result) and (
                len(prev_count_result) != 0 and (
                test_passed == 1 or test_passed == 2 or test_passed == 3)):

            perv_how_long_status = perv_how_long_status_local + 1
            send_messeg_to_slake(text_e, hooks)
        else:

            perv_how_long_status = perv_how_long_status_local + 1
            if (test_passed == 1 or test_passed == 2 or test_passed == 3) and perv_how_long_status_local % 5 == 0:
                text_e = text_e + "*The error was repeated {0} times*".format(perv_how_long_status_local)
                send_messeg_to_slake(text_e, hooks)

    # send_messeg_to_slake(text_e, hooks)
    for_status['STATUS'] = test_passed
    for_status['HOW_LONG_STATUS'] = perv_how_long_status_local
    for_status['RESULT_ERROR'] = count_result
    for_status['LINK_TO_ATLAN'] = link_to_atlan
    for_status['FOR_LOG'] = for_log

    with open("temp/" + path_to_table + ".json", 'w',
              encoding='utf-8') as f:
        json.dump(for_status, f, ensure_ascii=False, indent=4)
    if test_passed == 1 or test_passed == 2 or test_passed == 3:
        print('tests failed. The test results are recorded in the log and Atlan')
        return 1
    return 0


def get_previous_result():
    headers = {
        'Accept': 'application/vnd.github+json',
        'Authorization': f'token {GIT_TOKEN}',
    }
    count_artifacts = 1
    inter = 1
    id_artefact = 0
    while count_artifacts > 0:
        params = {
            "per_page": 100,
            "page": inter
        }
        response = requests.get(f"https://api.github.com/repos/{GITHUB_REPOSITORY}/actions/artifacts",
                                headers=headers, params=params)
        if 'artifacts' in response.json():
            artifacts = response.json()['artifacts']
        else:
            id_artefact = 0
            count_artifacts = 0
        count_artifacts = len(artifacts)
        for artifact in artifacts:
            if artifact['name'] == path_to_table:
                if artifact['workflow_run']['head_branch'] == GITHUB_REF_NAME:
                    id_artefact = artifact['id']
                    count_artifacts = 0
                    break
        inter = inter + 1
    if id_artefact != 0:
        response = requests.get(
            f'https://api.github.com/repos/{GITHUB_REPOSITORY}/actions/artifacts/{str(id_artefact)}]/zip',
            headers=headers)
        if response.status_code == 200:
            if not os.path.isdir("temp"):
                os.mkdir("temp")
            z = zipfile.ZipFile(io.BytesIO(response.content))
            z.extractall("temp/")


def main():
    if not os.path.isdir("temp"):
        os.mkdir("temp")

    now = datetime.now()
    now_int = int(datetime.utcnow().timestamp())
    if SCHEDULE == 'Manual':
        generation_yml_for_manual_test()
    else:
        generation_yml_for_test()
    test_passed, result_test = start_tests()
    text_e, for_log, count_result, link_to_atlan = create_log_and_messages(result_test, now_int,
                                                                           now,
                                                                           test_passed)
    wirte_and_send_results(test_passed, text_e, for_log, count_result, link_to_atlan)


def represent_literal(dumper, data):
    return dumper.represent_scalar(SafeLoader.DEFAULT_SCALAR_TAG,
                                   data, style="|")


# path_to_table
def generation_yml_for_manual_test():
    with open('Tests_soda/list_tables.yml') as f:
        get_params = yaml.load(f, Loader=SafeLoader)[path_to_table]

        print(json.dumps(get_params))
    if os.environ["CHANNEL_CUSTOM"] != '1':
        global hooks
        hooks = os.environ["CHANNEL_CUSTOM"]
    global ALWAYS_SEND
    ALWAYS_SEND = os.environ['ALWAYS_SEND']
    yml_for_soda = {}
    yml_for_soda['checks for ' + path_to_table] = []
    with open('Tests_soda/pattern_tests.yml') as f:
        get_shablons_tests = yaml.load(f, Loader=SafeLoader)
        for test in get_params:
            if test in get_shablons_tests:
                print(get_params[test])
                get_params[test]['path_to_table'] = path_to_table
                for i in get_shablons_tests[test]:
                    print(get_shablons_tests[test][i].format(**get_params[test]))
                    if "name" == i:
                        get_shablons_tests[test][i] = ruamel.yaml.scalarstring.DoubleQuotedScalarString(
                            get_shablons_tests[test][i].format(**get_params[test]))
                    else:
                        get_shablons_tests[test][i] = get_shablons_tests[test][i].format(**get_params[test])
                        get_shablons_tests[test][i] = wrapped(get_shablons_tests[test][i])
                if test in list_tests or "all_tests" in list_tests:
                    yml_for_soda['checks for ' + path_to_table].append({test: get_shablons_tests[test]})
                else:
                    should_not_start[get_shablons_tests[test]["name"].split('.')[0]] = test
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
                            for_custom_test[custom_test_param] = wrapped(
                                get_params[test][custom_test][custom_test_param])
                    if custom_test in list_tests or "all_tests" in list_tests:
                        yml_for_soda['checks for ' + path_to_table].append({custom_test: for_custom_test})
                    else:
                        should_not_start[get_params[test][custom_test]["name"].split('.')[0]] = custom_test

        print(json.dumps(get_shablons_tests))
    print(json.dumps(yml_for_soda))
    print(json.dumps(should_not_start))

    yaml_wr = ruamel.yaml.YAML()
    with open("temp/" + path_to_table + ".yml", 'w') as outfile:
        yaml_wr.dump(yml_for_soda, outfile)


def generation_yml_for_test():
    with open('Tests_soda/list_tables.yml') as f:
        get_params = yaml.load(f, Loader=SafeLoader)[path_to_table]

        print(json.dumps(get_params))

    if 'schedule' in get_params:
        if get_params['schedule'] == SCHEDULE:
            flag_all_test = True
        else:
            flag_all_test = False
    global hooks
    if "CHANNEL_SLACK" in get_params:
        hooks = os.environ[get_params["CHANNEL_SLACK"]]
    else:
        hooks = os.environ["CHANNEL"]
    if "ALWAYS_SEND" in get_params:
        global ALWAYS_SEND
        ALWAYS_SEND = get_params['ALWAYS_SEND']
    yml_for_soda = {}
    yml_for_soda['checks for ' + path_to_table] = []
    with open('Tests_soda/pattern_tests.yml') as f:
        get_shablons_tests = yaml.load(f, Loader=SafeLoader)
        for test in get_params:
            if test in get_shablons_tests:
                print(get_params[test])
                get_params[test]['path_to_table'] = path_to_table
                for i in get_shablons_tests[test]:
                    print(get_shablons_tests[test][i].format(**get_params[test]))
                    if "name" == i:
                        get_shablons_tests[test][i] = ruamel.yaml.scalarstring.DoubleQuotedScalarString(
                            get_shablons_tests[test][i].format(**get_params[test]))
                    else:
                        get_shablons_tests[test][i] = get_shablons_tests[test][i].format(**get_params[test])
                        get_shablons_tests[test][i] = wrapped(get_shablons_tests[test][i])
                if flag_all_test and "schedule" not in get_params[test]:
                    yml_for_soda['checks for ' + path_to_table].append({test: get_shablons_tests[test]})
                elif flag_all_test and "schedule" in get_params[test]:
                    if get_params[test]['schedule'] == SCHEDULE:
                        yml_for_soda['checks for ' + path_to_table].append({test: get_shablons_tests[test]})
                elif not flag_all_test and "schedule" in get_params[test]:
                    if get_params[test]['schedule'] == SCHEDULE:
                        yml_for_soda['checks for ' + path_to_table].append({test: get_shablons_tests[test]})
                else:
                    should_not_start[get_shablons_tests[test]["name"].split('.')[0]] = test
                if "ALWAYS_SEND" in get_params[test]:
                    ALWAYS_SEND = get_params[test]['ALWAYS_SEND']
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
                            for_custom_test[custom_test_param] = wrapped(
                                get_params[test][custom_test][custom_test_param])
                    if flag_all_test and "schedule" not in get_params[test][custom_test]:
                        yml_for_soda['checks for ' + path_to_table].append({custom_test: for_custom_test})
                    elif flag_all_test and "schedule" in get_params[test][custom_test]:
                        if get_params[test][custom_test]['schedule'] == SCHEDULE:
                            yml_for_soda['checks for ' + path_to_table].append({custom_test: for_custom_test})
                    elif not flag_all_test and "schedule" in get_params[test][custom_test]:
                        if get_params[test][custom_test][
                            'schedule'] == SCHEDULE:
                            yml_for_soda['checks for ' + path_to_table].append({custom_test: for_custom_test})
                    else:
                        should_not_start[get_params[test][custom_test]["name"].split('.')[0]] = custom_test


        print(json.dumps(get_shablons_tests))
    print(json.dumps(yml_for_soda))
    print(json.dumps(should_not_start))

    yaml_wr = ruamel.yaml.YAML()
    with open("temp/" + path_to_table + ".yml", 'w') as outfile:
        yaml_wr.dump(yml_for_soda, outfile)


if __name__ == "__main__":
    should_not_start = {}
    SNOWFLAKE_USER = os.environ['SNOWFLAKE_USER']
    SNOWFLAKE_PASSWORD = os.environ['SNOWFLAKE_PASSWORD']
    SNOWFLAKE_ACCOUNT = os.environ['SNOWFLAKE_ACCOUNT']
    SNOWFLAKE_WAREHOUSE = os.environ['SNOWFLAKE_WAREHOUSE']
    path_to_table = os.environ['PATH_TO_TABLE']
    hooks = os.environ["CHANNEL"]
    COMPANY = os.environ['COMPANY']
    GITHUB_REF_NAME = os.environ['GITHUB_REF_NAME']
    GIT_TOKEN = os.environ['GIT_TOKEN']
    GITHUB_REPOSITORY = os.environ["GITHUB_REPOSITORY"]
    API_KEY_ATLAN = os.environ["API_KEY_ATLAN"]
    DOMMEN_ATLAN = os.environ["DOMMEN_ATLAN"]
    GITHUB_RUN_ID = os.environ["GITHUB_RUN_ID"]
    SCHEDULE = os.environ["SCHEDULE"]
    print(os.environ)
    get_previous_result()
    if SCHEDULE == 'Manual':
        if len(path_to_table.split(':')) > 1:
            list_tests = path_to_table.split(':')[1].split(',')
            path_to_table = path_to_table.split(':')[0]
        else:
            list_tests = ['all_tests']
    if os.path.exists("temp/" + path_to_table + '.json'):
        with open("temp/" + path_to_table + '.json') as f:
            d = json.load(f)
        prev_for_log = d
        prev_len_res = len(d["FOR_LOG"])
        perv_how_long_status = d["HOW_LONG_STATUS"]
        prev_path_test = d["STATUS"]
        prev_count_result = d["RESULT_ERROR"]
    else:
        prev_for_log = []
        prev_len_res = 0
        perv_how_long_status = 0
        prev_path_test = 0
        prev_count_result = []

    main()
