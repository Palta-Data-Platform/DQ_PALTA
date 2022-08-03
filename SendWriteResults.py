import json
import requests
from datetime import datetime
import os
import traceback
import snowflake.connector
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from GetPutInforAtlan import GetPutInfoAtlan


class SendWriteResults:
    def __init__(self, path_to_table, GITHUB_REPOSITORY, GITHUB_REF_NAME,GITHUB_RUN_ID, COMPANY, SCHEDULE,
                 should_not_start, API_KEY_ATLAN, DOMMEN_ATLAN, ALWAYS_SEND, hooks, EMAILS, result_test, now_int, now,
                 test_passed,read_write_s3,run_identifier):
        self.read_write_s3=read_write_s3
        self.run_identifier=run_identifier
        self.GITHUB_REPOSITORY = GITHUB_REPOSITORY
        self.GITHUB_REF_NAME = GITHUB_REF_NAME
        self.GITHUB_RUN_ID=GITHUB_RUN_ID
        self.path_to_table = path_to_table
        self.COMPANY = COMPANY
        self.SCHEDULE = SCHEDULE
        self.should_not_start = should_not_start
        self.API_KEY_ATLAN = API_KEY_ATLAN
        self.DOMMEN_ATLAN = DOMMEN_ATLAN
        self.ALWAYS_SEND = ALWAYS_SEND
        self.hooks = hooks
        self.EMAILS = EMAILS
        self.result_test = result_test
        self.now_int = now_int
        self.now = now
        self.test_passed = test_passed

        self.id_artefact = self._get_previous_result()

    def _get_previous_result(self):
        name=self.read_write_s3.finde_s3('palta-clients-palta-dev','paltabrain/dataplatform/dq-service/'+self.path_to_table+'/')
        if name!=0:
            print("finde json")
            d=self.read_write_s3.read_s3_json('palta-clients-palta-dev',name)
            self.prev_for_log = d
            self.prev_len_res = len(d["FOR_LOG"])
            self.perv_how_long_status = d["HOW_LONG_STATUS"]
            self.prev_path_test = d["STATUS"]
            self.prev_count_result = d["RESULT_ERROR"]
        else:
            print("feast run")
            self.prev_for_log = []
            self.prev_len_res = 0
            self.perv_how_long_status = 0
            self.prev_path_test = 0
            self.prev_count_result = []

        return 0

    def _send_messeg_to_slake(self):
        # print({"text": str(text_er)})
        response = requests.post(
            self.hooks, json={"text": str(self.text_e)}, headers={"Content-Type": "application/json"}
        )
        # print(response.status_code)
        return response.status_code

    def __get_sf_connection(self, user_sf, password_sf, account_sf, use_db='', use_schema_meta='', warehouse=''):
        if use_db != '':
            entry = snowflake.connector.connect(
                user=user_sf, password=password_sf, account="%s" % (account_sf),
                database=use_db, schema=use_schema_meta, warehouse=warehouse)
        else:
            entry = snowflake.connector.connect(
                user=user_sf, password=password_sf, account="%s" % (account_sf), warehouse=warehouse
            )
        return entry

    def _log_insert_snowflake(self, for_log_iter):
        sql_log = """insert into DQ_LOG_RESULT_{8} (NAME_TEST,PATH_TO_TABLE, STATUS,RESULT,START_TIME_TS,ERROR_TEXT, 
        T_CHANGE_TS,LOGIC_QUERY,COMPANY,DURATION_REQUEST,LAUNCH_SCHEDULE) values ('{0}','{1}','{2}','{3}','{4}',
        '{5}', '{6}','{7}','{8}','{9}','{10}') """.format(for_log_iter['NAME_TEST'], for_log_iter['PATH_TO_TABLE'],
                                                           for_log_iter['STATUS'], for_log_iter['RESULT']['value'],
                                                           for_log_iter['START_TIME'], for_log_iter['ERROR_TEXT'],
                                                           for_log_iter['INSERT_DATETIME'],
                                                           for_log_iter['LOGIC_QUERY'].replace("'", "''"),
                                                           for_log_iter['COMPANY'], for_log_iter['DURATION_TESTS'],
                                                           for_log_iter['LAUNCH_SCHEDULE'],
                                                           )

        conction_for_meta = self.__get_sf_connection(os.environ['SNOWFLAKE_USER_META'],
                                                   os.environ['SNOWFLAKE_PASSWORD_META'],
                                                   os.environ['SNOWFLAKE_ACCOUNT_META'],
                                                   use_db='BI__META', use_schema_meta='DQ',
                                                   warehouse=os.environ['SNOWFLAKE_WAREHOUSE_META'])

        try:
            sf_cursor = conction_for_meta.cursor()
            sf_cursor.execute(sql_log, timeout=900)
            conction_for_meta.commit()

        except  Exception:
            print(sql_log)
            print(traceback.format_exc())
            print("DQ_LOG_RESULT_{0} недоступен".format(for_log_iter['COMPANY'], ))
        conction_for_meta.close()

    def _send_email(self):
        FROM = os.environ['SEND_EMAIL']
        TO = self.EMAILS if isinstance(self.EMAILS, list) else [self.EMAILS]
        TEXT = self.text_email.replace("*", '').replace(">", '')
        message = MIMEMultipart("alternative")
        message["Subject"] = self.path_to_table
        message["From"] = FROM
        message["To"] = ", ".join(TO)
        # Prepare actual message

        part2 = MIMEText(TEXT, "plain")
        message.attach(part2)

        try:
            server = smtplib.SMTP("smtp.gmail.com", 587)
            server.ehlo()
            server.starttls()
            server.login(os.environ['SEND_EMAIL'], os.environ['PASSWORD_EMAIL'])
            server.sendmail(FROM, TO, message.as_string())
            server.close()
            print("successfully sent the mail")
        except:
            print("failed to send mail")
            print(traceback.format_exc())

    def create_log_and_messages(self):
        result_error = ''
        result_ok = ''
        count_result = {}
        for_log = []
        for ch in self.result_test['checks']:
            name_test = ch['name'].split('.')[0]
            for_log_iter = {"NAME_TEST": name_test,
                            "PATH_TO_TABLE": self.path_to_table,
                            "STATUS": ch["outcome"],
                            "RESULT": ch["diagnostics"],
                            "START_TIME": self.now_int,
                            "ERROR_TEXT": ch["name"],
                            "INSERT_DATETIME": int(datetime.utcnow().timestamp()),
                            "LOGIC_QUERY": ch['definition'],
                            "COMPANY": self.COMPANY,
                            "DURATION_TESTS": (datetime.now() - self.now).total_seconds(),
                            "LAUNCH_SCHEDULE": self.SCHEDULE

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
            self._log_insert_snowflake(for_log_iter)
        if "FOR_LOG" in self.prev_for_log:
            for ch in self.prev_for_log["FOR_LOG"]:

                if ch["NAME_TEST"] in self.should_not_start:
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
                         "COMPANY": self.COMPANY,
                         "DURATION_TESTS": ch['DURATION_TESTS'],
                         "LAUNCH_SCHEDULE": self.SCHEDULE

                         }
                    )
                    if ch["NAME_TEST"] in self.prev_for_log['RESULT_ERROR']:
                        count_result[name_test] = self.prev_for_log['RESULT_ERROR'][ch["NAME_TEST"]]

        table = "*Table:" + self.path_to_table + "*"
        if self.test_passed == 0:
            text_e = ":white_check_mark:" + table + " all checks passed " + "\n>" + result_ok
            text_email = table + "all checks passed" + "\n>" + result_ok
        elif self.test_passed == 1 or self.test_passed == 2:
            text_e = ":bangbang: " + table + ' Have_failure' + "\n>" + result_error
            text_email = table + ' Have_failure' + "\n>" + result_error
        else:
            log_erros = []
            for i in self.result_test['logs']:
                if i['level'] == "error":
                    log_erros.append(i)
            text_e = ":fire: " + table + " test not start,Soda encountered a runtime issue " + "\n" + json.dumps(
                log_erros,
                ensure_ascii=False,
                indent=4)
            text_email = table + " test not start,Soda encountered a runtime issue " + "\n" + json.dumps(
                log_erros,
                ensure_ascii=False,
                indent=4)
            for_log_iter = {"NAME_TEST": "",
                            "PATH_TO_TABLE": self.path_to_table,
                            "STATUS": "tests not start",
                            "RESULT": {'value': -1},
                            "START_TIME": self.now_int,
                            "ERROR_TEXT": "",
                            "INSERT_DATETIME": int(datetime.utcnow().timestamp()),
                            "LOGIC_QUERY": json.dumps(log_erros),
                            "COMPANY": self.COMPANY,
                            "DURATION_TESTS": (datetime.now() - self.now).total_seconds(),
                            "LAUNCH_SCHEDULE": self.SCHEDULE

                            }
            for_log.append(
                for_log_iter
            )
            self._log_insert_snowflake(for_log_iter)
        link_to_git_workflow = "https://github.com/{0}/actions/runs/{1}".format(
            self.GITHUB_REPOSITORY,
            self.GITHUB_RUN_ID)

        link_to_git_workflow_slack = "<" + link_to_git_workflow + " |link to the test workflow>\n>"

        text_e = text_e + link_to_git_workflow_slack
        text_email = text_email + link_to_git_workflow_slack
        # Atlan
        get_info_atlan = GetPutInfoAtlan(self.API_KEY_ATLAN, self.DOMMEN_ATLAN)
        link_to_atlan = ''
        if get_info_atlan.flag_all_ok and self.test_passed != 3:
            guid, link_to_atlan, owners_experts = get_info_atlan.finde_table_in_atlan(
                self.path_to_table.replace('.', '/'))
            if guid != '':
                dependents = get_info_atlan.get_dependent_objects(guid)
            else:
                dependents = ''
            text_e = text_e + link_to_atlan
            text_e = text_e + owners_experts
            text_e = text_e + dependents
            get_info_atlan.put_atlan(guid, self.test_passed, result_error, for_log, link_to_git_workflow,
                                     self.should_not_start)
            text_email = text_email + link_to_atlan
            text_email = text_email + owners_experts
            text_email = text_email + dependents
        else:
            if self.test_passed == 3:
                print("problems with tests")
            else:
                print("problems with atlan")
        # Atlan
        self.text_e = text_e
        self.for_log = for_log
        self.count_result = count_result
        self.link_to_atlan = link_to_atlan
        self.text_email = text_email

    def wirte_and_send_results(self):

        for_status = {}

        if self.ALWAYS_SEND == 'True':
            self._send_messeg_to_slake()
            if self.EMAILS != '':
                self._send_email()
        else:
            if self.prev_path_test != self.test_passed:

                self.perv_how_long_status = 0
                self._send_messeg_to_slake()
                if self.EMAILS != '':
                    self._send_email()
            elif (self.prev_len_res != len(
                    self.for_log) or self.prev_count_result != self.count_result) and (
                    len(self.prev_count_result) != 0 and (
                    self.test_passed == 1 or self.test_passed == 2 or self.test_passed == 3)):

                self.perv_how_long_status = self.perv_how_long_status + 1
                self._send_messeg_to_slake()
                if self.EMAILS != '':
                    self._send_email()
            else:

                self.perv_how_long_status = self.perv_how_long_status + 1
                if (
                        self.test_passed == 1 or self.test_passed == 2 or self.test_passed == 3) and self.perv_how_long_status % 5 == 0:
                    self.text_e = self.text_e + "*The error was repeated {0} times*".format(self.perv_how_long_status)
                    self._send_messeg_to_slake()
                    if self.EMAILS != '':
                        self._send_email()

        # send_messeg_to_slake(text_e, hooks)
        for_status['STATUS'] = self.test_passed
        for_status['HOW_LONG_STATUS'] = self.perv_how_long_status
        for_status['RESULT_ERROR'] = self.count_result
        for_status['LINK_TO_ATLAN'] = self.link_to_atlan
        for_status['FOR_LOG'] = self.for_log

        with open("temp/" + self.path_to_table + ".json", 'w',
                  encoding='utf-8') as f:
            json.dump(for_status, f, ensure_ascii=False, indent=4)

            self.read_write_s3.write_s3('palta-clients-palta-dev','paltabrain/dataplatform/dq-service/'+self.path_to_table+'/'+self.run_identifier+'.json',str.encode(json.dumps(for_status,ensure_ascii=False, indent=4)))
        if self.test_passed == 1 or self.test_passed == 2 or self.test_passed == 3:
            print('tests failed. The test results are recorded in the log and Atlan')
            return 1
        return 0
