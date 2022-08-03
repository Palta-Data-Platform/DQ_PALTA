from datetime import datetime
import os
from RunSoda import Run_soda
from SendWriteResults import SendWriteResults
from ReadWriteS3 import ReadWriteS3
import random
import string


# TODO сделать рефакторинг кода:
#  1) использова tempfile
#  2) Использовать шаблон для slack сообщений и писем
# TODO продумать если несколько репозиториев с разными тестами, как выдавать общий статус по таблице
# TODO Добавить тесты на аномалии


def main():
    if not os.path.isdir("temp"):
        os.mkdir("temp")

    now = datetime.now()
    now_int = int(datetime.utcnow().timestamp())
    run_soda = Run_soda(path_to_table, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT, SCHEDULE,
                        SNOWFLAKE_WAREHOUSE, hooks, SODA_CLOUD, API_KEY_ID_SODA, API_KEY_SECRET_SODA)
    read_write_s3 = ReadWriteS3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION)
    send_write_results = SendWriteResults(path_to_table, GITHUB_REPOSITORY, GITHUB_REF_NAME, GITHUB_RUN_ID, COMPANY,
                                          SCHEDULE,
                                          should_not_start, API_KEY_ATLAN, DOMMEN_ATLAN, run_soda.ALWAYS_SEND,
                                          run_soda.hooks, run_soda.EMAILS, run_soda.result_test, now_int, now,
                                          run_soda.test_passed, read_write_s3, run_identifier)
    send_write_results.create_log_and_messages()
    send_write_results.wirte_and_send_results()


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
    GITHUB_REPOSITORY = os.environ["GITHUB_REPOSITORY"]
    API_KEY_ATLAN = os.environ["API_KEY_ATLAN"]
    DOMMEN_ATLAN = os.environ["DOMMEN_ATLAN"]
    GITHUB_RUN_ID = os.environ["GITHUB_RUN_ID"]
    SCHEDULE = os.environ["SCHEDULE"]
    AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
    AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
    AWS_REGION = os.environ['AWS_REGION']
    if "RUN_INDENTIFIER" in os.environ:
        run_identifier = os.environ['RUN_INDENTIFIER']
    else:
        run_identifier = ''.join(random.choices(string.ascii_uppercase + string.digits, k=15))
    if 'SODA_CLOUD' in os.environ:
        SODA_CLOUD = os.environ["SODA_CLOUD"]
        API_KEY_ID_SODA = os.environ["API_KEY_ID_SODA"]
        API_KEY_SECRET_SODA = os.environ["API_KEY_SECRET_SODA"]
    else:
        SODA_CLOUD = 'False'
        API_KEY_ID_SODA = ''
        API_KEY_SECRET_SODA = ''
    main()
