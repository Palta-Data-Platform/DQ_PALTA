import os
import uuid

from tests_runer.main import StartTests

if __name__ == "__main__":
    os.environ["SNOWFLAKE_ACCOUNT"] = ""
    os.environ["SNOWFLAKE_DB"] = ""
    os.environ["SNOWFLAKE_PASSWORD"] = ""
    os.environ["SNOWFLAKE_USER"] = ""
    os.environ["SNOWFLAKE_WAREHOUSE"] = ""
    table = ""
    run_identifier = str(uuid.uuid4())
    StartTests.main(
        PATH_TO_TABLE=table, run_identifier=run_identifier, simple_run=True
    )
