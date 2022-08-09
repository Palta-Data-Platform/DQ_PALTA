# soda_tests
Tests_soda - tests written in SodaCL\
before manual starting tests on your computer, you need to initialize the variables\
`API_KEY_ATLAN` - Api keys for interacting with Atlan

`AWS_ACCESS_KEY_ID` - AWS access key for logging to S3 and get previous result tests

`AWS_REGION` - AWS region where the log will be written to S3 and get previous result tests

`AWS_SECRET_ACCESS_KEY` - AWS secret access key for logging to S3 and get previous result tests

`CHANNEL` - Default Slack channel

`COMPANY` - Name company for log

`DOMMEN_ATLAN` - Atlan domain with which we will interact

`DQ_INSTAL` - SSH public key for create workflows

`PASSWORD_EMAIL`  -The password of the application from the mail from which messages will be sent

`SEND_EMAIL` - the mail from which messages will be sent

`SNOWFLAKE_ACCOUNT` - Snowflake account where run tests

`SNOWFLAKE_ACCOUNT_META` - Snowflake account where sends logs

`SNOWFLAKE_PASSWORD` - Snowflake password where run tests

`SNOWFLAKE_PASSWORD_META` - where run tests where sends logs

`SNOWFLAKE_USER` - Snowflake user how run tests

`SNOWFLAKE_USER_META` Snowflake user how sends logs

`SNOWFLAKE_WAREHOUSE` - Snowflake warehouse where run tests

`SNOWFLAKE_WAREHOUSE_META` - - Snowflake warehouse where sends logs

`TEST_CHANNEL` - Slack channel for test run sends

`GITHUB_REF_NAME` - any ref name

`GITHUB_REPOSITORY` - any github rep

at startup, the tests are started and the result is sent to the slack channel, atlan, to artefact.
