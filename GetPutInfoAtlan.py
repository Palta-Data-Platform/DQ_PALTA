import requests
import json
from datetime import datetime
from requests.exceptions import Timeout, ConnectionError


class GetPutInfoAtlan:

    def __init__(self, apikey, dommen):
        self.apikey = apikey
        self.dommen = dommen
        self.headers = {
            'Content-Type': 'application/json',
            "APIKEY": apikey,
        }

        self.qualifiedname = ''
        try:
            self.all_users = self._get_users()
            self.flag_all_ok = True
        except Timeout:
            print('Timeout error')
            self.flag_all_ok = False
        except ConnectionError:
            print('Connection error')
            self.flag_all_ok = False
        except:
            print('Unidentified error')
            self.flag_all_ok = False

    def get_dependent_objects(self, guid):
        params = (
            ("depth", "2"),
            ("direction", "OUTPUT"),
        )

        response = requests.get(
            self.dommen + "api/metadata/atlas/tenants/default/lineage/" + guid,
            headers=self.headers,
            params=params,
        )
        # print(response.text)
        json_respose = response.json()["guidEntityMap"]
        dependent_objects = "*Dependent objects:*"
        objects = {}
        for i in json_respose:
            if (
                    json_respose[i]["typeName"] != "AtlanProcess"
                    and json_respose[i]["status"] == "ACTIVE"
            ):
                # print(json.dumps(json_respose[i]))
                if json_respose[i]["typeName"] in objects:
                    objects[json_respose[i]["typeName"]].append(
                        json_respose[i]["displayText"]
                    )
                else:
                    objects[json_respose[i]["typeName"]] = [json_respose[i]["displayText"]]
                    # print(json_respose[i]['displayText'])
        for k in objects:
            dependent_objects = (
                    dependent_objects
                    + "\n>\t*"
                    + k.replace("Atlan", "")
                    + "*: "
                    + ",".join(objects[k])
            )
        # print(Dependent_objects)
        return dependent_objects + "\n"

    def finde_table_in_atlan(self, qualifiedname):
        type_name = 'AtlanTable'
        "поиск объектов по параметрам qualifiedName,classifications,meaningNames,typeName "
        data = {
            "searchType": "BASIC",
            "typeName": "AtlanAsset",
            "excludeDeletedEntities": "true",
            "includeClassificationAttributes": "true",
            "includeSubClassifications": "true",
            "includeSubTypes": "true",
            "limit": 4000,
            "offset": 0,
            "attributes": [
                "description",
                "messsage",
                "name",
                "displayName",
                "rowCount",
                "source",
                "sourceType",
                "integration",
                "integrationType",
                "status",
                "statusMeta",
                "table",
                "tables",
                "type",
                "typeName",
                "updatedAt",
                "updatedBy",
                "lastSyncedAt",
                "lastSyncedBy",
                "dataType",
                "isPrimary",
                "experts",
                "owners",
                "colCount",
                "previewImageId",
                "previewImageId",
                "url",
                "classificationNames"
            ],
            "query": "(*instacart order*)",
            "entityFilters": {
                "condition": "AND",
                "criterion": [
                    {
                        "condition": "AND",
                        "criterion": [
                            {
                                "attributeName": "typeName",
                                "attributeValue": type_name,
                                "operator": "eq",
                            }
                        ],
                    }, {
                        "condition": "AND",
                        "criterion": [
                            {
                                "attributeName": "qualifiedName",
                                "attributeValue": qualifiedname,
                                "operator": "CONTAINS",
                            }
                        ],
                    },
                ],
            },
        }
        response = requests.post(
            "https://palta.atlan.com/api/metadata/atlas/tenants/default/search/basic",
            headers=self.headers,
            data=json.dumps(data),
        )
        # print(response.text)
        if response.status_code == 200:
            res = response.json()["entities"]
            owners_experts = self._get_owners_experts(res)
            self.qualifiedname = res[0]["attributes"]["qualifiedName"]
            if len(res) > 0:
                link_to_table = self.dommen + "a/" + res[0]['guid'] + "/overview"
                link_to_atlan = "<" + link_to_table + " |Link to table in Atlan>\n>"
                return res[0]['guid'], link_to_atlan, owners_experts
            else:
                return '', '', ''
        else:
            return '', '', ''

    def _get_owners_experts(self, res):

        owners = ""
        experts = ""
        if "owners" in res[0]["attributes"]:
            if res[0]["attributes"]["owners"] is not None:
                for j in res[0]["attributes"]["owners"]:
                    owners = owners + self.all_users[j] + "; "
        if "experts" in res[0]["attributes"]:
            if res[0]["attributes"]["experts"] is not None:
                for j in res[0]["attributes"]["experts"]:
                    experts = experts + self.all_users[j] + "; "
        return "*Owners*:" + owners + "\n>" + "*Experts*:" + experts + "\n>"

    def _get_users(self):
        params = (
            ("limit", "1000"),
            ("preview", "false"),
        )

        response = requests.get(
            self.dommen + "api/auth/tenants/default/users",
            headers=self.headers,
            params=params,
        )
        all_users = {}
        for i in response.json()["results"]:
            all_users[i["id"]] = i["username"]

        return all_users

    def put_atlan(self, guid, status, text_e, results_tests, link_to_git_workflows, should_not_start):
        if status == 0:
            status_text = "VERIFIED"
            text_e = "OK"
        else:
            status_text = "ISSUE"
            text_e = text_e.replace('\n>', '')

        name_table = self.qualifiedname.split("/")[-1].upper()
        self._change_status(guid=guid, status=status_text, statusdetail=text_e,
                            name=name_table)
        self._send_result_to_atlan_dq(guid=guid, all_test_list=results_tests, should_not_start=should_not_start)
        self._put_tests(guid, link_to_git_workflows)

    def _change_status(self, guid, status, statusdetail, name, typename='AtlanTable'):
        data = {
            "entity": {
                "typeName": typename,
                "attributes": {
                    "qualifiedName": self.qualifiedname,
                    "name": name,
                    "typeName": typename,
                    "statusDetail": statusdetail,
                    "status": status,
                },
                "guid": guid,
            }
        }

        response = requests.post(
            self.dommen + "api/metadata/atlas/tenants/default/entity",
            headers=self.headers,
            data=json.dumps(data).encode("utf-8"),
        )
        # print(response.text)
        return 0

    def _put_tests(self, guid, link_to_git_workflows):
        params = (("isOverwrite", "False"),)

        data = {"080b69d5-f86d-4df4-2ce5-e7a0b9c4752b": {
            "1afce759-11fd-44a8-b4bc-6dc2097bfca1": link_to_git_workflows

        }}
        # print(data)
        response = requests.post(
            self.dommen
            + "api/metadata/atlas/tenants/default/entity/guid/"
            + guid
            + "/businessmetadata",
            headers=self.headers,
            params=params,
            data=json.dumps(data, ensure_ascii=False),
        )
        # print(response.text)
        return 0

    def __get_list_dq(self, guid):
        params = (
            ("guid", guid),
            ("limit", "100"),
            ("sort", "-created_at"),
            ("offset", "0"),
            ("typeName", "AtlanTable"),
        )

        response = requests.get(
            self.dommen + "api/metadata/tenants/default/dataquality/test",
            headers=self.headers,
            params=params,
        )
        # print(response.text)
        return response.json()

    def _send_result_to_atlan_dq(self, guid, all_test_list, should_not_start):
        all_test = {}
        # print(path_to_table)
        for test in all_test_list:
            all_test[test['NAME_TEST']] = test
        # print(path_to_table)
        # print(all_test)
        all_dq_list = self.__get_list_dq(guid)
        all_dq = []
        for i in all_dq_list["records"]:
            all_dq.append(i["name"])
        # print(all_dq)
        for test in all_test:
            if test not in all_dq:
                # print(len(all_test[test][4].replace('\n', '').replace(' ', '')))
                data = {

                    "name": all_test[test]["NAME_TEST"],
                    "description": all_test[test]["ERROR_TEXT"].replace('\n', '').replace('<', ' < ').replace('>',
                                                                                                              ' > '),
                    "sourceAsset": {
                        "assetGuid": guid,
                        "typeName": "AtlanTable"

                    },

                }
                # print(data)
                respons = requests.post(
                    self.dommen + "api/metadata/tenants/default/dataquality/test",
                    headers=self.headers,
                    json=data,
                )
            # print(respons.text)
        all_dq_list = self.__get_list_dq(guid)["records"]

        for dq_list in all_dq_list:
            now = datetime.now()
            new_format = "%Y-%m-%dT%H:%M:%SZ"
            if dq_list["name"] in all_test:
                # print(dq_list["last_result"])
                if dq_list["last_result"] is not None:
                    time_up_dq = dq_list["last_result"]['created_at']
                else:
                    time_up_dq = '2002-06-21T04:02:34.161485Z'
                # print(time_up_dq)
                result_meta = "Test passed"
                if all_test[dq_list["name"]]["STATUS"] != 'pass':
                    result_meta = json.dumps(all_test[dq_list["name"]]["RESULT"])

                result = str(all_test[dq_list["name"]]["RESULT"]["value"])
                data = {
                    "testId": dq_list["id"],
                    "name": dq_list["name"],
                    "dateTime": now.strftime(new_format),
                    "result": result,
                    "runTime": now.strftime(new_format),
                    "resultMeta": result_meta,
                }
                # print(data)
                if dq_list["name"] not in should_not_start:
                    response = requests.post(
                        self.dommen
                        + "api/metadata/tenants/default/dataquality/test/"
                        + dq_list["id"]
                        + "/results/new",
                        headers=self.headers,
                        data=json.dumps(data).encode("utf-8"),
                    )
                # print(response.text)
            else:
                # print(dq_list["id"])
                response = requests.post(
                    "https://palta.atlan.com/api/metadata/tenants/default/dataquality/test/"
                    + dq_list["id"]
                    + "/archive",
                    headers=self.headers,
                )
                # print(response.text)

        return 0
