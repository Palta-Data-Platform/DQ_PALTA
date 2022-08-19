import os

import yaml
from yaml.loader import SafeLoader


def test_yml_pattern_tests():
    if os.path.exists("tests_soda/pattern_tests.yml"):
        with open("tests_soda/pattern_tests.yml") as f:
            get_params = yaml.load(f, Loader=SafeLoader)

def _extract_yaml(path):
    with open(path) as f:
        return yaml.load(f, Loader=SafeLoader)
def test_yml_list_tables():
    path = "tests_soda/"
    agr_yml_json = {}
    for file_n in os.listdir(path):
        if file_n == 'pattern_tests.yml':
            continue
        yml_f = _extract_yaml(path + file_n)
        for test in yml_f:
            if test not in agr_yml_json:
                agr_yml_json[test] = yml_f[test]
            else:
                assert test not in agr_yml_json, f"table {test} dublicates in {path + file_n}"

