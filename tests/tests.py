import yaml
from yaml.loader import SafeLoader
import os
def test_yml_pattern_tests():
    if os.path.exists("Tests_soda/pattern_tests.yml"):
        with open('Tests_soda/pattern_tests.yml') as f:
            get_params = yaml.load(f, Loader=SafeLoader)


def test_yml_list_tables():
    path="Tests_soda/"
    def _extract_yaml(path):
        with open(path) as f:
            return yaml.load(f, Loader=SafeLoader)
    list_file = os.listdir(path)
    agr_yml_json={}
    for file_n in list_file:
        if file_n !='pattern_tests.yml':
            yml_f=_extract_yaml(path+file_n)
            for test in yml_f:
                if test not in agr_yml_json:
                    agr_yml_json[test]=yml_f[test]
                else:
                    print(f"table {test} dublicate in {path+file_n}  ")
                    assert 0==1


    return agr_yml_json