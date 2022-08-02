import yaml
from yaml.loader import SafeLoader
import os
def test_yml_list_tables():
    if os.path.exists("Tests_soda/list_tables.yml"):
        with open('Tests_soda/list_tables.yml') as f:
            get_params = yaml.load(f, Loader=SafeLoader)
def test_yml_pattern_tests():
    if os.path.exists("Tests_soda/pattern_tests.yml"):
        with open('Tests_soda/pattern_tests.yml') as f:
            get_params = yaml.load(f, Loader=SafeLoader)


