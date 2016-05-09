import os
import argparse
import time

# data
DATA_DIR = "/Users/lt/data"

# code
PROJ_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FEATURE_WORK_PATH = os.path.join(PROJ_ROOT_PATH, "feature_work")

# install
CVT_PATH = os.path.join(FEATURE_WORK_PATH, "convert_vector")
CONFIG_PATH = os.path.join(FEATURE_WORK_PATH, "config")

APP_NAME = "user_features"
VERSION = "v_1_test"
APP = "_".join([APP_NAME, VERSION])
RAW_FILE_NAME = "user_feature_raw_dup"  # user_feature_raw_dup user_features_test
TEST_FILE_NAME = "user_features_test"  # user_features_test
# /Users/lt/PycharmProjects/model_framework/feature_work/config/user_features
APP_PATH = os.path.join(CONFIG_PATH, APP_NAME)
LABEL = "punish_status"


def wash(value):
    if value == "NULL":
        value = 0
    value = float(value)
    return 0 if value < 0 or value > 50000000 else value


class TimeRecord(object):
    t = None

    def __init__(self, name):
        self.t = time.time()
        self.name = name

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        # print "{name}   cost {time} seconds".format(**{"name": self.name, "time": time.time() - self.t})
        pass

def safe_get(tmp_list, index, default):
    try:
        return tmp_list[index]
    except IndexError:
        return default


def parse_method(method):
    items = method.split("#")

    if safe_get(items, 1, None) == None:
        items.append("freq")

    if safe_get(items, 2, None) == None:
        items.append(12)

    items[2] = int(items[2])
    key, value, fraction = items
    return key, value, fraction


def init_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument('-app', type=str, dest="app",
                        help="app name")
    parser.add_argument('-f', type=str, dest='app_file',
                        help="app file")
    return parser.parse_args()


def isfloat(value):
    try:
        float(value)
        return True
    except ValueError:
        return False


if __name__ == "__main__":
    # global app,app_file
    # args = init_arguments()
    # app = args.app
    # app_file = args.app_file
    # print app, app_file
    print APP_PATH
