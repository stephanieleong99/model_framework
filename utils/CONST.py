import os
import argparse

# data
data_dir = "/Users/dongjian/data"

# code
root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
feature_ext_root_path = os.path.join(root_path, "feature_work")

# install
cvt_root_path = os.path.join(feature_ext_root_path, "convert_vector")
config_path = os.path.join(feature_ext_root_path, "config")

app_name = "user_features"
version = "v_1_4__fix_pay_arrived_rate"
app = "_".join([app_name, version])
app_file = "user_feature_raw_dup"  # user_feature_raw_dup user_features_test
test_file = "user_features_test"  # user_features_test
app_root_path = os.path.join(config_path, app_name)
label_name = "punish_status"


def parse_method(method):
    items = method.split("#")
    if len(items) > 1:
        key, value = items
    else:
        key, value = items[0], "None"
    return key, value


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
    args = init_arguments()
    app = args.app
    app_file = args.app_file
    print app, app_file
