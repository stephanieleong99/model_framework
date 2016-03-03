import os

#data
data_dir = "/Users/dongjian/data"
data_file = os.path.join(data_dir,"ab_user_train_data")


#code
root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
feature_ext_root_path = os.path.join(root_path,"feature_ext")
#install
cvt_root_path = os.path.join(feature_ext_root_path,"convert_vector")
config_path = os.path.join(feature_ext_root_path,"config")

app = "abn_dis_user_poi"
app_file = "abn_dis_user_poi__order_ab_0__users_20000"
app_root_path = os.path.join(config_path,app)


def parse_method(method):
    items = method.split("#")
    if len(items) > 1:
        key, value = items
    else:
        key, value = items[0], "None"
    return key, value
