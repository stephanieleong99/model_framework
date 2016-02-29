import os

#data
data_dir = "/Users/dongjian/data"
data_file = os.path.join(data_dir,"ab_user_train_data")


#code
root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
feature_ext_root_path = os.path.join(root_path,"feature_ext")
#install
cvt_root_path = os.path.join(feature_ext_root_path,"convert_vector")
app = "abnormal_dis_analysis"
app_file = "user_abn_dis__without_right_users"
app_root_path = os.path.join(cvt_root_path,app)


def parse_method(method):
    items = method.split("#")
    if len(items) > 1:
        key, value = items
    else:
        key, value = items[0], "None"
    return key, value
