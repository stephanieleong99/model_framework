#修改为项目路径
export PYTHONPATH=$PYTHONPATH:/Users/dongjian/PycharmProjects/UserDetected/
python reconstruct_conf.py
python gen_feature_ids.py
python fea_extra.py
python Pearson.py
python parse_fea.py
