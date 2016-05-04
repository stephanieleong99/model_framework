#修改为项目路径
export PYTHONPATH=$PYTHONPATH:/Users/lt/PycharmProjects/model_framework/
python reconstruct_conf.py
python gen_feature_ids.py
python fea_extra.py -m train
#python Pearson.py
#python parse_fea.py
