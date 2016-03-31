

# 复制
cat user_feature_raw | awk 'BEGIN{pos_num=550000;}{if($NF==1){for(i=0;i<50;i++){print $0;}} else if(pos_num>=0){ print $0;pos_num-=1}}' > user_feature_raw_dup
cat user_feature_raw| awk '{print $NF}'|sort|uniq -c #校验数据
cat user_feature_raw_dup| awk '{print $NF}'|sort|uniq -c #校验数据

#python脚本
cd /Users/dongjian/PycharmProjects/UserDetected/feature_work/convert_vector && python reconstruct_conf.py
cd /Users/dongjian/PycharmProjects/UserDetected/feature_work/convert_vector && python gen_feature_ids.py
cd /Users/dongjian/PycharmProjects/UserDetected/feature_work/convert_vector && python fea_extra.py -m train
head user_features_test_features_lines

#data file
awk 'BEGIN{srand()}{b[rand()NR]=$0}END{for(x in b)print b[x]}' user_features_test_features_lines > whole_file
## check
head user_features_test_features_lines
wc whole_file
tail -n 800000 whole_file > train_file
head -n 200000 whole_file > test_file

cat test_file| cut -b 1 > y_test
#train

/Users/dongjian/PycharmProjects/UserDetected/model/liblinear/train -v 5 -e 0.1 -s 7 -c 0.5  whole_file

/Users/dongjian/PycharmProjects/UserDetected/model/liblinear/train  -e 0.0001 -s 7 -c 0.5 /Users/dongjian/data/whole_file /Users/dongjian/data/user_feature_model
/Users/dongjian/PycharmProjects/UserDetected/model/liblinear/predict -b 1 /Users/dongjian/data/test_file /Users/dongjian/data/user_feature_model /Users/dongjian/data/predict
cat ~/data/predict|tail -n+2 | cut -b 1  > ~/data/y_predict

paste y_test y_predict > rs
awk -F \t '{if($1==1 && $2==1 ){print 1}}' rs|wc -l > val # tp
 \t '{if($1==0 && $2==0 ){print 1}}' rs|wc -l >> val # tn
awk -F \t '{if($1==0 && $2==1 ){print 1}}' rs|wc -l >> val # fp
awk -F \t '{if($1==1 && $2==0 ){print 1}}' rs|wc -l >> val # fn

h'ge'a



# 构建正,负样本的测试文件.
awk -F '\t' '{if(NR==1 ||$NF == 1){print $0}}' user_feature_raw > user_feature_raw_label_1
awk -F '\t' '{if(NR==1 ||$NF == 0){print $0}}' user_feature_raw > user_feature_raw_label_0

#test
cp user_features_test_features_lines main_user_features_test_features_lines
python /Users/dongjian/PycharmProjects/UserDetected/feature_work/convert_vector/fea_extra.py -m test
/Users/dongjian/PycharmProjects/UserDetected/model/liblinear/predict -b 1 /Users/dongjian/data/user_features_test_features_lines /Users/dongjian/data/user_feature_model /Users/dongjian/data/user_features_test_predict




#print model
tr -s "&#&" "#" < user_features_test_features_ids | sort -k 2 -t "#" -n >  user_features_test_features_ids_sort
tail -n+7 user_feature_model > model_xishu
paste user_features_test_features_ids_sort model_xishu > model_xishu_ids
sort -t$'\t' -k2 -nr model_xishu_ids > model_xishu_ids_sort

#tran id to name
#把feature_lines 转化为带特征name的
python /Users/dongjian/PycharmProjects/UserDetected/feature_work/convert_vector/tran_id_to_value.py #user_features_test_features_lines_with_info

#paste
da &&awk -F ' ' 'BEGIN{print "value"}{print $0}' user_features_test_features_lines_with_info > user_features_test_value_for_paste #带上前缀value

da &&awk -F ' ' 'BEGIN{print "prob"}{if(NR!=1){print $2}}' user_features_test_predict > user_features_test_for_paste #带上前缀 prob
da &&paste user_features_test_for_paste user_feature_raw_label_0 > user_features_test_rs #将prob 和原始文件组合起来 user_features_test 为目标文件
da &&paste user_features_test_rs user_features_test_value_for_paste > user_features_test_final #将value 和原始文件组合起来

#select
#筛选阈值在xx以上的文件.
da &&awk -F '\t' '{if($1>0.75&&$1<0.8&&NR!=1){print $0}}' user_features_test_rs | sort -k 1 -r -n -g > user_features_test_rs_above_9_tmp && head -n 1 user_features_test_rs | cat - user_features_test_rs_above_9_tmp > user_features_test_rs_above_9_ttmp && mv user_features_test_rs_above_9_ttmp user_features_test_rs_bw_75_80
da &&awk -F '\t' '{if($1>0.9&&NR!=1){print $0}}' user_features_test_final | sort -k 1 -r -n -g > user_features_test_rs_above_9_tmp && head -n 1 user_features_test_rs | cat - user_features_test_rs_above_9_tmp > user_features_test_rs_above_9_ttmp && mv user_features_test_rs_above_9_ttmp user_features_test_rs_ab_9
da &&awk -F '\t' '{if($1>0.85&&NR!=1){print $0}}' user_features_test_final | sort -k 1 -r -n -g > user_features_test_rs_above_9_tmp && head -n 1 user_features_test_rs | cat - user_features_test_rs_above_9_tmp > user_features_test_rs_above_9_ttmp && mv user_features_test_rs_above_9_ttmp user_features_test_rs_ab_85
da &&awk -F '\t' '{if($1>0&&NR!=1){print $0}}' user_features_test_final | sort -k 1 -r -n -g > user_features_test_rs_above_9_tmp && head -n 1 user_features_test_rs | cat - user_features_test_rs_above_9_tmp > user_features_test_rs_above_9_ttmp && mv user_features_test_rs_above_9_ttmp user_features_test_rs_all


#csv
tr -s "," "#" < user_features_test_rs_all |awk -F '\t' 'BEGIN{OFS=","}{$1=$1;print $0}'  > user_features_test_rs_label_0.csv

da &&awk -F '\t' 'BEGIN{OFS=","}{$1=$1;print $0}' user_features_test >  user_features_test.csv



#assist

## 查看训练集
tr -s ',' '#' < user_feature_raw |awk -F '\t' 'BEGIN{OFS=","}{$1=$1;if($NF==1 || NR ==1 ){print $0}}' |head -1000 > user_feature_raw_label_1.csv

tr -s ',' '#' < user_feature_raw |awk -F '\t' 'BEGIN{OFS=","}{$1=$1;if($NF==0 || NR ==1 ){print $0}}' |head -1000 > user_feature_raw_label_0.csv

#sort
tail -n+2 user_features_test_rs |sort -k 1 -r -n > user_features_test_rs_tmp && head -n 1 user_features_test_rs | cat - user_features_test_rs_tmp > user_features_test_rs_sort



#get line
head -1 user_features_test|awk -F " " '{for(i=1;i<=NF;i++){print $i}}' > tmp_0
cat user_features_test| grep 17858529893|awk -F " " '{for(i=1;i<=NF;i++){print $i}}' > tmp_1 && paste tmp_0 tmp_1 > tmp_rs && cat tmp_rs
