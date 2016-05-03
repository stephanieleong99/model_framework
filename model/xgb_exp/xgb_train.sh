train_data=user_feature_raw
version=v_1_test_origin
app_name=user_feature_origin
app=${app_name}_${version}
features_lines=${app_name}_${version}_features_lines
test_file=user_features_test
features_ids_name=${app}_features_ids

#python脚本
cd /Users/dongjian/PycharmProjects/UserDetected/feature_work/convert_vector && python reconstruct_conf.py
cd /Users/dongjian/PycharmProjects/UserDetected/feature_work/convert_vector && python gen_feature_ids.py
cd /Users/dongjian/PycharmProjects/UserDetected/feature_work/convert_vector && python fea_extra.py -m train
cd ~/data && head ${features_lines}

mv ${features_lines} ${features_lines}_train
cd /Users/dongjian/PycharmProjects/UserDetected/feature_work/convert_vector && python fea_extra.py -m test
mv ${features_lines} ${features_lines}_test

/Users/dongjian/work/tmp/xgboost/xgboost v_1_test_origin.conf
/Users/dongjian/work/tmp/xgboost/xgboost v_1_test_origin.conf  task=pred model_in=0002.model

awk -F '&#&' 'BEGIN{OFS="\t";print 0,"none","i"}{$1=$1;print $2,$1,"q"}' ${features_ids_name} > featmap
/Users/dongjian/work/tmp/xgboost/xgboost v_1_test_origin.conf task=dump model_in=0002.model fmap=featmap name_dump=v_1_test_origin.dump



#带上preds

cd ~/data &&awk -F ' ' 'BEGIN{print "value"}{print $0}' user_features_v_1_16__fix_mean_visit_fir_one_poi_features_lines_with_info > user_features_value_for_paste #带上前缀value
#cd ~/data &&awk -F ' ' 'BEGIN{print "prob"}NR==1{if($2==0){num=3}else{num=2}}{if(NR!=1){print $num}}' user_features_predict> user_features_for_paste #带上前缀 prob
cd ~/data &&awk -F ' ' 'BEGIN{print "prob"}{print $0}' pred.txt > user_features_for_paste #带上前缀 prob

cd ~/data &&paste user_features_for_paste ${test_file} > user_features_rs #将prob 和原始文件组合起来 user_features 为目标文件
cd ~/data &&paste user_features_rs user_features_value_for_paste > ${features_lines}_final #将value 和原始文件组合起来


cd ~/data &&awk -F '\t' '{if($1>0.9&&NR!=1){print $0}}' ${features_lines}_final | sort -k 1 -r -n -g > user_features_rs_above_9_tmp && head -n 1 user_features_rs | cat - user_features_rs_above_9_tmp > user_features_rs_above_9_ttmp && mv user_features_rs_above_9_ttmp ${features_lines}_rs_all
wc ${features_lines}_rs_all

#csv
cd ~/data && tr -s "," "#" < ${features_lines}_rs_all |awk -F '\t' 'BEGIN{OFS=","}{$1=$1;print $0}'  > ${features_lines}_rs_prob.csv
(cd ~/data&& awk 'BEGIN{srand()}{b[rand()NR]=$0}END{for(x in b)print b[x]}' ${features_lines}_rs_prob.csv > tmp && head -1 ${features_lines}_rs_prob.csv|cat - tmp > tmp_1 && head -n 1 tmp_1 && tail -n+2  tmp_1|tail -n 60|sort -t$'\t' -k 1rn) >  ${features_lines}_rs_prob_60.csv
cd ~/data && echo ${features_lines}_rs_prob_60.csv && open ${features_lines}_rs_prob_60.csv
