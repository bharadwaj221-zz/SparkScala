#!/usr/bin/env bash
for confidence in {0.5,0.6,0.7,0.75,0.8}
do
    for support in {0.001,0.0001,0.00001,0.000001}
    do

        echo "Starting Association Rule Mining for Sup = ${support} and Confidence = ${confidence}"
        spark2-submit --master yarn --conf spark.ui.showConsoleProgress=true --num-executors 100 --executor-memory 10G --driver-memory 50G --conf spark.memory.fraction=0.2 --conf spark.yarn.executor.memoryOverhead=600 --executor-cores 8 --class com.visa.dpd.mai.mrm.AssociationRuleMining /data/mdp/staging/mrm/mrm-0.0.1-SNAPSHOT.jar yarn hdfs://nameservice1/projects/mdp/data/mrm/mrm_cust_xb_mrch_dtl/usr_cls=Accelerate/*/xb=1/* /hive/mrm.db/ARMResults/Accelerate ${support} ${confidence}
        spark2-submit --master yarn --conf spark.ui.showConsoleProgress=true --num-executors 100 --executor-memory 10G --driver-memory 50G --conf spark.memory.fraction=0.2 --conf spark.yarn.executor.memoryOverhead=600 --executor-cores 8 --class com.visa.dpd.mai.mrm.AssociationRuleMining /data/mdp/staging/mrm/mrm-0.0.1-SNAPSHOT.jar yarn hdfs://nameservice1/projects/mdp/data/mrm/mrm_cust_xb_mrch_dtl/usr_cls=Decelerate/*/xb=1/* /hive/mrm.db/ARMResults/Decelerate ${support} ${confidence}
        spark2-submit --master yarn --conf spark.ui.showConsoleProgress=true --num-executors 100 --executor-memory 10G --driver-memory 50G --conf spark.memory.fraction=0.2 --conf spark.yarn.executor.memoryOverhead=600 --executor-cores 8 --class com.visa.dpd.mai.mrm.AssociationRuleMining /data/mdp/staging/mrm/mrm-0.0.1-SNAPSHOT.jar yarn hdfs://nameservice1/projects/mdp/data/mrm/mrm_cust_xb_mrch_dtl/usr_cls=Constant/*/xb=1/* /hive/mrm.db/ARMResults/Constant ${support} ${confidence}
        spark2-submit --master yarn --conf spark.ui.showConsoleProgress=true --num-executors 100 --executor-memory 10G --driver-memory 50G --conf spark.memory.fraction=0.2 --conf spark.yarn.executor.memoryOverhead=600 --executor-cores 8 --class com.visa.dpd.mai.mrm.AssociationRuleMining /data/mdp/staging/mrm/mrm-0.0.1-SNAPSHOT.jar yarn hdfs://nameservice1/projects/mdp/data/mrm/mrm_cust_xb_mrch_dtl/usr_cls=Dropped/*/xb=1/* /hive/mrm.db/ARMResults/Dropped ${support} ${confidence}
        spark2-submit --master yarn --conf spark.ui.showConsoleProgress=true --num-executors 100 --executor-memory 10G --driver-memory 50G --conf spark.memory.fraction=0.2 --conf spark.yarn.executor.memoryOverhead=600 --executor-cores 8 --class com.visa.dpd.mai.mrm.AssociationRuleMining /data/mdp/staging/mrm/mrm-0.0.1-SNAPSHOT.jar yarn hdfs://nameservice1/projects/mdp/data/mrm/mrm_cust_xb_mrch_dtl/*/*/xb=1/* /hive/mrm.db/ARMResults/Overall ${support} ${confidence}
    done
done

confidence=0.5
for support in "0.001" "1.0E-4" "1.0E-5"
    do
mkdir -p /data/mdp/staging/mrm/association_rules/Accelerate/Sup=${support}_Conf=${confidence}/FPGrowth
mkdir -p /data/mdp/staging/mrm/association_rules/Decelerate/Sup=${support}_Conf=${confidence}/FPGrowth
mkdir -p /data/mdp/staging/mrm/association_rules/Constant/Sup=${support}_Conf=${confidence}/FPGrowth
mkdir -p /data/mdp/staging/mrm/association_rules/Dropped/Sup=${support}_Conf=${confidence}/FPGrowth

echo "ACCELERATE"
python /data/mdp/staging/mrm/mrm-scripts/replace_merchant_names.py /tmp/ARMResults/Accelerate/Sup=${support}_Conf=${confidence}/FPGrowth /data/mdp/staging/mrm//mrm_mrch_ids.txt /data/mdp/staging/mrm/association_rules/Accelerate/Sup=${support}_Conf=${confidence}/FPGrowth
echo "DECELERATE"
python /data/mdp/staging/mrm/mrm-scripts/replace_merchant_names.py /tmp/ARMResults/Decelerate/Sup=${support}_Conf=${confidence}/FPGrowth /data/mdp/staging/mrm//mrm_mrch_ids.txt /data/mdp/staging/mrm/association_rules/Decelerate/Sup=${support}_Conf=${confidence}/FPGrowth
echo "CONSTANT"
python /data/mdp/staging/mrm/mrm-scripts/replace_merchant_names.py /tmp/ARMResults/Constant/Sup=${support}_Conf=${confidence}/FPGrowth /data/mdp/staging/mrm//mrm_mrch_ids.txt /data/mdp/staging/mrm/association_rules/Constant/Sup=${support}_Conf=${confidence}/FPGrowth
echo "DROPPED"
python /data/mdp/staging/mrm/mrm-scripts/replace_merchant_names.py /tmp/ARMResults/Dropped/Sup=${support}_Conf=${confidence}/FPGrowth /data/mdp/staging/mrm//mrm_mrch_ids.txt /data/mdp/staging/mrm/association_rules/Dropped/Sup=${support}_Conf=${confidence}/FPGrowth
done

spark2-submit --master yarn --conf spark.ui.showConsoleProgress=true --num-executors 50 --executor-memory 10G --driver-memory 10G  --class com.visa.dpd.mai.mrm.AssociationRuleMining /data/mdp/staging/mrm/mrm-0.0.1-SNAPSHOT.jar yarn hdfs://nameservice1/projects/mdp/data/mrm/mrm_cust_catg_stor_dtl/usr_cls=Accelerate/*/* Accelerate 0.5 0.000001