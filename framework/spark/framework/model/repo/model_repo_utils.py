#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb 17 11:30:37 2019

@author: lambda
"""

import pickle

from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.ml import Pipeline
from pyspark.ml import PipelineModel
import pyspark


class ModelRepoUtils:
    __hdfs_model_path='/lambda_models/repo/applications/'

    @staticmethod
    def publish_app_models(app_name,models):
        model_lst=[]
        schema = StructType([StructField('model_name', StringType(), True),StructField('model_location', StringType(), True),StructField('model_type', StringType(), True),StructField('sequence', IntegerType(), True),StructField('active_flag', StringType(), True) ,StructField('is_champion', StringType(), True),StructField('app_name', StringType(), True),StructField('is_champion', IntegerType(), True)])
        for model in models:          
              version=spark.sql('select nvl(max(version),0) as max_ver from lambda_models.model_repository mr where mr.app_name="{0}"'.format(app_name)).collect()[0]["max_ver"]
              version=version+1
              model_hdfspath=ModelRepoUtils._ModelRepoUtils__hdfs_model_path + app_name + '/' + str(version) + '/' + model[1]
              print(model_hdfspath)
              print(type(model[0]))
              model[0].write().overwrite().save(model_hdfspath)
              model_lst.append((model[1],model_hdfspath,model[2],model[3],model[4],model[5],app_name,version))
        spark.createDataFrame(model_lst,schema).toDF("model_name","model_location","model_type","sequence","active_flag","is_champion","app_name","version").createOrReplaceTempView("new_model")
        spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
        app_model_df=spark.sql("select * from new_model union select model_name,model_location,model_type,sequence,'N',is_champion,app_name,version from lambda_models.model_repository where app_name='{0}'".format(app_name))
        app_model_df.persist( pyspark.StorageLevel.MEMORY_AND_DISK_2)
        app_model_df.createOrReplaceTempView("final_records")
        app_model_df.show()
        spark.sql("alter table lambda_models.model_repository drop partition (app_name='{0}')".format(app_name))
        app_model_df.show()
        spark.sql("insert into lambda_models.model_repository partition (app_name,version) select * from final_records")
            
            
    @staticmethod
    def get_model_pipeline(app_name):
        model_object_list=[]
        model_location_list=spark.sql("select model_location,model_type,version from (select *,dense_rank() over (order by version DESC) as rnk from lambda_models.model_repository where app_name='{0}' and active_flag='Y') inv where inv.rnk=1 order by inv.sequence".format(app_name)).collect()
        for pl_models in model_location_list:
            print("yo bro")
            print(pl_models["model_location"])
            model_obj = PipelineModel.load(pl_models["model_location"])    
            model_object_list.append((model_obj,pl_models["model_type"]))
        return model_object_list

'''
schema = StructType([StructField('model_name', StringType(), True),StructField('model_location', StringType(), True)])
dict_spk=[("a","b"),("c","d")]
spark.createDataFrame(dict_spk,schema).show()
        
drop table if exists lambda_models.model_repository;

create table lambda_models.model_repository (
model_name string,
model_location string,
model_type varchar(2),
sequence int,
active_flag varchar(1),
is_champion varchar(1)
)
partitioned by (app_name string,version int);
'''