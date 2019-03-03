#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb 17 17:19:48 2019

@author: lambda
"""

from pyspark.ml.stat import Correlation
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier
from framework.model.repo.model_repo_utils  import ModelRepoUtils
from pyspark.ml import Pipeline

pulsar=sqlContext.read.format('csv').options(header='true', inferSchema='true').load('/pulsar/pulsar_stars.csv')
pulsar.limit(10).show()

pulsar.approxQuantile(" Mean of the integrated profile",[x/100 for x in range(1,101,1)], 0)

r1 = Correlation.corr(pulsar)


dataset = [[Vectors.dense([1, 0, 0, -2])], [Vectors.dense([4, 5, 0, 3])], [Vectors.dense([6, 7, 0, 8])], [Vectors.dense([9, 0, 0, 1])]]

dataset = spark.createDataFrame(dataset, ['features'])


pulsar=pulsar.toDF(*['Mean of the integrated profile', 'Standard deviation of the integrated profile', 'Excess kurtosis of the integrated profile', 'Skewness of the integrated profile', 'Mean of the DM-SNR curve', 'Standard deviation of the DM-SNR curve', 'Excess kurtosis of the DM-SNR curve', 'Skewness of the DM-SNR curve', 'target_class'])



assembler = VectorAssembler(inputCols=['Mean of the integrated profile', 'Standard deviation of the integrated profile', 'Excess kurtosis of the integrated profile', 'Skewness of the integrated profile', 'Mean of the DM-SNR curve', 'Standard deviation of the DM-SNR curve', 'Excess kurtosis of the DM-SNR curve', 'Skewness of the DM-SNR curve'], outputCol='features')

pulsar_vec = assembler.transform(pulsar)

splits=pulsar_vec.randomSplit([0.75,0.25], 1)
train=splits[0]
test=splits[1]

dt = DecisionTreeClassifier(labelCol="target_class", featuresCol="features")

pipeline = Pipeline(stages=[dt])

model=pipeline.fit(train)

ModelRepoUtils.publish_app_models("TEST_APP",[(model,'DT_PULSAR_PREDICTION','ML',1,'Y','Y')])

spark.sql("select * from lambda_models.model_repository").show()

model=ModelRepoUtils.get_model_pipeline("TEST_APP")

type(model)


