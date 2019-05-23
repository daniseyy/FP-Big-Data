import os
import logging
import pandas as pd

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql import types
from pyspark.sql.functions import explode
import pyspark.sql.functions as func

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RecommendationEngine:
    """A Yelp recommendation engine
    """

    def __train_all_model(self):
        """Train the ALS model with the current dataset
        """
        
      

        
        #Model 1
        logger.info("Training the ALS model 1")
        self.als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="businessId", ratingCol="Stars", coldStartStrategy="drop")
        self.model1 = self.als.fit(self.df0)
        logger.info("ALS model 1 built!")
        
        #Model 2
        logger.info("Training the ALS model 2")
        self.als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="businessId", ratingCol="Stars", coldStartStrategy="drop")
        self.model2 = self.als.fit(self.df1)
        logger.info("ALS model 2 built!")
        
        #Model 3
        logger.info("Training the ALS model 3")
        self.als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="businessId", ratingCol="Stars", coldStartStrategy="drop")
        self.model3 = self.als.fit(self.df2)
        logger.info("ALS model 3 built!")
        
    def __train_model(self, model):
        """Train the ALS model with the current dataset
        """
        
        logger.info("Training the ALS model...")
        self.als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="businessId", ratingCol="Stars", coldStartStrategy="drop")
        if model == 0:
            self.model1 = self.als.fit(self.df0)
        elif model == 1:
            self.model2 = self.als.fit(self.df1)
        elif model == 2:
            self.model3 = self.als.fit(self.df2)
        logger.info("ALS model built!")

    def get_top_stars(self, model, userId, business_count):
        """Recommends up to business_count top unrated business to userId
        """
        
        if model == 0:
            users = self.df0.select(self.als.getUserCol())
            users = users.filter(users.userId == user_id)
            userSubsetRecs = self.model1.recommendForUserSubset(users, businesss_count)
            userSubsetRecs = userSubsetRecs.withColumn("recommendations", explode("recommendations"))
            userSubsetRecs = userSubsetRecs.select(func.col('userId'),
                                                   func.col('recommendations')['businessId'].alias('businessId'),
                                                   func.col('recommendations')['Stars'].alias('Rating')).\
                                                                                        drop('recommendations')
            userSubsetRecs = userSubsetRecs.drop('Rating')
            userSubsetRecs = userSubsetRecs.join(self.businessdf, ("businessId"), 'inner')
            # userSubsetRecs.show()
            # userSubsetRecs.printSchema()
            userSubsetRecs = userSubsetRecs.toPandas()
            userSubsetRecs = userSubsetRecs.to_json()
            return userSubsetRecs
        elif model == 1:
            users = self.df1.select(self.als.getUserCol())
            users = users.filter(users.userId == userId)
            userSubsetRecs = self.model2.recommendForUserSubset(users, business_count)
            userSubsetRecs = userSubsetRecs.withColumn("recommendations", explode("recommendations"))
            userSubsetRecs = userSubsetRecs.select(func.col('userId'),
                                                   func.col('recommendations')['businessId'].alias('businessId'),
                                                   func.col('recommendations')['Stars'].alias('Rating')).\
                                                                                        drop('recommendations')
            userSubsetRecs = userSubsetRecs.drop('Rating')
            userSubsetRecs = userSubsetRecs.join(self.businessdf, ("businessId"), 'inner')
            # userSubsetRecs.show()
            # userSubsetRecs.printSchema()
            userSubsetRecs = userSubsetRecs.toPandas()
            userSubsetRecs = userSubsetRecs.to_json()
            return userSubsetRecs
        elif model == 2:
            users = self.df2.select(self.als.getUserCol())
            users = users.filter(users.userId == userId)
            userSubsetRecs = self.model3.recommendForUserSubset(users, business_count)
            userSubsetRecs = userSubsetRecs.withColumn("recommendations", explode("recommendations"))
            userSubsetRecs = userSubsetRecs.select(func.col('userId'),
                                                   func.col('recommendations')['businessId'].alias('businessId'),
                                                   func.col('recommendations')['Stars'].alias('Rating')).\
                                                                                        drop('recommendations')
            userSubsetRecs = userSubsetRecs.drop('Rating')
            userSubsetRecs = userSubsetRecs.join(self.businessdf, ("businessId"), 'inner')
            # userSubsetRecs.show()
            # userSubsetRecs.printSchema()
            userSubsetRecs = userSubsetRecs.toPandas()
            userSubsetRecs = userSubsetRecs.to_json()
            return userSubsetRecs

    def get_top_business_recommend(self, model, businessId, user_count):
        """Recommends up to businesss_count top unrated businesss to user_id
        """
        
        if model == 0:
            business = self.df0.select(self.als.getItemCol())
            business = business.filter(business.businessId == businessId)
            businessSubsetRecs = self.model1.recommendForItemSubset(business, user_count)
            businessSubsetRecs = businessSubsetRecs.withColumn("recommendations", explode("recommendations"))
            businessSubsetRecs = businessSubsetRecs.select(func.col('businessId'),
                                                     func.col('recommendations')['userId'].alias('userId'),
                                                     func.col('recommendations')['Stars'].alias('Rating')).\
                                                                                            drop('recommendations')
            businessSubsetRecs = businessSubsetRecs.drop('Rating')
            businessSubsetRecs = businessSubsetRecs.join(self.businessdf, ("businessId"), 'inner')
            # userSubsetRecs.show()
            # userSubsetRecs.printSchema()
            businessSubsetRecs = businessSubsetRecs.toPandas()
            businessSubsetRecs = businessSubsetRecs.to_json()
            return businessSubsetRecs
        elif model == 1:
            business = self.df1.select(self.als.getItemCol())
            business = business.filter(business.businessId == businessId)
            businessSubsetRecs = self.model2.recommendForItemSubset(business, user_count)
            businessSubsetRecs = businessSubsetRecs.withColumn("recommendations", explode("recommendations"))
            businessSubsetRecs = businessSubsetRecs.select(func.col('businessId'),
                                                     func.col('recommendations')['userId'].alias('userId'),
                                                     func.col('recommendations')['Stars'].alias('Rating')).\
                                                                                            drop('recommendations')
            businessSubsetRecs = businessSubsetRecs.drop('Rating')
            businessSubsetRecs = businessSubsetRecs.join(self.businesssdf, ("businessId"), 'inner')
            # userSubsetRecs.show()
            # userSubsetRecs.printSchema()
            businessSubsetRecs = businessSubsetRecs.toPandas()
            businessSubsetRecs = businessSubsetRecs.to_json()
            return businessSubsetRecs
        elif model == 2:
            businesss = self.df2.select(self.als.getItemCol())
            businesss = businesss.filter(businesss.businessId == businessId)
            businessSubsetRecs = self.model3.recommendForItemSubset(business, user_count)
            businessSubsetRecs = businessSubsetRecs.withColumn("recommendations", explode("recommendations"))
            businessSubsetRecs = businessSubsetRecs.select(func.col('businessId'),
                                                     func.col('recommendations')['userId'].alias('userId'),
                                                     func.col('recommendations')['Stars'].alias('Rating')).\
                                                                                            drop('recommendations')
            businessSubsetRecs = businessSubsetRecs.drop('Rating')
            businessSubsetRecs = businessSubsetRecs.join(self.businesssdf, ("businessId"), 'inner')
            # userSubsetRecs.show()
            # userSubsetRecs.printSchema()
            businessSubsetRecs = businessSubsetRecs.toPandas()
            businessSubsetRecs = businessSubsetRecs.to_json()
            return businessSubsetRecs

    def get_stars_for_business_ids(self, model, userId, businessId):
        """Given a user_id and a list of business_ids, predict Stars for them
        """
        
        if model == 0:
            request = self.spark_session.createDataFrame([(userId, businessId)], ["userId", "businessId"])
            Stars = self.model1.transform(request).collect()
            return Stars
        elif model == 1:
            request = self.spark_session.createDataFrame([(userId, businessId)], ["userId", "businessId"])
            Stars = self.model2.transform(request).collect()
            return Stars
        elif model == 2:
            request = self.spark_session.createDataFrame([(userId, businessId)], ["userId", "businessId"])
            Stars = self.model3.transform(request).collect()
            return Stars

   


    def __init__(self, spark_session, dataset_path):
        """Init the recommendation engine given a Spark context and a dataset path
        """
        logger.info("Starting up the Recommendation Engine: ")
        self.spark_session = spark_session
        # Load Stars data for later use
        logger.info("Loading Stars data...")
        
       
     
        file_name1 = 'data_part_1.txt'
        dataset_file_path1 = os.path.join(dataset_path, file_name1)
        exist = os.path.isfile(dataset_file_path1)
        if exist:
            self.df0 = spark_session.read.csv(dataset_file_path1, header=None, inferSchema=True)
            self.df0 = self.df0.selectExpr("_c0 as userId", "_c1 as businessId", "_c2 as Stars")

        file_name2 = 'data_part_2.txt'
        dataset_file_path2 = os.path.join(dataset_path, file_name2)
        exist = os.path.isfile(dataset_file_path2)
        if exist:
            self.df1 = spark_session.read.csv(dataset_file_path2, header=None, inferSchema=True)
            self.df1 = self.df1.selectExpr("_c0 as userId", "_c1 as businessId", "_c2 as Stars")

        file_name3 = 'data_part_3.txt'
        dataset_file_path3 = os.path.join(dataset_path, file_name3)
        exist = os.path.isfile(dataset_file_path3)
        if exist:
            self.df2 = spark_session.read.csv(dataset_file_path3, header=None, inferSchema=True)
            self.df2 = self.df2.selectExpr("_c0 as userId", "_c1 as businessId", "_c2 as Stars")
        # Train the model
        self.__train_all_model()
