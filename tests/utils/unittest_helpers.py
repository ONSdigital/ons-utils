# Import pyspark packages
from pyspark.sql import types as T
from epds_utils.testing.pyspark_test_case import PySparkTest

# Import python packages
import pandas as pd
import os
import subprocess
import unittest
from importlib import reload

# import helper module
from epds_utils.hdfs import hdfs_utils

#import module to test
from cprices.cprices.utils import helpers


class TestUtils(PySparkTest): 
    """Test class for utils functions: create_dataframe and find.""" 

    def input_data_find(self):
        """Create unit test input data for find"""

        input_data = [(12345, 1), (23456, 2)]
        schema = T.StructType([
            T.StructField("col1", T.IntegerType(), False),
            T.StructField("col2", T.IntegerType(), False)
        ])
        df1 = self.spark.createDataFrame(input_data, schema)
        
        input_data = [(10000, 1), (20000, 2)]
        schema = T.StructType([
            T.StructField("col1", T.IntegerType(), False),
            T.StructField("col2", T.IntegerType(), False)
        ])
        df2 = self.spark.createDataFrame(input_data, schema)         
        
        dictionary = {
            "key1" : "value1",
            "key2": {
                "subkey1" : df1,
                "subkey2" : {
                    "subkey3" : df2,
                    "subkey4" : "value2"
                }
            }
        }
        
        return df1, df2, dictionary

    def test_find(self):
        """Unit test for find"""
        
        df1, df2, dictionary = self.input_data_find()
        
        # Subtest 1
        key = "key1"
        dictionary = {"key1" : "value1"}
        value = "value1"
        generator = helpers.find(key, dictionary)
        for v in generator:            
            self.assertEqual(value, v)
            
        # Subtest 2
        key = "subkey1"
        generator = helpers.find(key, dictionary)
        for v in generator:            
            self.assertDFEqual(df1, v, rounding_scale = 3)
            
        # Subtest 3 
        key = "subkey3"
        generator = helpers.find(key, dictionary)
        for v in generator:            
            self.assertDFEqual(df2, v, rounding_scale = 3)


if __name__ == "__main__":
    
    suite = unittest.TestSuite()
    #unittest.main(exit=False)
    
    suite.addTest(TestUtils("test_find"))
    
    runner = unittest.TextTestRunner()
    runner.run(suite)
