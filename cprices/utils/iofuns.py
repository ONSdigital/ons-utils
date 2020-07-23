# import spark libraries
from pyspark.sql import DataFrame

# import python libraries
import os
import copy
from datetime import datetime
from functools import reduce
from importlib import reload


def extract_data(spark, config, dev_config):
    
    """
    Creates a dictionary of spark dataframes from the staged data to feed into 
    the core pipepline.
    
    Parameters
    ----------
    spark : 
        spark session
        
    config : python file/module 
        It has the staged_data dictionary with all the data sources, suppliers
        and items. Each combination (e.g. web_scraped, mysupermarket, laptop)
        is a path of dictionary keys that lead to a value. This is initialised
        as an empty dictionary {}. 
        
    dev_config : python file/module 
        It has the path to the HDFS directory from where the staged data will 
        be imported.
        
    Returns
    -------
    dfs : dictionary of spark dataframes 
        Each path of keys leads to a value/spark dataframe as it was read 
        from HDFS for the corresponding table.
        
    """
    
    staged_dir = dev_config.staged_dir
    staged_data = copy.deepcopy(config.input_data)
    
    # webscraped data
    for data_source in staged_data:
        if data_source == 'web_scraped':
            for supplier in staged_data[data_source]:
                for item in staged_data[data_source][supplier]:
                    path = os.path.join(
                        staged_dir, 
                        data_source, 
                        supplier, 
                        item+'.parquet'
                    )
                    staged_data[data_source][supplier][item] = (
                        spark
                        .read
                        .parquet(path)
                    )
        elif data_source in ['scanner', 'conventional']:
            for supplier in staged_data[data_source]:
                path = os.path.join(staged_dir, data_source,supplier)
                staged_data[data_source][supplier] = spark.read.parquet(path)
    
    return staged_data

    
def load_data(dfs, dev_config):
    
    """
    Stores output dataframes in HDFS.
    
    Parameters
    ----------
    dfs : dictionary of spark dataframes
        The output dataframes from all scenarios to store in HDFS.
        
    dev_config : python file/module 
        It has the path to the HDFS directory where the dfs will be stored.
                         
    Notes
    -----
    The run_id consists of the current date, time and username
    (YYYYMMDD_HHMMSS_username). That's the name of the folder that will be
    created inside the processed data folder in HDFS for this particular 
    run and will contain all the output dataframes. 
    The run_id is printed on the screen for the user to explore the output 
    data.
    
    The configuration dataset is a two-column table where the first column 
    shows the stage of the core pipeline and the second column shows (as a
    dictionary) all the config parameters for the corresponding stage. This
    can be used as a reference for the user in case they want to check the 
    configuration of this run.
        
    """
    
    # create run id using username and current time
    username = os.environ['HADOOP_USER_NAME']
    current_date_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_id = '_'.join([current_date_time, username])
    
    # create directory path to export processed data
    processed_dir = os.path.join(dev_config.processed_dir, run_id)
        
    for name in dfs:
        print(f'{name}...')
        if name in ['analysis']:
            # store analysis output as csv
            df = dfs[name] 
            df = df.repartition(1)
            df.cache().count()
            path = os.path.join(processed_dir, 'analysis')    
            df.write.save(path, header=True, format='csv', mode='overwrite')
        else:
            path = os.path.join(processed_dir, name)
            dfs[name].write.parquet(path)  
            
    return run_id
    
        