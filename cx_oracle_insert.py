#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import cx_Oracle
import math
import logging

# Create and configure logger 
logging.basicConfig(filename=r"C:\Users\Name\Documents\_Automations\FLU\script.log", 
                    format='%(asctime)s %(message)s', 
                    filemode='w',
                    datefmt='%d-%b-%y %H:%M:%S')

# Creating an object 
logger = logging.getLogger() 
  
# set up logging to console 
console = logging.StreamHandler() 
console.setLevel(logging.DEBUG) 

# add the handler to the root logger 
logging.getLogger('').addHandler(console) 
logger = logging.getLogger(__name__)


# Setting the threshold of logger to INFO 
logger.setLevel(logging.INFO)  
 

def latest_file():   
   basepath = r'\\SERVER\sFTP_Data\Folder\keyrec.TXT'
   xferout_df = pd.read_csv(basepath,  sep=';', index_col = False, header=0, dtype=str)
   xferout_df = xferout_df.astype(str)
   xferout_df.rename(columns={'DATE-KEYED':'KEYREC_DT',
                           'KEY-REC#':'KEYREC_NBR',
                           'NDC':'NDC',
                           'NDC DESC':'NDC_DESC',
                           'TRANS IN STR':'RECEIVING_STORE',
                           'TRANSFER OUT STR':'SENDING_STORE',
                           'ITEM #':'ITEM_NBR',
                           'QUANTITY':'QTY',
                           'UNIT COST':'UNIT_COST_SIGN',
                           'UNIT RETAIL':'UNIT_COST',
                           'EXT COST':'EXT_COST',
                           'EXT RETAIL':'EXT_RETAIL',
                           'TRANSFER DATE':'TRANSFER_DT',
                           'STATUS':'STATUS',
                           'FULL/PARTIAL FLAG':'FULL_PARTIAL',
                           })

   return xferout_df

# Function to insert data into oracle
def oracle_insert(df, tablename):
    conn_usr = u'user' #enter your rdw username
    conn_pw = u'password' #enter your rdw password
    dsnStr = cx_Oracle.makedsn("SERVER", "1248", "NAME")

    con = cx_Oracle.connect(user = conn_usr, password = conn_pw, dsn = dsnStr)


    for col in range(1, len(df.columns)+1):
        if col == 1:
           
            inserttext = ':1'
        else:
            
            inserttext = inserttext+', :{}'.format(col)

    sql_query1 = 'TRUNCATE TABLE {}'.format(tablename)
    sqlquery = 'INSERT INTO {} VALUES({})'.format(tablename, inserttext)
    sql_query2 = 'GRANT SELECT ON {} TO PUBLIC'.format(tablename)
    # print(sqlquery)

    # convert the dataframe to a list
    df_list = df.values.tolist()
    cur = con.cursor()
    
 
    cur.execute(sql_query1)
    logger.info("Completed: %s", sql_query1)
    
    for b in df_list :

        for index, value in enumerate(b):
            if isinstance(value, float) and math.isnan(value):
                b[index] = None
            elif isinstance(value, type(pd.NaT)):
                b[index] = None
                
    con.cursor().executemany(sqlquery, df_list)
    logger.info("Completed: %s", sqlquery)


    cur.execute(sql_query2)
    logger.info("Completed: %s", sql_query2)
    logger.info("Preview: %s", df)
    logger.info("Total orders imported: %s", df.shape)
    
    con.close()


df = latest_file()
df

#logger.info("No. of records returned %s", df.shape)
oracle_insert(df, 'USERNAME.KEY_REC_TBL')
logger.info("End of script")

