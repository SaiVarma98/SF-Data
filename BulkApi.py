

#               importing Salesforce Libraries

import pandas as pd
from simple_salesforce import SalesforceLogin,SFType,Salesforce,bulk
import sqlalchemy as sa    
import urllib 
import time
import configparser 
import pyodbc
import datetime
import numpy as np
    




cp=configparser.ConfigParser()
cp.read("configfile.ini")

Legacy_db=cp["Legacy_DB"]
sfdc=cp["SFDC"]

start_time=time.time()



#              Salesforce Connection Establishment

try :
   session_id,instance=SalesforceLogin(username=sfdc['u_name'], password=sfdc['u_pass'], security_token=sfdc['security_token'],domain=sfdc['domain'])
   sf=Salesforce(instance=instance,session_id=session_id) 

except :
   print(" Unable to connect to Salesforce. Please check your connection details in config file.\n ")
   exit()


    
if  sf :
    print("\nSuccessfully Connected to Salesforce....")
    
   

    
   
#  Data Retriving From Salesforce
   
obj=input("Enter Object name : ")

#check =input("Press YES/Y if you want to enter a manual Soql Query ....")

#if check.upper() == "YES" or check.upper() == 'Y':
#if 1==0 :
    #soql=input("Enter your Soql Query manually..")


#else :
if 1==1:
   try :
      print("\n  Getting Field list, please wait  \n")
      meta_soql="select Id, EntityDefinition.QualifiedApiName,MasterLabel, Name, QualifiedApiName, Label, Length,DataType, DurableId,IsAutonumber, IsCalculated, IsCreatable, IsCompound, IsDependentPicklist, IsNameField, IsUnique, IsUpdatable, RelationshipName from EntityParticle  where EntityDefinitionId='"+obj+"'"
      Entity_Particle=bulk.SFBulkType('EntityParticle',session=sf.bulk.session,headers=sf.bulk.headers,bulk_url=sf.bulk_url)
      meta_data=pd.DataFrame(Entity_Particle.query_all(meta_soql)).drop('attributes',axis=1)
      meta_data['EntityDefinition']=meta_data['EntityDefinition'][0]['QualifiedApiName']
      compund_list=list(meta_data[meta_data['IsCompound']==True]['Name'])
      object_sf=SFType(obj,session_id,instance)
      object_meta=object_sf.describe().get('fields')
      object_meta=pd.DataFrame(object_meta)
      metadata_list=object_meta['name'].to_list()
      for i in compund_list:
          metadata_list.remove(i)

      soql='select '+','.join(metadata_list)+' from '+obj


   except :
      print("unable to get records... Please check the Object name")
      exit()

#print(soql)
#print("step 2 completed")

object_sf_bulk=bulk.SFBulkType(obj,session=sf.bulk.session,headers=sf.bulk.headers,bulk_url=sf.bulk_url)
target_data=pd.DataFrame(object_sf_bulk.query_all(soql))
dc_col=target_data.columns.to_list()
dc={}
dele =   [i for i in dc_col if type(target_data[i][0])==type(dc)]
if len(dele):

    target_data=target_data.drop(dele,axis=1)
    datetime_list = list(meta_data[meta_data['DataType']=='datetime']['Name'])

    for j in datetime_list:
         target_data[j]=target_data[j].apply(pd.to_datetime,errors='coerce',yearfirst=True,unit='ms',origin='unix')





#print("step 3 completed")
if len(target_data) != 0:

   csv_check =input("Press YES/Y if you want to downlaod a CSV File....")

   if csv_check .upper() == "YES" or csv_check .upper() == 'Y':
          try :
             target_data.to_csv("{}.csv".format(obj))
             print("\n CSV file Downladed Successfully")

          except :
             print(" Some issue in Downloading CSV file. Please check your code")

   else :
      print("You have not opted valid option, so skipping csv download")
   
  

 
#   Data base Connectivity
   print("connecting to database....\n\n")
   server= Legacy_db['server'] #   FG5ZQL3
   database = Legacy_db['database']
   try:     
     if Legacy_db['trusted_conn']=='True' :

         conn_str = urllib.parse.quote_plus("Driver={SQL Server};"
                   "Server="+Legacy_db['server']+";"
                   "Database="+Legacy_db['database']+";"
                   "Trusted_Connection=yes;")
     else:
         conn_str = urllib.parse.quote_plus("Driver={SQL Server};"
                    "Server="+Legacy_db['server']+";"
                    "Database="+Legacy_db['database']+";"
                    "UID="+Legacy_db['u_name']+";"
                    "PWD="+Legacy_db['u_pass']+";")

 
     engine = sa.create_engine('mssql+pyodbc:///?odbc_connect={}'.format(conn_str))   
        
     target_data.to_sql(obj , engine, schema='dbo', if_exists='replace', index=False) #add table name account
     print("{} Records  copied Successfully".format(len(target_data)))

   except:
     print("Connection attempt to Database failed, Please check your Database Credintials in Config file")
     
   
   
else:
    print("No records found. Please check source data...")
   
#print("step 4 completed")
        
    

print("\nExceution time : {} sec".format(time.time()-start_time))
#time.sleep(30)







    






