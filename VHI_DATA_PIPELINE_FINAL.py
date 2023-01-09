# importing all the packages
# --------------------------
import numpy as np
import os
import os
import csv
import pyodbc
import pandas as pd
import shutil
import os
from datetime import datetime

# getting information about all the files in the directory
# -------------------------------------------------------- 

def info():
    try:
        os.chdir(r'\\Hasserver\vhi')
        
        Sent = 'sent_IR'
        
    except OSError as err:
        print("Error in the directory mentioned:", err)
    finally:
        print('The directory has been found successfully')
        counter = 0
        jpg = 0
        pdf = 0
        othe = 0
        for f in os.listdir():
            file_name, file_exc = os.path.splitext(f)
            counter += 1
            if file_exc == '.jpg':
                jpg +=1
            elif file_exc == '.PDF':
                pdf +=1
            
            else:
                othe +=1
                          
    print('Total number of files in the folder:' + str(counter))
    print('Total number of jpg files:'+ str(jpg))
    print('Total number of pdf files:'+ str(pdf))
    print('The number of folders:' + str(othe))
    
info()

# backing up the file in seperate directory
#-----------------------------------------------

z = 0
now = datetime.now()
dt_string = now.strftime("%d.%m.%Y_%H.%M") 
# path to source directory
src_dir = r'\\Hasserver\vhi'
# path to destination directory
dest_dir = r'\\Hasserver\vhi\Archive'

os.chdir(src_dir)

for f in os.listdir():                
    file_name, file_exc = os.path.splitext(f)
    file_name_split = file_name.split('_')
    size = len(file_name_split)
    if size == 8:
        try:
            source = src_dir + '\\' + file_name + file_exc
            file_name = file_name + '_'+ dt_string + file_exc
            destination = dest_dir + '\\' + file_name
            print(destination)
        except:
            pass
        finally:
            shutil.copy(source, destination)
            print('copyed successfully')
            # z+=1
            # print(z)
            
            
print('BACKUP SUCCESSFUL')


# getting data nfrom IBM DB2 froma temporary view table
# ------------------------------------------------------

def conn():
    global Hdpr_Df
    try:
        conn = pyodbc.connect('DSN=HASP720;UID=IBRAHIM;PWD=HOSFEB!21')
        sql = "Select CONSULTANT, INSURECON from HDCIRFP where INSURECODE='VHI'" 
    except:
        print('There is an error in the connection')
        
    finally:
        Hdpr_Df = pd.read_sql(sql,conn)
        Hdpr_Df.drop(Hdpr_Df[Hdpr_Df['CONSULTANT'] == 813].index, inplace = True)
        Hdpr_Df.drop(Hdpr_Df[Hdpr_Df['CONSULTANT'] == 851].index, inplace = True)
        Hdpr_Df.drop(Hdpr_Df[Hdpr_Df['CONSULTANT'] == 854].index, inplace = True)
        Hdpr_Df.drop(Hdpr_Df[Hdpr_Df['CONSULTANT'] == 872].index, inplace = True)
        Hdpr_Df.drop(Hdpr_Df[Hdpr_Df['CONSULTANT'] == 852].index, inplace = True)
        Hdpr_Df['CONSULTANT'] = Hdpr_Df['CONSULTANT'].astype(int)
        Hdpr_Df['INSURECON'] = Hdpr_Df['INSURECON'].astype(int)
        Hdpr_Df.to_csv(r'\\Hasserver\vhi\Archive\IBM_DB2_DF.csv', index=False)
        print('The connection has been established successfully')
        

conn()


# getting the file names existing in the directory
# ------------------------------------------------

print('please find the below list of files that exist in the directory')
counter = 0
other = 0
columns = ['IAS' , 'PRACT', 'LISTING' , 'SFTP','Date','Account Management','INSURECON','Policy Number']
df = pd.DataFrame(columns=[columns])

for f in os.listdir():
    counter += 1
    file_name, file_exc = os.path.splitext(f)
    file_name_split = file_name.split('_')
    
    size = len(file_name_split)
    if size == 8:
        df.loc[len(df)] = file_name_split
    else:
        other +=1
        print('The number of files in unusual names:' + ' '+ str(other))   
    


print('All the file name has been uppended to the dataframe df')    


# performing transformation in all the directory
# ----------------------------------------------

def etl():
    global df
    global df_rough   
    df = df.drop(['IAS', 'PRACT','SFTP','Policy Number','LISTING'], axis=1, level=0)
    df.to_csv(r'\\Hasserver\vhi\Archive\file_name.csv', index=False)
    df_rough = pd.read_csv (r'\\Hasserver\vhi\Archive\file_name.csv')
    Hdpr_Df = pd.read_csv (r'\\Hasserver\vhi\Archive\IBM_DB2_DF.csv')
    df_rough['Date_1'] = df_rough['Date'].astype(str)
    df_rough['Year'] = df_rough['Date_1'].str[0:4]
    df_rough['Month'] = df_rough['Date_1'].str[4:6]
    df_rough['Day'] = df_rough['Date_1'].str[6:8]
    df_rough.drop('Date_1', axis=1, inplace=True)
    df_rough = df_rough.assign(CONSULTANT = np.nan)
etl()


# filling the consultant number by cheking it with ethe IBM DB2 database [to make a joint in the table]
# ---------------------------------------------------------------------------------------------------------

count = 0
a = 0
b = 0
Null = df_rough['CONSULTANT'].isna().sum()

for index, row in df_rough.iterrows():
    a+=1
    value_1 = row["INSURECON"]
    Null = df_rough['CONSULTANT'].isna().sum()
    # print(index)
    if Null == 0:
        break
    else:
        for index_1, row in Hdpr_Df.iterrows():
            value_2 = row["INSURECON"]
            value_3 = row["CONSULTANT"]
            if value_2 == value_1:
                df_rough.at[index,'CONSULTANT'] = value_3
                
                
df_rough['CONSULTANT'] = df_rough['CONSULTANT'].replace(np.nan, 0)
df_rough['CONSULTANT'] = df_rough['CONSULTANT'].astype(int)


# will show the number of null values in the final table
# ------------------------------------------------------

Null = df_rough['CONSULTANT'].isna().sum()

print(Null) 


# will rename all the files in the folder 
# ------------------------------------------

os.chdir(r'\\Hasserver\vhi')

count = 0

for index, row in df_rough.iterrows():
    value_1 = row["INSURECON"]
    value_2 = row["Year"]
    value_3 = row["Month"]
    value_4 = row["CONSULTANT"]
    for f in os.listdir():
        file_name, file_exc = os.path.splitext(f)
        file_name_split = file_name.split('_')
        size = len(file_name_split)
        if size == 8:
            item =file_name_split[6]
            value_CN = int(item)
            if value_CN == value_1:
                count +=1
                # print(count)
                # print('{}.{}.{}({})_{}{}'.format(value_4,value_2,value_3,value_1,count,file_exc))
                new_name = '{}.{}.{}({})_{}{}'.format(value_4,value_2,value_3,value_1,count,file_exc)
                os.rename(f, new_name) 
        else:
            pass
        
        
        ### --------------- CODE EXCUTED ------------------###