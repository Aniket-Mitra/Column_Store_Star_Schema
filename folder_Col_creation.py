import xml.etree.ElementTree as ET
import os
from functools import reduce
import pandas as pd
import json
from pathlib import Path

#XML FILE CONTAINING STAR SCHEMA#
mytree=ET.parse("extra3.xml")
myroot=mytree.getroot()

overall_db_name=list(myroot.attrib.values())

datatype_store={}
cc_dt=1
datatype_store_fct={}  
name_fact_var_cols=[]
dt_list=[]



#FUNCTION TO CHECK DATADATY EG: Correct DATE format
def check_datatypes(good_tbls,datatype_store,type_count=False):
 for i in good_tbls:
       #print(good_tbls[i])
       col_name=good_tbls[i].columns.tolist()
       for j in range(len(col_name)):
            if datatype_store[i][j]=='str':
              try:
                     good_tbls[i][col_name[j]].astype('str')
              except:
                     type_count=True

            elif datatype_store[i][j]=='float':
              try:
                     good_tbls[i][col_name[j]].astype('float')
              except:
                     type_count=True

            elif datatype_store[i][j]=='int':
              try:
                     good_tbls[i][col_name[j]].astype('int')
              except:
                     type_count=True

            elif datatype_store[i][j]=='month':
              
                  if   len(set(good_tbls[i][col_name[j]].values.tolist())-set(['January','February','March','April','May','June','July','August','September','October','November','December']))!=0:
                         #print('agga')
                         type_count=True

            elif datatype_store[i][j]=='date':
              
                  try:
                            #print('yo')
                            pd.to_datetime(good_tbls[i][col_name[j]],format="%d/%m/%Y")
                            
                  except:
                            try:
                                pd.to_datetime(good_tbls[i][col_name[j]],format="%Y-%m-%d")
                            except:
                                type_count=True
 return type_count
     

#Parsing the .xml schema
dimension_name_list=[]
fact_name_list=[]
locations=[]
for x in myroot:
  if x.tag=="DimensionSet":
    for x1 in x:
                               
                               dimension_name_list.append(x1.text.strip('\n'))
                               cc_dt=1
                               for x2 in x1:
                                       cc_dt+=1
                                       if cc_dt==2:
                                              datatype_store[x1.text.strip('\n')]=[x2.attrib['datatype']]
                                       else:
                                              datatype_store[x1.text.strip('\n')].append(x2.attrib['datatype'])
                                             
  elif x.tag=='FactTable':
    fact_tbl_nm=x.text.strip('\n')
    for x2 in x:
      fact_name_list.append(x2.text.strip('\n'))
      if x2.tag=='FactVarSet':
        dt_list.append(x2.attrib['datatype'])
        name_fact_var_cols.append(x2.text.strip('\n'))
    datatype_store_fct['Fact_Table']=dt_list
  elif x.tag=='colStore':
     for x3 in x:
       locations.append(x3.text)


for jj in range(len(locations)):
          locations[jj]=[i for i in locations[jj].split('\\') if len(i)>0]

          

for jjj in range(len(locations)):
          locations[jjj].insert(4,overall_db_name[0])
          locations[jjj][0]+=os.sep
         


counter_same_location=False

location_saver=[]
for i in locations:
    dir=os.path.join(*i).replace("\\","/")
    if not os.path.exists(dir):
      location_saver.append(dir)
    else:
      counter_same_location=True



# CHECK ONE: If data is already stored in column folder in specified location
if counter_same_location==False:


 dim_tbls=[]
 pk={}
 error_count=False
 for x in myroot:
  if x.tag=="DimensionSet":
    for x1 in x:
      dim_tbls.append(x1.text.strip('\n'))
      pk[x1.text.strip('\n')]=x1.attrib['Primary_Key'].split()

 good_tbls={}
 for i in dim_tbls:
  tbl=pd.read_csv(i+'.csv')
  for i1 in pk:
    if i1==i:
        if len(pk[i1])==1:
          if tbl[pk[i1][0]].duplicated().any()==False  and tbl[pk[i1][0]].isnull().sum()==0:
            good_tbls[i]=tbl
          else:
            error_count=True
 no_counter=False
 if error_count!=True:

  dim_tbls2={}
  for i in dim_tbls:
    dim_tbls2[i]=good_tbls[i].columns.tolist()
 




  col_fact={}
  cuntr=1
  for x in myroot:
   if x.tag=="FactTable":
     for i in range(len(x)):
       col_fact[str(x[i].tag)+str(cuntr)]=x[i].text.strip('\n')
       cuntr+=1
 

  
  pk2=[i[0] for i in list(pk.values())]
  pk3=[col_fact[i] for i in col_fact  if 'DimLinks' in i]
  if set(pk2)!=set(pk3):
   no_counter=True


 else:
   no_counter==False

 constraint_counter=False
 fact_tbl={}
 #Check two: If Primary Key Constraint is violeted?
 if no_counter!=True and error_count==False:
            df=pd.read_csv(fact_tbl_nm+'.csv') ####
            dfff=df[name_fact_var_cols]
            fact_tbl['Fact_Table']=dfff
            
            for i in col_fact:
              df_col=df[[col_fact[i].strip('\n')]]
              if 'DimLinks' in i:
                for j in dim_tbls2:
                   if col_fact[i] in dim_tbls2[j]:
                     
                     
                     if len(set(df_col[col_fact[i]].tolist())-set(good_tbls[j][col_fact[i]].tolist()))==0 or \
                        (str(list(set(df_col[col_fact[i]].tolist())-set(good_tbls[j][col_fact[i]].tolist()))[0])=='nan' and\
                        len(set(df_col[col_fact[i]].tolist())-set(good_tbls[j][col_fact[i]].tolist())))==1:
                                 #print('paaaaaaaaaaaaaaaa')
                                 
                                 pass
                                  
                     else:
                                  
                                  constraint_counter=True

 
            # Check three: Foreign Key Constraint check
            if constraint_counter!=True:

                     # check 4: check datatype issue if overall Integrity Constriant os OK
                     type_count=check_datatypes(good_tbls,datatype_store)
                     
                     type_count1=check_datatypes(fact_tbl,datatype_store_fct)
                     
                     if type_count!=True and type_count1!=True:
                          for i in good_tbls:
                                df=pd.merge(left=df,right=good_tbls[i],how='inner')
                          #df.to_csv('D:\\abc.csv',index=None)
                
                          for dir in location_saver:
                                 if not os.path.exists(dir):
                  
                                        os.makedirs(dir)
                 
                          for table_ind in good_tbls:
                             good_tbls[table_ind].to_csv(location_saver[0]+'/'+dim_tbls[list(good_tbls).index(table_ind)]+'.csv',index=None)
                          print('Successful')
            
                          for i in df.columns.tolist():
                   
                              df[i].to_csv(location_saver[1]+'/'+i+'.csv',index=None)
                          path_temp=location_saver[1].split('/')[:-1]
                          base_pth= Path('/'.join(path_temp))
                          
                          jsonpath = base_pth / ("table_info"+".json")
                          
                          y = json.dumps(dim_tbls2)
                          base_pth.mkdir(exist_ok=True)
                          jsonpath.write_text(y)
                     else:
                          print('Datatype Issue')
                          
              
            else: 
                   print('Foreign Key Constraint Issue')
 else:
   print('Sorry PK issue')
else:
  print('Schema with same name already exists')
