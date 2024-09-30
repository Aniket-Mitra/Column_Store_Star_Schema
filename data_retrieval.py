import json
from pathlib import Path
import pandas as pd

### query format very similar to sql format
#ip1='colselect customer_name,product_category, max shipping_Cost from MarketExample'
#ip1='colselect customer_name,product_category, shipping_Cost from MarketExample where customer_name="CATHY HWANG" and product_category=["TECHNOLOGY,FURNITURE"]'
#ip1='colselect customer_name,product_category, shipping_Cost from MarketExample'

#ip1='colselect Month, sum Variable_Costs, Store_Name from MarketData'
#ip1="colselect sum Variable_Costs, Category from MarketData"

#ip1='colselect Territory,Store_Name,Product_Name from MarketData where Product_Name="Boot" and  Store_Name=["ABC Store,Cloth Sohppy"] '




ip2=ip1.split()  #split ip1
ind1=ip2.index('colselect')
#dtr=ip2[ind1+1]
#dtr=dtr.split(',')
ind11=ip2.index('from')
dtr222=ip2[ind11+1]
setter=['groupingby','sum','min','max','mean'] # available aggregation functions on data- summing, min, max, averaging of fact cols based on grouping of specified dimension cols
c1=len(list(set(setter)-set(ip2)))

p1=r'/home/aniket/Downloads/datacol/C:/Users/Aniket/Documents/'
p2=r'/ColFolderFact'
p3=r'/table_info.json'
#places=[r'C:\Users\Aniket\Documents',
    #    r'C:\Users\Aniket\Documents\MarketExample\ColFolderFact',
  #      r'C:\Users\Aniket\Documents\MarketExample\table_info.json']

places=[p1+dtr222+p2,p1+dtr222+p3]

fl = open(places[-1])
table_info=json.load(fl)


df=pd.DataFrame()
if c1==len(setter):
    if 'where' not in ip2:
      i1=ip1.index('colselect')+10
      i2=ip1.index('from')-2
      i3=ip1[i1:i2+1].split(",")
      for jj in i3:
        i4=jj.split()
        dd=pd.read_csv(places[0]+'/'+i4[0]+'.csv')
        df=pd.concat([df,dd],axis=1)
    else:
     all_and=[]
     names_t_f=[]
     ip1a=ip1

     i1=ip1.index('colselect')+10
     i2=ip1.index('from')-2
   
     i3=ip1[i1:i2+1].split(",")
     for jj in i3:
        i4=jj.split()
        dd=pd.read_csv(places[0]+'/'+i4[0]+'.csv')
        df=pd.concat([df,dd],axis=1)


     ap_counter=False
     if '[' in ip1a:
         ip1a=ip1a.replace('[','').replace(']','')
         print('ip1a',ip1a)
         ap_counter=True
         
     
     while True:
       try:
       
          kj_ind1=ip1a.index('"')
          kj_ind2=ip1a[kj_ind1+1:len(ip1a)].index('"')
          
          part1=ip1a[kj_ind1+1:kj_ind1+kj_ind2+1] 
          all_and.append(part1)
          for i in range(kj_ind1,0,-1):
             if ip1a[i]==" ":
                 kj_ind3=i
                 break
          name_to_find=ip1a[kj_ind3+1:kj_ind1-1]
          names_t_f.append(name_to_find)
          ip1a=ip1a[kj_ind1+kj_ind2+2:]
          
       except:
           break 

     ip2_=ip2.index("from")
     splittr=ip2[ip2_+1].split('.')

     #print('SDS',all_and)
     #print('DFD',names_t_f)

     

     if ap_counter==False:
        for z,k in zip(all_and,names_t_f):
           df=df[df[k]==z]
     elif ap_counter==True:
        for z,k in zip(all_and,names_t_f):
           df=df[df[k].isin(z.split(','))]

else:
   fact=[]
   dims=[]
   agg=[]
   i1=ip1.index('colselect')+10
   i2=ip1.index('from')-2
   #print(ip1[i1],ip1[i2])
   i3=ip1[i1:i2+1].split(",")
   for jj in i3:
       i4=jj.split()
       if len(i4)==1:
              dims.append(i4[0])
       else:
              agg.append(i4[0])
              fact.append(i4[1:][0])
   


   for i in dims:
    dd=pd.read_csv(places[0]+'/'+i+'.csv')
    df=pd.concat([df,dd],axis=1)
   for i in fact:
    dd=pd.read_csv(places[0]+'/'+i+'.csv')
    df=pd.concat([df,dd],axis=1)

   if 'sum' in agg:
          df=df.groupby(dims)[fact].sum()
   elif 'mean' in agg:
          df=df.groupby(dims)[fact].mean()
   elif 'max' in agg:
          df=df.groupby(dims)[fact].max()
   elif 'min' in agg:
          df=df.groupby(dims)[fact].min()
   
     
print(df) # final result of query print
df.to_csv(p1+'Query_Result.csv') #storage of final result of query
