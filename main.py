import requests
import time
import os
import pandas as pd
from datetime import datetime
import shutil
# import threading
# from threading import Thread

TOTAL_TARDING_MINUTES=375
INTERVAL=5
REPEAT_CODE=TOTAL_TARDING_MINUTES/INTERVAL
EXPDATE='05-01-2023'
STK1_bse=14650
STK2_bse=14700
STK3_bse=14750
STK4_bse=14800


def DATA_EXTRACT_BSE():
    symbolce1_bse="OPTIDXNIFTY"+str(EXPDATE)+"CE"+str(STK1_bse)+str(".00")    
    symbolce2_bse="OPTIDXNIFTY"+str(EXPDATE)+"CE"+str(STK2_bse)+str(".00")     
    symbolce3_bse="OPTIDXNIFTY"+str(EXPDATE)+"CE"+str(STK3_bse)+str(".00")     
    symbolce4_bse="OPTIDXNIFTY"+str(EXPDATE)+"CE"+str(STK4_bse)+str(".00")      
    #--------------------------------------------------------------------------
    symbolpe1_bse="OPTIDXNIFTY"+str(EXPDATE)+"PE"+str(STK1_bse)+str(".00")
    symbolpe2_bse="OPTIDXNIFTY"+str(EXPDATE)+"PE"+str(STK2_bse)+str(".00")
    symbolpe3_bse="OPTIDXNIFTY"+str(EXPDATE)+"PE"+str(STK3_bse)+str(".00")
    symbolpe4_bse="OPTIDXNIFTY"+str(EXPDATE)+"PE"+str(STK4_bse)+str(".00")
    #--------------------------------------------------------------------------
    COL=["STK","EXP","NIFTY","SYMBOL","OI","CHNGOI","OICHNGPER","TTV","IV","LTP","CHNG","PCHNG","TBQ","TSQ","BQ","BP","AQ","AP","SPOT"]
    url = 'https://www.nseindia.com/api/option-chain-indices?symbol=BANKNIFTY'
    headers={'user-agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
         'accept-language':'n-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7','accept-encoding':'gzip, deflate, br'}
    r=requests.get(url,headers=headers).json()
    data=r["filtered"]["data"]
      
    df=pd.DataFrame(data)
    df.to_csv("RAW_DATA_bse.csv",index=False)
    ndf=pd.read_csv("RAW_DATA_bse.csv")
    ndf=ndf.dropna()
    rows=len(ndf)
     
    for i in range(0,rows):
        STKCE=ndf["CE"].iloc[i]
        STKCE=eval(STKCE)
        STKPE=ndf["PE"].iloc[i]
        STKPE=eval(STKPE)
        CEDF=pd.DataFrame(STKCE,index=[0])
        PEDF=pd.DataFrame(STKPE,index=[0])
        CEDF.to_csv("FILTERED_DATA_bse.csv",index=False,mode='a',header=False)
        PEDF.to_csv("FILTERED_DATA_bse.csv",index=False,mode='a',header=False)
    final=pd.read_csv("FILTERED_DATA_bse.csv")
    final.columns=COL
    final.to_csv("FILTERED_DATA_bse.csv",index=False)
    #os.system('copy FILTERED_DATA_bse.csv LIVE_DATA_bse.csv')
    shutil.copy('FILTERED_DATA_bse.csv','LIVE_DATA_bse.csv')
    
    os.remove('FILTERED_DATA_bse.csv')
    os.remove('RAW_DATA_bse.csv')
    
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    
    
    ce1_stk=final["STK"][(final.SYMBOL==symbolce1_bse)].to_string(index=False)      
    ce2_stk=final["STK"][(final.SYMBOL==symbolce2_bse)].to_string(index=False)     
    ce3_stk=final["STK"][(final.SYMBOL==symbolce3_bse)].to_string(index=False)     
    ce4_stk=final["STK"][(final.SYMBOL==symbolce4_bse)].to_string(index=False)   
    
    pe1_stk=final["STK"][(final.SYMBOL==symbolpe1_bse)].to_string(index=False)      
    pe2_stk=final["STK"][(final.SYMBOL==symbolpe2_bse)].to_string(index=False)     
    pe3_stk=final["STK"][(final.SYMBOL==symbolpe3_bse)].to_string(index=False)     
    pe4_stk=final["STK"][(final.SYMBOL==symbolpe4_bse)].to_string(index=False)       
    
    ce1_ltp=final["LTP"][(final.SYMBOL==symbolce1_bse)].to_string(index=False)
    ce1_iv=final["IV"][(final.SYMBOL==symbolce1_bse)].to_string(index=False)
    ce1_oi=final["OI"][(final.SYMBOL==symbolce1_bse)].to_string(index=False)
    ce1_ttv=final["TTV"][(final.SYMBOL==symbolce1_bse)].to_string(index=False)
    ce1_choi=final["CHNGOI"][(final.SYMBOL==symbolce1_bse)].to_string(index=False)
    
    ce2_ltp=final["LTP"][(final.SYMBOL==symbolce2_bse)].to_string(index=False)     
    ce2_iv=final["IV"][(final.SYMBOL==symbolce2_bse)].to_string(index=False)     
    ce2_oi=final["OI"][(final.SYMBOL==symbolce2_bse)].to_string(index=False)     
    ce2_ttv=final["TTV"][(final.SYMBOL==symbolce2_bse)].to_string(index=False)     
    ce2_choi=final["CHNGOI"][(final.SYMBOL==symbolce2_bse)].to_string(index=False)     
    
    ce3_ltp=final["LTP"][(final.SYMBOL==symbolce3_bse)].to_string(index=False)     
    ce3_iv=final["IV"][(final.SYMBOL==symbolce3_bse)].to_string(index=False)     
    ce3_oi=final["OI"][(final.SYMBOL==symbolce3_bse)].to_string(index=False)     
    ce3_ttv=final["TTV"][(final.SYMBOL==symbolce3_bse)].to_string(index=False)     
    ce3_choi=final["CHNGOI"][(final.SYMBOL==symbolce3_bse)].to_string(index=False)     
    
    ce4_ltp=final["LTP"][(final.SYMBOL==symbolce4_bse)].to_string(index=False)     
    ce4_iv=final["IV"][(final.SYMBOL==symbolce4_bse)].to_string(index=False)     
    ce4_oi=final["OI"][(final.SYMBOL==symbolce4_bse)].to_string(index=False)     
    ce4_ttv=final["TTV"][(final.SYMBOL==symbolce4_bse)].to_string(index=False)     
    ce4_choi=final["CHNGOI"][(final.SYMBOL==symbolce4_bse)].to_string(index=False)         
    
    pe1_ltp=final["LTP"][(final.SYMBOL==symbolpe1_bse)].to_string(index=False)     
    pe1_iv=final["IV"][(final.SYMBOL==symbolpe1_bse)].to_string(index=False)     
    pe1_oi=final["OI"][(final.SYMBOL==symbolpe1_bse)].to_string(index=False)     
    pe1_ttv=final["TTV"][(final.SYMBOL==symbolpe1_bse)].to_string(index=False)     
    pe1_choi=final["CHNGOI"][(final.SYMBOL==symbolpe1_bse)].to_string(index=False)     
    
    pe2_ltp=final["LTP"][(final.SYMBOL==symbolpe2_bse)].to_string(index=False)     
    pe2_iv=final["IV"][(final.SYMBOL==symbolpe2_bse)].to_string(index=False)     
    pe2_oi=final["OI"][(final.SYMBOL==symbolpe2_bse)].to_string(index=False)     
    pe2_ttv=final["TTV"][(final.SYMBOL==symbolpe2_bse)].to_string(index=False)     
    pe2_choi=final["CHNGOI"][(final.SYMBOL==symbolpe2_bse)].to_string(index=False)     
    
    pe3_ltp=final["LTP"][(final.SYMBOL==symbolpe3_bse)].to_string(index=False)     
    pe3_iv=final["IV"][(final.SYMBOL==symbolpe3_bse)].to_string(index=False)     
    pe3_oi=final["OI"][(final.SYMBOL==symbolpe3_bse)].to_string(index=False)     
    pe3_ttv=final["TTV"][(final.SYMBOL==symbolpe3_bse)].to_string(index=False)     
    pe3_choi=final["CHNGOI"][(final.SYMBOL==symbolpe3_bse)].to_string(index=False)     
    
    pe4_ltp=final["LTP"][(final.SYMBOL==symbolpe4_bse)].to_string(index=False)     
    pe4_iv=final["IV"][(final.SYMBOL==symbolpe4_bse)].to_string(index=False)     
    pe4_oi=final["OI"][(final.SYMBOL==symbolpe4_bse)].to_string(index=False)     
    pe4_ttv=final["TTV"][(final.SYMBOL==symbolpe4_bse)].to_string(index=False)     
    pe4_choi=final["CHNGOI"][(final.SYMBOL==symbolpe4_bse)].to_string(index=False)     
        
   
    DIX={"TIME":current_time,"ce1_stk":[ce1_stk],"ce1_ltp":[ce1_ltp],"ce1_iv":[ce1_iv],"ce1_oi":[ce1_oi],"ce1_ttv":[ce1_ttv],"ce1_choi":[ce1_choi],
         "ce2_stk":[ce2_stk],"ce2_ltp":[ce2_ltp],"ce2_iv":[ce2_iv],"ce2_oi":[ce2_oi],"ce2_ttv":[ce2_ttv],"ce2_choi":[ce2_choi],
         "ce3_stk":[ce3_stk],"ce3_ltp":[ce3_ltp],"ce3_iv":[ce3_iv],"ce3_oi":[ce3_oi],"ce3_ttv":[ce3_ttv],"ce3_choi":[ce3_choi],
         "ce4_stk":[ce4_stk],"ce4_ltp":[ce4_ltp],"ce4_iv":[ce4_iv],"ce4_oi":[ce4_oi],"ce4_ttv":[ce4_ttv],"ce4_choi":[ce4_choi],
         "pe1_stk":[pe1_stk],"pe1_ltp":[pe1_ltp],"pe1_iv":[pe1_iv],"pe1_oi":[pe1_oi],"pe1_ttv":[pe1_ttv],"pe1_choi":[pe1_choi],
         "pe2_stk":[pe2_stk],"pe2_ltp":[pe2_ltp],"pe2_iv":[pe2_iv],"pe2_oi":[pe2_oi],"pe2_ttv":[pe2_ttv],"pe2_choi":[pe2_choi],
         "pe3_stk":[pe3_stk],"pe3_ltp":[pe3_ltp],"pe3_iv":[pe3_iv],"pe3_oi":[pe3_oi],"pe3_ttv":[pe3_ttv],"pe3_choi":[pe3_choi],
         "pe4_stk":[pe4_stk],"pe4_ltp":[pe4_ltp],"pe4_iv":[pe4_iv],"pe4_oi":[pe4_oi],"pe4_ttv":[pe4_ttv],"pe4_choi":[pe4_choi]
        }
         

    FINAL=pd.DataFrame(DIX)
    FINAL.to_csv("LIVE_RECORD_bse.csv",mode="a",index=False,header=False)
    print(current_time+' '+'DATA RECORDED SUCCESSFULLY')
    time.sleep(INTERVAL*60)
