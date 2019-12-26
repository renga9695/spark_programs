import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime,timedelta
import xlrd
current=datetime.now()
dat= current - timedelta(days=12)
dat=dat.strftime("%Y-%m-%d")
previous= current - timedelta(days=13)
yest= previous.strftime("%Y-%m-%d")
#current,previous,dat,yest

datas = pd.read_excel(r"C:\Users\user1\Desktop\python_files\massive_data\cablevision.20190201.20190228.xlsx","Worksheet",skiprows=2)
pro=datas[["MSO","DMA",dat,yest]][(datas.MSO=="Cequel III")]
x = datas[dat][datas.MSO=="Cequel III"]
y= datas["DMA"][datas.MSO=="Cequel III"]
plt.bar(y,x,label= dat)
plt.xlabel("DMA")
plt.ylabel("Counts")
plt.title("Cequel III")
plt.xticks(rotation=75)
plt.subplots_adjust(right=1.00,left=0.75,top=1.00)
plt.legend()
plt.tick_params(axis='x',which='major',labelsize=7.81,direction='out')

plt.show()