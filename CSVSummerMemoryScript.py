import pandas as pd
import numpy
import glob, os




str1 = "*_LOSS4CSV_*.csv"
str2 = r'c:\LosstoCsvTest'
os.chdir(str2)
files = glob.glob(str1)
print(files)



for file in files:
   with open('c:\data1.txt', 'a') as f:

      tp = pd.read_csv(file , encoding='utf-16', sep=',', low_memory = False, chunksize=2, iterator=True,header=False)


      for chunk in tp:

         header = chunk.columns


         chunk.to_csv(f, sep=',', encoding='utf-8', index=False, header= False)



      # #df = pd.concat(list(tp) , ignore_index=True)
      # tp = pd.DataFrame(tp).transpose()
      # tp.to_csv(f, sep=',', encoding='utf-8', index=False)