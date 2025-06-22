import glob 
import pandas as pd
import json

#tác dụng chính là để tìm các file hoặc thư mục theo pattern 
#thường dùng để lấy danh sách file trong thư mục 

"""
Các pattern 
*       : Bất kỳ chuỗi ký tự nào 
?       : Một ký tự bất kỳ 
[abc]   : Một ký tự là a hoặc b hoặc c
"""

#option recursive để tìm kiếm tất cả các thư mục con 

src_file_names = glob.glob("./data/retail_db/*/part-*")

print(src_file_names)

for fileName in src_file_names:
    df = pd.read_csv(fileName,header=None)
    print(f"Shape of file name is {df.shape}")

#get column names from schema 
url = "./data/retail_db/schemas.json"

with open(url,'r') as f:
    schemas = json.load(f)

print(schemas)


def getColumnsTableFromDictionary(schemas,tableName):
    tableInfo = schemas[tableName] 
    return [columnInfo["column_name"] for columnInfo in tableInfo]

print(f"Column in table departments is {getColumnsTableFromDictionary(schemas,"departments")}")

import re

s = 's/a\\b\\c'
re.split('[/\\\]',s)

#nhớ tách / và \ thì viết regex '[/\\\]' trong Python
print( re.split('[/\\\]',s) )

#viết logic in ra tất cả hình dáng table dựa trên cấu trúc thư mục tự phân tích ra 
url2 = "./data/retail_db/*/part-*"

listUrls = glob.glob(url2,recursive=True)

print( re.split('[/\\\]',listUrls[0]) ) 

for file in listUrls:
    print(f"Processing file {file}")
    elementLevelFile = re.split('[/\\\]',file)
    tableName = elementLevelFile[-2]
    print(f"Extracted table name is {tableName}")
    print(f"Getting column name from table")
    columnName = getColumnsTableFromDictionary(schemas,tableName)
    df = pd.read_csv(file,names=columnName)
    print(f"Shape of {tableName} is {df.shape}")

