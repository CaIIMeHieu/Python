# cần đường dẫn lưu file json, tên thư mục, tên file
# lấy ra được các file csv, đọc và lưu mới file json

import pandas as pd
import glob
import re
import os

csv_path = "./data/retail_db/*/part-*"
json_path = "./data/retail_db_json_file"


def read_csv(filePath):
    listElementFileLevel = re.split(r'[/\\]', filePath)
    folderName = listElementFileLevel[-2]
    jsonFileName = listElementFileLevel[-1]
    df = pd.read_csv(filePath)
    print(f"Dataframe at foreach {df}")
    return df, folderName, jsonFileName

def to_json(df,json_path, folderName, jsonFileName):
    folderPath = f"{json_path}/{folderName}"
    json_file_path = f"{json_path}/{folderName}/{jsonFileName}"
    os.makedirs(folderPath,exist_ok=True)
    df.to_json(json_file_path,orient='records',lines=True)

def processing_read_csv_and_save_new_json_file():
    list_csv_path = glob.glob(csv_path,recursive=True)
    for path in list_csv_path:
        folderName = ""
        jsonFileName = ""
        print(f"Path {path}")
        df, folderName, jsonFileName = read_csv(path)
        to_json(df,json_path,folderName,jsonFileName)

#processing_read_csv_and_save_new_json_file()

print( glob.glob(csv_path,recursive=True) )



