import glob
import json
import os 
import re 
import pandas as pd


schemas = json.load(open('schemas.json', 'r'))

def get_column_names(schemas, ds_name, sorting_key='column_position'):
    column_details = schemas[ds_name]
    print('Column details is ... ',column_details)
    print('\n Type of column_details is ... ', type(column_details))
    column = sorted( column_details, key=lambda x: x[sorting_key])
    return [col['column_name'] for col in column]

def read_csv(file,schemas): 
    file_path_list = re.split('[/\\\]',file)
    ds_name = file_path_list[-2]
    columns = get_column_names(schemas, ds_name)
    df = pd.read_csv(file,names=columns)
    return df

def to_json(df, tgt_base_dir, ds_name, file_name):
    json_file_path = f'{tgt_base_dir}/{ds_name}/{file_name}'
    os.makedirs(os.path.dirname(json_file_path), exist_ok=True)
    df.to_json(json_file_path, orient='records', lines=True)

def file_converter(src_base_dir, tgt_base_dir, ds_name):
    schemas = json.load(open('schemas.json', 'r'))
    files = glob.glob(f'{src_base_dir}/{ds_name}/part-*')
    if len(files) == 0:
        raise FileNotFoundError(f'No files found for dataset: {ds_name} in {src_base_dir}/{ds_name}')
    for file in files:
        print(f'Processing file: {file}')
        df = read_csv(file, schemas)
        file_name = re.split('[/\\\]',file)[-1]
        to_json(df, tgt_base_dir, ds_name, file_name)

def process_files(ds_name=None):
    src_base_dir = 'data/retail_db/'
    tgt_base_dir = 'data/retail_db_json/'
    schemas = json.load(open('schemas.json', 'r'))
    if ds_name:
        file_converter(src_base_dir, tgt_base_dir, ds_name)
    for ds_name in schemas.keys():
        try :
            print(f'Processing dataset: {ds_name}')
            file_converter(src_base_dir, tgt_base_dir, ds_name)
        except FileNotFoundError as e:
            print(f'Error: {e}')

if __name__ == '__main__':
    process_files()

#pass run time argument to python application
# Example: python app.py ds_name
# If ds_name is provided, it will process only that dataset.  
# If no ds_name is provided, it will process all datasets.
# To run the script, use the command:
# python app.py ds_name
# where ds_name is the name of the dataset you want to process.
# If you want to process all datasets, just run:
# python app.py
# This will process all datasets defined in schemas.json.
# The processed files will be saved in the target directory specified in the code.

#Khi thiết lập biến môi trường trong terminal, biến chỉ có hiệu lực trong phiên làm việc hiện tại, 
# tắt terminal sẽ làm mất biến.

#except trong try catch, sẽ không dừng chương trình nếu có lỗi xảy ra,
# mà sẽ in ra lỗi và tiếp tục xử lý các file khác.
# Ví dụ:
# try:  
#     file_converter(src_base_dir, tgt_base_dir, ds_name)
# except Exception as e:
#     print(f'Error processing {ds_name}: {e}')