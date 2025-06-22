# hiệu năng không chỉ là tốc độ mà còn là khả năng mở rộng, bảo trì và sử dụng tài nguyên hiệu quả.
# Bottleneck là điểm yếu trong hệ thống, nơi hiệu suất bị giới hạn hoặc giảm sút.
# Bottleneck có thể do nhiều nguyên nhân như:
# - Thiếu tài nguyên (CPU, RAM, I/O)
# - Thiết kế không tối ưu (thuật toán, cấu trúc dữ liệu)      
# - Tắc nghẽn trong luồng dữ liệu (đọc/ghi đĩa, truy vấn cơ sở dữ liệu, đọc ghi file lớn)


# tối ưu hiệu năng ứng dụng Python : 

# 1. Sử dụng chunk để xử lý dữ liệu lớn, tránh tải toàn bộ dữ liệu vào RAM cùng một lúc.
import pandas as pd
# from sqlalchemy import create_engine
# import psycopg2
# from psycopg2 import sql
# import time
# from contextlib import closing

def process_large_data_in_chunks(file_path, chunk_size=10000):
    """
    Xử lý dữ liệu lớn theo từng khối để tránh quá tải bộ nhớ.
    """
    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        # Thực hiện các phép toán trên từng khối dữ liệu
        process_chunk(chunk)

def process_chunk(chunk):
    """
    Xử lý từng khối dữ liệu.
    """
    # Ví dụ: Lọc dữ liệu, tính toán, v.v.
    filtered_chunk = chunk[chunk['status'] == 'CLOSED']
    # Tiếp tục xử lý dữ liệu đã lọc
    print(filtered_chunk.head())


# 2. Batch insert dùng executemany để ghi nhiều bản ghi vào 1 lần.
def insert_data_batch(conn, data):
    """
    Chèn dữ liệu vào cơ sở dữ liệu theo lô.
    """
    with closing(conn.cursor()) as cursor:
        insert_query = sql.SQL("INSERT INTO orders (order_id, order_date, customer_id, status) VALUES %s")
        psycopg2.extras.execute_values(cursor, insert_query, data)
        conn.commit()

# 3. Sử dụng multiprocessing để xử lý song song.
import multiprocessing
def process_data_in_parallel(data):
    """
    Xử lý dữ liệu song song bằng multiprocessing.
    """
    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        results = pool.map(process_chunk, data)
    return results

# Challenge 
import pyodbc
import time
import pandas as pd
from memory_profiler import profile
import psutil
import os

connection_string = (
    "DRIVER={SQL Server};"
    "SERVER=DESKTOP-O1721LD;"
    "DATABASE=MIKI_SHOP;"
    "UID=sa;"
    "PWD=d11052003;"
)

process = psutil.Process(os.getpid())

@profile
def main():
    start_time = time.time()
    print("Loading CSV...")

    df = pd.read_csv('./data/retail_db/orders/part-00000', names=['order_id', 'order_date', 'customer_id', 'status'])
    print(f"CSV Loaded: {len(df)} rows")
    print(f"RAM after loading CSV: {process.memory_info().rss / 1024 ** 2:.2f} MB")

    with pyodbc.connect(connection_string) as conn:
        cursor = conn.cursor()

        insert_start = time.time()
        for index, row in df.iterrows():
            if index % 1000 == 0 and index > 0:
                print(f"Inserted {index} rows. RAM: {process.memory_info().rss / 1024 ** 2:.2f} MB, CPU: {process.cpu_percent()}%")
            cursor.execute(
                "INSERT INTO orders_csv (order_id, order_date, customer_id, status) VALUES (?, ?, ?, ?)",
                row["order_id"], row["order_date"], row["customer_id"], row["status"]
            )
        conn.commit()
        insert_end = time.time()

    end_time = time.time()
    print(f"\nTotal Time: {end_time - start_time:.2f} seconds")
    print(f"Insert Time: {insert_end - insert_start:.2f} seconds")
    print(f"Final RAM usage: {process.memory_info().rss / 1024 ** 2:.2f} MB")
    print(f"Peak CPU usage: {process.cpu_percent()}%")

#if __name__ == "__main__":
    #main()
    #print("Main function")


import pandas as pd
import time
import psutil
import os
from multiprocessing import Pool, cpu_count
from db_utils import insert_batch  

CHUNK_SIZE = 10000

def process_csv_in_chunks(file_path):
    start_time = time.time()
    process = psutil.Process(os.getpid())
    total_inserted = 0

    data_chunks = []
    for chunk in pd.read_csv(file_path, names=['order_id', 'order_date', 'customer_id', 'status'], chunksize=CHUNK_SIZE):
        data_chunks.append(chunk.values.tolist())

    print(f"🔄 Tổng số chunk: {len(data_chunks)} — bắt đầu ghi...")

    with Pool(cpu_count()) as pool:
        results = pool.map(insert_batch, data_chunks)

    total_inserted = sum(results)
    end_time = time.time()

    print(f"\n✅ Tổng bản ghi insert: {total_inserted}")
    print(f"⏱️ Thời gian: {end_time - start_time:.2f}s")
    print(f"🧠 RAM: {process.memory_info().rss / 1024**2:.2f} MB")

if __name__ == "__main__":
    process_csv_in_chunks('./data/retail_db/orders/part-00000')

