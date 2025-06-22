import pyodbc

connection_string = (
    "DRIVER={SQL Server};"
    "SERVER=DESKTOP-O1721LD;"
    "DATABASE=MIKI_SHOP;"
    "UID=sa;"
    "PWD=d11052003;"
)

BATCH_SIZE = 500

def insert_batch(data_chunk):
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    cursor.fast_executemany = True
    # data_chunk là 1 danh sách 7 phần tử, mỗi phần tử là 1 list chứ 10000 records
    # tuple row là một tuple chứa các giá trị của một hàng trong data_chunk
    # tuple là một kiểu dữ liệu trong Python, tương tự như danh sách nhưng không thể thay đổi
    values = [tuple(row) for row in data_chunk]
    print("Số lượng bản ghi:", len(values))
    for i in range(0, len(values), BATCH_SIZE):
        batch = values[i:i + BATCH_SIZE]
        cursor.executemany(
            "INSERT INTO orders_csv (order_id, order_date, customer_id, status) VALUES (?, ?, ?, ?)",
            batch
        )
        conn.commit()
    conn.close()
    return len(data_chunk)
