"""
Là một thư viện xử lý, phân tích dữ liệu, thậm chí là cả ML 
Cung cấp API đọc dữ liệu từ nhiều nguồn khác nhau
Tích hợp với File Formats, Databases, REST APIs
"""
import pandas as pd 

"""
DataFrame là cấu trúc dữ liệu giống bảng 
"""

headers = ['order_id', 'order_date', 'customer_id', 'status']
# Chỉ định headers=0 thì sẽ lấy dòng đầu tiên làm tên cột
df = pd.read_csv('./data/retail_db/part-00000',names=headers)
 #print(df)

#Lọc dữ liệu bằng truy vấn với pandas
df_filtered = df.query("status == 'CLOSED'")

#Chú ý tên cột trong điều kiện truy vấn phải là tên cột trong DataFrame
#print(df_filtered)

#Lấy giá trị duy nhất trong 1 cột
unique_status = df['status'].unique()
#print("Unique status:", unique_status)

#sử dụng filter như IN trong SQL
filtered_df = df.query("status == ('CLOSED', 'PENDING')")
#print("Filtered DataFrame:\n", filtered_df)

# Nhóm dữ liệu và tính toán với pandas 
# groupby() là hàm dùng để nhóm dữ liệu theo một hoặc nhiều cột
# kết hợp với hàm agg() để tính toán thống kê như đếm, trung bình, tổng, ...
#print( df.groupby("status").agg({"order_id": "count"}) )

#hàm apply được xử dụng để áp dụng một logic tùy chỉnh lên từng cột hoặc hàng trong DataFrame
# axis 0 là áp dụng lên từng cột, axis 1 là áp dụng lên từng hàng

df['sum'] = df.apply(lambda row:row['customer_id'] + row['order_id'],axis=1)

#print("DataFrame after applying custom logic:\n", df)

customers = pd.DataFrame({
    'id' : [1, 2, 3],
    'name' : ['Alice', 'Bob', 'Charlie'],
})

orders = pd.DataFrame({
    'order_id' : [101, 102, 103, 104],
    'customer_id' : [1, 2, 3, 1],
    'amount' : [250, 150, 300, 220]
})

orders_customers = pd.merge(orders,customers,left_on='customer_id', right_on='id',how='inner'); 

result = orders_customers.agg({'amount': 'sum', 'id':'count'});

result2 = orders_customers.groupby('customer_id').agg({'amount': 'sum', 'id':'count'})

a = orders_customers.groupby('customer_id').agg({'amount': 'sum', 'id':'count'}).query("id > 0")

a.sort_values(by='amount',ascending=False)

print("Orders customers DataFrame:\n", a.sort_values(['amount','id'],ascending=[False,True]))

a.to_json('./data/retail_db/orders_customers.json',orient='records',lines=True)

print( pd.read_json('./data/retail_db/orders_customers.json',orient='records',lines=True) )

#orient = 'records' sẽ lưu mỗi dòng là một JSON object
#lines = True sẽ lưu mỗi dòng là một JSON object riêng biệt, giúp dễ dàng đọc và ghi
#print("Merged DataFrame :\n", orders_customers)