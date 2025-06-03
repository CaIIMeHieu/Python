import requests
import json
import random
from datetime import datetime, timedelta
import logging
import time
import warnings
import pyodbc

# Vô hiệu hóa cảnh báo SSL
warnings.filterwarnings("ignore", category=requests.packages.urllib3.exceptions.InsecureRequestWarning)

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# API endpoint
API_URL = "https://localhost:7226/api/Track"

# Kết nối MSSQL và lấy danh sách customer_id
conn_str = (
    "DRIVER={SQL Server};"
    "SERVER=DESKTOP-O1721LD;"
    "DATABASE=MIKI_SHOP;"
    "UID=sa;"
    "PWD=d11052003;"
)
conn = pyodbc.connect(conn_str)
query = "SELECT ID FROM [user]"
cursor = conn.cursor()
cursor.execute(query)
customers = [row.ID for row in cursor.fetchall()]
cursor.close()
conn.close()
logger.info(f"Loaded {len(customers)} customer IDs from MSSQL")

# Dữ liệu từ bảng product
products = [
    {"id": "lj5wpaem", "category_id": 14},
    {"id": "lj5z1j0l", "category_id": 14},  
    {"id": "lj5zdnfy", "category_id": 14},  
    {"id": "lj5zj0r6", "category_id": 14}, 
    {"id": "lj5zmqla", "category_id": 14}, 
    {"id": "lj5zxkjr", "category_id": 15}, 
    {"id": "lj60197a", "category_id": 15}, 
    {"id": "lj604iuu", "category_id": 15},  
    {"id": "lj608inc", "category_id": 15}, 
    {"id": "lj60c701", "category_id": 15},  
    {"id": "lj60gk1d", "category_id": 16}, 
    {"id": "lj60k6ou", "category_id": 16},  
    {"id": "lj60nlrp", "category_id": 16},  
    {"id": "lj60r1jc", "category_id": 16}, 
    {"id": "lj60wcwj", "category_id": 16}, 
    {"id": "lj6121vr", "category_id": 17},  
    {"id": "lj61ejdv", "category_id": 17},  
    {"id": "lj61hmlt", "category_id": 17}, 
]

# Từ khóa tìm kiếm
search_queries = ["dây chuyền", "nhẫn", "lắc tay", "bông tai", "vàng 18K", "ngọc trai", "kim cương"]

def generate_event():
    # Tính thời điểm đầu tháng
    now = datetime.now()
    start_of_month = datetime(now.year, now.month, 1)
    
    # Tính khoảng thời gian từ đầu tháng đến hiện tại (tính bằng giây)
    time_diff = (now - start_of_month).total_seconds()
    
    # Chọn ngẫu nhiên một thời điểm trong khoảng từ đầu tháng đến hiện tại
    random_seconds = random.randint(0, int(time_diff))
    timestamp = (start_of_month + timedelta(seconds=random_seconds)).isoformat()
    
    event_type_weights = {"view": 0.6, "search": 0.3, "add_to_cart": 0.1}
    event_type = random.choices(
        list(event_type_weights.keys()),
        weights=list(event_type_weights.values()),
        k=1
    )[0]
    
    product = random.choice(products) if event_type != "search" else None
    
    event = {
        "customer_id": random.choice(customers),
        "event_type": event_type,
        "product_id": product["id"] if product else None,
        "category_id": product["category_id"] if product else None,
        "search_query": random.choice(search_queries) if event_type == "search" else None,
        "timestamp": timestamp
    }
    return event

def send_events(num_events=1000000, batch_size=1000):
    headers = {"Content-Type": "application/json"}
    for i in range(0, num_events, batch_size):
        batch = [generate_event() for _ in range(min(batch_size, num_events - i))]
        batch = [event for event in batch if all(key in event for key in ["customer_id", "event_type", "product_id", "category_id", "search_query", "timestamp"])]
        if not batch:
            logger.warning("Empty batch after filtering, skipping...")
            continue
        try:
            print(json.dumps(batch, indent=2))
            response = requests.post(API_URL, json=batch, headers=headers, verify=False)
            response.raise_for_status()
            logger.info(f"Sent batch {i//batch_size + 1}: {len(batch)} events")
        except requests.exceptions.HTTPError as e:
            logger.error(f"Error sending batch: {e}")
            logger.error(f"Response: {e.response.text}")
        except Exception as e:
            logger.error(f"Error sending batch: {e}")
        time.sleep(0.1)

if __name__ == '__main__':
    send_events(num_events=9400000)