import pandas as pd

urlPath = './data/Automobile_data.csv'

dataFromCsv = pd.read_csv(urlPath)

print("First five rows of the CSV file:\n", dataFromCsv.head(5))
print("\nLast five rows of the CSV file:\n", dataFromCsv.tail(5))

#chỉ định các giá trị được coi là bị thiếu trong dataset 
dataFromCsv = pd.read_csv(urlPath, na_values=['?','n.a','NaN'])
print("\nData after handling missing values:\n", dataFromCsv.query("price == '?'"))

#chỉ định các giá trị bị thiếu cho nhiều cột khác nhau
dataFromCsv = pd.read_csv(urlPath,na_values={
    'price': ['?','n.a','NaN'],
    'horsepower': ['?','n.a','NaN'],
    'normalized-losses': ['?','n.a','NaN']
})

#dataFromCsv.to_csv(urlPath)
#in ra xe đắt nhất 
#lấy ra dataframe 2 cột
data2 = dataFromCsv[['company','price']]
result = data2[data2.price == data2.price.max()]
print(result)


#in ra tất cả car toyota 
toyotaCar = dataFromCsv[(dataFromCsv.company == 'toyota') & (dataFromCsv.horsepower == 62)]

print(toyotaCar)

#đếm số xe mỗi company
countQuantityResult = dataFromCsv.groupby(["company","body-style"]).agg({
    'company' : ['count'], 
    'price': 'max' 
})

print(countQuantityResult)
countQuantityResult = countQuantityResult.reset_index()
countQuantityResult.to_csv("./data/maxPriceAndQuantity.csv",index=False,header=['Công ty','Kiểu dáng thân','Số lượng','Giá'])

sortCarsByPrice = dataFromCsv.sort_values(by=['price'],ascending=True)

print(sortCarsByPrice)

GermanCars = {'Company': ['Ford', 'Mercedes', 'BMV', 'Audi'], 'Price': [23845, 171995, 135925 , 71400]}
japaneseCars = {'Company': ['Toyota', 'Honda', 'Nissan', 'Mitsubishi '], 'Price': [29995, 23600, 61500 , 58900]}

df1 = pd.DataFrame.from_dict(GermanCars)
df2 = pd.DataFrame.from_dict(japaneseCars)

dfUnion = pd.concat([df1,df2]) 

Car_Price = {'Company': ['Toyota', 'Honda', 'BMV', 'Audi'], 'Price': [23845, 17995, 135925 , 71400]}
car_Horsepower = {'Company': ['Toyota', 'Honda', 'BMV', 'Audi'], 'horsepower': [141, 80, 182 , 160]}

df3 = pd.DataFrame.from_dict(Car_Price)
df4 = pd.DataFrame.from_dict(car_Horsepower)

dfJoin23 = pd.merge(df3,df4,how='inner',on='Company')

print(dfJoin23)


