import pandas as pd 
import numpy as np

babynames = pd.Series([
    'Jathonathon', 'Zeltron', 'Ruger', 'Phreddy', 'Ruger', 'Chad', 'Chad',
    'Ruger', 'Ryan', 'Ruger', 'Chad', 'Ryan', 'Phreddy', 'Phreddy', 'Phreddy',
    'Mister', 'Zeltron', 'Ryan', 'Ruger', 'Ruger', 'Jathonathon',
    'Jathonathon', 'Ruger', 'Chad', 'Zeltron'], dtype='string')

# đếm số lần xuất hiện và lọc ra các tên Chad, Ruger, Zeltron


count_names = babynames.value_counts().loc[["Chad","Ruger","Zeltron"]]

#print( count_names )
bees = pd.Series([True, True, False, np.nan, True, False, True, np.nan])
knees = pd.Series([5,2,9,1,3,10,5,2], index = [7,0,2,6,3,5,1,4])
#nếu giá trị bees là Nan thì x2 giá trị bên knees tương ứng index

#print ( knees[ pd.isna(bees).to_numpy() ] * 2 )

# in ra xe có giá hỏi bé hơn giá hợp lý
asking_prices = pd.Series([5000, 7600, 9000, 8500, 7000], index=['civic', 'civic', 'camry', 'mustang', 'mustang'])
fair_prices = pd.Series([5500, 7500, 7500], index=['civic', 'mustang', 'camry'])


mapping_price = asking_prices.index.map(fair_prices) 
smaller_prices = asking_prices < mapping_price

#print( asking_prices[smaller_prices] )

generator = np.random.default_rng(123)
beef_prices = pd.Series(
    data = np.round(generator.uniform(low=3, high=5, size=10), 2),
    index = generator.choice(10, size=10, replace=False)
)

sorted_series_by_index = beef_prices.sort_index()

the_difference_two_element = sorted_series_by_index.diff()

print( sorted_series_by_index )
print( the_difference_two_element.idxmax() )