import numpy as np
import pandas as pd

generator = np.random.default_rng(123)
beef_prices = pd.Series(
    data = np.round(generator.uniform(low=3, high=5, size=10), 2),
    index = generator.choice(10, size=10, replace=False)
)

print( beef_prices.sort_index().diff().idxmax() )