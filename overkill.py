from kan import *

# create a KAN: 2D inputs, 1D output, and 5 hidden neurons. cubic spline (k=3), 5 grid intervals (grid=5).
model = KAN(width=[3,8,1], grid=8, k=3, seed=0)

#|%%--%%| <Wt1Mxhjp5k|5y6hCMYfrr>

import pandas as pd

df = pd.read_parquet('stock.parquet')
df.info()

#|%%--%%| <5y6hCMYfrr|2wxtMiQZvM>

df = df.filter(
    items=[
        'suite_area',
        'condominium',
        'tax',
        'asking_price'
    ]
)
df.info()

#|%%--%%| <2wxtMiQZvM|DrrrpzklPO>

from sklearn.model_selection import train_test_split

X_train, X_test, y_train, y_test = train_test_split(df.drop('asking_price', axis=1), df.asking_price)

X_train.shape, X_test.shape, y_train.shape, y_test.shape

#|%%--%%| <DrrrpzklPO|bwtCc8OMKO>

import torch

X_train_tensor = torch.tensor(X_train.values)
y_train_tensor = torch.tensor(y_train.values)

X_test_tensor = torch.tensor(X_test.values)
y_test_tensor = torch.tensor(y_test.values)

dataset = {
    'train_input' : X_train_tensor,
    'train_label' : y_train_tensor,
    'test_input'  : X_test_tensor,
    'test_label'  : y_test_tensor,
}

#|%%--%%| <bwtCc8OMKO|VKAxuare3T>

model(dataset['train_input'])
model.plot(beta=100)

#|%%--%%| <VKAxuare3T|cyvUD9u0ni>

model.train(dataset, opt="LBFGS", steps=50, lamb=0.01, lamb_entropy=10.);

#|%%--%%| <cyvUD9u0ni|wPZYfZT8mN>

model.plot()

