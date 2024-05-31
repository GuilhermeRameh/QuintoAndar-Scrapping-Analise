r"""°°°
# Gegraphical data clustering
°°°"""
#|%%--%%| <cPmSHzi4Ko|4di5OUS70t>

import pandas as pd
import matplotlib.pyplot as plt
from geopy.geocoders import Nominatim, ArcGIS, Photon
from geopy.extra.rate_limiter import RateLimiter
from sklearn.cluster import DBSCAN
import numpy as np
from geopy.exc import GeocoderUnavailable

#|%%--%%| <4di5OUS70t|WBTgqosukn>

geolocator = Nominatim(user_agent='em_nome_do_pai')
# geolocator = Photon()
# geolocator = ArcGIS(username='cwrneiro', password='Chronos.42progoogle', referer='http://www.example.com')
delayed_geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

#|%%--%%| <WBTgqosukn|M8P8J2blTh>

df = pd.read_parquet('stock.parquet')#.sample(n=50)
df.shape

#|%%--%%| <M8P8J2blTh|hTlPK3bmGy>

df['full_address'] = df.street + ', ' + df.neighborhood
df.full_address

#|%%--%%| <hTlPK3bmGy|GUyKjN7jrh>

def get_lat_long(address: str):
    location = delayed_geocode(address)
    
    if location is not None:
        return location.latitude, location.longitude

    return None, None

#|%%--%%| <GUyKjN7jrh|eODOt2qZPE>

'''
df[['latitude', 'longitude']] = df.full_address.apply(lambda address: pd.Series(get_lat_long(address)))
df
'''

df[['latitude', 'longitude']] = None, None

n_rows = 50

for i in range(len(df.index)//n_rows):
    print(f'Getting coordinates for columns {i*n_rows} to {(i+1)*n_rows}')

    df.iloc[i*n_rows:(i+1)*n_rows, [df.columns.get_loc(c) for c in ['latitude', 'longitude']]] = df.iloc[i*n_rows:(i+1)*n_rows].full_address.apply(lambda address: pd.Series(get_lat_long(address)))

    print(f'Success, with {df.iloc[i*n_rows:(i+1)*n_rows].isna().sum()}% na')


    file_path = f'data/chunk_{i}.parquet'
    
    print(f'Saving to {file_path}')

    df.iloc[i*n_rows:(i+1)*n_rows].to_parquet(file_path)

    print('Saved')

#|%%--%%| <eODOt2qZPE|8KzMNOoN23>
h
r"""°°°
é interessante rodar esse geocoding com todos os dados e salvar em parquet, pq tem que fazer devagar o geocoding pra n dar erro de mta request
°°°"""
#|%%--%%| <8KzMNOoN23|kMnZZVlHXd>

from pathlib import Path

data_dir = Path('data')
df = pd.concat(
    pd.read_parquet(parquet_file)
    for parquet_file in data_dir.glob('*.parquet')
)
df.shape

#|%%--%%| <kMnZZVlHXd|pZ4Iqwc9dL>

df = df.dropna(
    subset=['latitude', 'longitude']
)
df.shape

#|%%--%%| <pZ4Iqwc9dL|IbOWjqUTmr>

plt.scatter(df.longitude, df.latitude, alpha=0.4)

#|%%--%%| <IbOWjqUTmr|vwAeMTWVB2>

db = DBSCAN(eps=0.2, min_samples=3).fit(df[['longitude', 'latitude']])
labels = db.labels_

#|%%--%%| <vwAeMTWVB2|duTOWNADVB>

df['cluster'] = labels

#|%%--%%| <duTOWNADVB|ohVmIDe5RA>

n_clusters_ = len(set(labels)) - (1 if -1 in labels else 0)
n_noise_ = list(labels).count(-1)

#|%%--%%| <ohVmIDe5RA|XoJHGPNHi9>

print("Estimated number of clusters: %d" % n_clusters_)
print("Estimated number of noise points: %d" % n_noise_)

#|%%--%%| <XoJHGPNHi9|qp79KHd4dl>

unique_labels = set(labels)
core_samples_mask = np.zeros_like(labels, dtype=bool)
core_samples_mask[db.core_sample_indices_] = True

colors = [plt.cm.Spectral(each) for each in np.linspace(0, 1, len(unique_labels))]
for k, col in zip(unique_labels, colors):
    if k == -1:
        # Black used for noise.
        col = [0, 0, 0, 1]

    class_member_mask = labels == k

    xy = df[class_member_mask & core_samples_mask]
    plt.plot(
        xy['longitude'],
        xy['latitude'],
        "o",
        markerfacecolor=tuple(col),
        markeredgecolor="k",
        markersize=14,
    )

    xy = df[class_member_mask & ~core_samples_mask]
    plt.plot(
        xy['longitude'],
        xy['latitude'],
        "o",
        markerfacecolor=tuple(col),
        markeredgecolor="k",
        markersize=6,
    )

plt.title(f"Estimated number of clusters: {n_clusters_}")
plt.show()

#|%%--%%| <qp79KHd4dl|MbSvYK54rM>

df.groupby('cluster').describe()

