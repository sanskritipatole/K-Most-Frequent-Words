import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import MinMaxScaler

# Load the dataset
data = pd.read_csv('newdata.csv')

# Select the relevant features for clustering
features = ['requesting_host', 'datetime', 'request', 'status', 'response_size']
df = data[features]

# Preprocess the datetime feature
df['datetime'] = pd.to_datetime(df['datetime'])
df['datetime'] = df['datetime'].astype('int64') // 10**9  # Convert to Unix timestamp

# Scale the numerical features
scaler = MinMaxScaler()
df[['status', 'response_size']] = scaler.fit_transform(df[['status', 'response_size']])

# Perform clustering using K-means
k = 3  # Number of clusters
kmeans = KMeans(n_clusters=k, random_state=42)
df['cluster'] = kmeans.fit_predict(df[['datetime', 'status', 'response_size']])

# Print the cluster assignments
print(df[['requesting_host', 'cluster']])

# Get cluster statistics
cluster_counts = df['cluster'].value_counts()
print('\nCluster statistics:')
print(cluster_counts)
