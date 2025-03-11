import pandas as pd
import numpy as np
from sklearn.cluster import AgglomerativeClustering
from gower import gower_matrix
import matplotlib.pyplot as plt
import seaborn as sns

# Load events data from CSV file
def load_data(csv_file):
    return pd.read_csv(csv_file)

# Preprocess the data (handling mixed data types)
def preprocess_data(data):
    # Convert categorical columns to string
    for column in data.select_dtypes(include=['category', 'object']).columns:
        data[column] = data[column].astype(str)
    return data

# Compute Gower distance matrix
def compute_gower_distance(data):
    return gower_matrix(data)

# Perform Agglomerative Clustering using Gower distance
def perform_clustering(gower_dist_matrix, n_clusters):
    clustering = AgglomerativeClustering(n_clusters=n_clusters, metric='precomputed', linkage='average')
    labels = clustering.fit_predict(gower_dist_matrix)
    return labels

# Visualize the clusters (optional)
def visualize_clusters(data, labels):
    # Since it's mixed data, we only visualize the first two columns for simplicity
    plt.figure(figsize=(12, 8))
    sns.scatterplot(x=data.iloc[:, 0], y=data.iloc[:, 1], hue=labels, palette="viridis")
    plt.title("Agglomerative Clustering with Gower Distance")
    plt.xlabel("Feature 1")
    plt.ylabel("Feature 2")
    plt.show()

def main():
    # Load the data
    csv_file = '../../data/BPI2017-Final.csv'
    data = load_data(csv_file)
    data = data.head(10000)
    data.fillna("None", inplace=True)

    # Drop columns that won't be used for clustering
    columns_to_drop = ['event_None', 'event_Unnamed: 0', 'event_start_timestamp', 'event_timestamp', 'event_EventID', "event_EventOrigin","event_Selected","event_CreditScore","event_OfferedAmount","event_CaseID"]
    #columns_to_drop = ['event_None', 'event_Unnamed: 0', 'event_EventID']
    data = data.drop(columns=columns_to_drop)

    # Preprocess the data
    processed_data = preprocess_data(data)

    # Compute Gower distance matrix
    gower_dist_matrix = compute_gower_distance(processed_data)

    # Perform clustering
    n_clusters = 3  # Number of clusters
    labels = perform_clustering(gower_dist_matrix, n_clusters)

    # Add cluster labels to the original data
    data['Cluster'] = labels
    print(data)

    # Visualize the clusters (optional)
    visualize_clusters(processed_data, labels)

if __name__ == "__main__":
    main()