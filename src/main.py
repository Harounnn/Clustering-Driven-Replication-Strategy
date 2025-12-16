# main.py (Modified Output Version)

import pandas as pd
import numpy as np
import argparse
import glob
import os

# --- Import custom logic ---
# NOTE: Ensure kmeans.py and scoring.py are in the same directory as this script.
try:
    from kmeans_plusplus import kmeans
    from scoring import ClusterClassifier
except ImportError as e:
    print(f"Error importing custom modules: {e}")
    print("Please ensure 'kmeans.py' and 'scoring.py' are in the same directory as 'main.py'.")
    exit(1)

# --- Configuration for Scoring (Define your category rules here) ---
# NOTE: These placeholders MUST be replaced with real values derived from your dataset.

# Define the features used for clustering and scoring
CLUSTERING_FEATURES = [
    "access_freq_norm",
    "age_norm",
    "write_ratio_norm",
    "locality_norm",
    "concurrency_norm",
]

# 1. Global Medians (Medians of the normalized features (0 to 1))
GLOBAL_MEDIANS = {
    "access_freq_norm": 0.5,
    "age_norm": 0.5,
    "write_ratio_norm": 0.5,
    "locality_norm": 0.5,
    "concurrency_norm": 0.5,
}

# 2. Category Weights 
WEIGHTS = {
    "Hot":      {"access_freq_norm": 1.0, "age_norm": 0.8, "write_ratio_norm": 0.5, "locality_norm": 0.5, "concurrency_norm": 1.0},
    "Shared":   {"access_freq_norm": 0.7, "age_norm": 0.2, "write_ratio_norm": 1.0, "locality_norm": 0.2, "concurrency_norm": 0.5},
    "Moderate": {"access_freq_norm": 0.5, "age_norm": 0.5, "write_ratio_norm": 0.5, "locality_norm": 0.5, "concurrency_norm": 0.5},
    "Archival": {"access_freq_norm": 0.1, "age_norm": 1.0, "write_ratio_norm": 0.1, "locality_norm": 0.5, "concurrency_norm": 0.1},
}

# 3. Category Directions 
DIRECTIONS = {
    "Hot":      {"access_freq_norm": +1, "age_norm": -1, "write_ratio_norm": +1, "locality_norm": +1, "concurrency_norm": +1},
    "Shared":   {"access_freq_norm": +1, "age_norm": +1, "write_ratio_norm": +1, "locality_norm": +1, "concurrency_norm": +1}, 
    "Moderate": {"access_freq_norm": 0,  "age_norm": 0,  "write_ratio_norm": 0,  "locality_norm": 0,  "concurrency_norm": 0},
    "Archival": {"access_freq_norm": -1, "age_norm": +1, "write_ratio_norm": -1, "locality_norm": -1, "concurrency_norm": -1},
}

# 4. Replication Factors for Tie-breaking
REPLICATION_FACTORS = {
    "Hot": 3,
    "Shared": 2,
    "Moderate": 1,
    "Archival": 4
}
# ------------------------------------------------------------------


def run_classification_pipeline(input_csv_path, k=4, output_csv_path="cluster_assignments.csv"):
    """
    Reads feature data, runs K-Means clustering, and applies category scoring,
    outputting centroids and their final category assignments.
    """
    print(f"--- Starting Classification Pipeline ---")
    print(f"1. Reading features from: {input_csv_path}")
    
    try:
        feature_file = pd.read_csv(input_csv_path)
    except FileNotFoundError:
        print(f"Error: Feature CSV file not found at {input_csv_path}")
        return

    # 1. Prepare Data for Clustering
    X = feature_file[CLUSTERING_FEATURES].values
    n_files = len(feature_file)

    if n_files < k:
        print(f"Error: {n_files} samples found, but K={k} is requested. Cannot cluster.")
        return

    # 2. Run K-Means Clustering
    print(f"2. Running K-Means clustering with K={k} on {n_files} samples...")
    # centroids is a numpy array of shape (k, n_features)
    centroids, labels = kmeans(X, k, number_of_files=n_files, random_state=42)
    feature_file['cluster'] = labels
    print(f"Clustering complete. Data assigned to {k} clusters.")

    # 3. Prepare Cluster Data for Scoring
    cluster_data = {}
    for i in range(k):
        cluster_df = feature_file[feature_file['cluster'] == i]
        # Format: {cluster_name: {feature_name: list_of_values}}
        cluster_data[f"C{i}"] = {
            f: cluster_df[f].tolist() for f in CLUSTERING_FEATURES
        }

    # 4. Classify Clusters using scoring.py
    print("3. Classifying clusters into categories using ClusterClassifier...")
    classifier = ClusterClassifier(GLOBAL_MEDIANS, WEIGHTS, DIRECTIONS, REPLICATION_FACTORS)
    cluster_assignments = classifier.classify(cluster_data)
    print("Classification complete.")
    
    # 5. Create the Final Cluster Output Table (NEW LOGIC)
    print("4. Generating final output table (Centroids and Categories)...")
    
    # Convert numpy array of centroids into a DataFrame
    centroids_df = pd.DataFrame(centroids, columns=CLUSTERING_FEATURES)
    centroids_df.insert(0, 'cluster_id', range(k)) # Add an integer ID for mapping
    
    # Extract the category mapping from the classification results
    category_map = {
        int(k.replace('C', '')): v for k, v in cluster_assignments.items()
    }
    
    # Map the categories to the centroids DataFrame
    centroids_df['category'] = centroids_df['cluster_id'].map(category_map)

    # To satisfy the request "the id of each cluster is the centroid of it", 
    # we convert the centroid (a multi-dimensional point) into a readable string ID.
    # The actual unique identifier is the row itself, but we can prefix it.
    
    # Create the Centroid ID by converting the features (the centroid point) to a string
    # We round the centroid values for a concise ID representation
    def create_centroid_id(row):
        # Round the normalized values for a short identifier
        centroid_values = [f"{row[col]:.4f}" for col in CLUSTERING_FEATURES]
        return "CENTROID_" + "_".join(centroid_values)
        
    centroids_df.insert(0, 'centroid_id', centroids_df.apply(create_centroid_id, axis=1))

    # Select and reorder final columns
    final_output_df = centroids_df[['centroid_id', 'category'] + CLUSTERING_FEATURES]
    
    # 6. Save Final Output
    final_output_df.to_csv(output_csv_path, index=False)
    print(f"\n--- SUCCESS ---")
    print(f"Cluster centroid assignments ({k} clusters) saved to: {output_csv_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run K-Means clustering and category scoring on Spark-generated feature data.")
    parser.add_argument("--input_path", required=True, help="Path to the directory containing the features CSV file (e.g., ./out/features_out/) or the file itself (e.g., ./out/features_out/part-00000*.csv).")
    parser.add_argument("--k", type=int, default=4, help="Number of clusters (K) for K-Means.")
    parser.add_argument("--output_csv", default="final_categories.csv", help="Output filename for the final cluster assignments.")
    args = parser.parse_args()
    
    # Resolve the input path to the actual Spark output file
    if os.path.isdir(args.input_path):
        input_pattern = os.path.join(args.input_path, "part-00000*.csv")
    elif '*' in args.input_path:
        input_pattern = args.input_path
    else:
        input_pattern = args.input_path
        
    resolved_paths = glob.glob(input_pattern)
    
    if not resolved_paths:
        print(f"Error: No features CSV file found matching pattern: {input_pattern}")
    else:
        # Use the first matched file
        run_classification_pipeline(resolved_paths[0], args.k, args.output_csv)