import numpy as np

class ClusterClassifier:
     """
    ClusterClassifier performs cluster-based category assignment using weighted scoring.

    Attributes:
        global_medians (dict): Global median values for each feature.
        weights (dict): Weight of each feature per category.
        directions (dict): Expected direction (+1, -1, 0) of each feature per category.
        replication_factors (dict): Factor for tie-breaking when multiple categories have the same score.
    """
    def __init__(self, global_medians, weights, directions, replication_factors):
          """
        Initialize the classifier with global medians, weights, directions, and replication factors.

        Args:
            global_medians (dict): {feature_name: median_value}
            weights (dict): {category: {feature: weight}}
            directions (dict): {category: {feature: expected_direction}}
            replication_factors (dict): {category: factor for tie-breaking}
        """
        self.global_medians = global_medians
        self.weights = weights
        self.directions = directions
        self.replication_factors = replication_factors

    def f(self, x):
         """
        Weighting function to assign more importance to stronger deviations.

        Args:
            x (float): Input deviation metric, in range [0, 1] or higher.

        Returns:
            float: Weighted value. Here, squared: x^2
        """
        return x ** 2

    def compute_cluster_medians(self, clusters):
          """
        Compute the median value for each feature in each cluster.

        Args:
            clusters (dict): {cluster_name: {feature_name: list_of_values}}

        Returns:
            dict: {cluster_name: {feature_name: median_value}}
        """
        medians = {}
        for cluster_name, features in clusters.items():
            medians[cluster_name] = {
                p: np.median(values) for p, values in features.items()
            }
        return medians

    def score_category(self, cluster_medians, category):
         """
        Compute the score of a cluster for a given category.

        Implements the rules:
            - For Moderate: minimal deviation is rewarded.
            - For others: score increases when deviation matches expected direction.

        Args:
            cluster_medians (dict): {feature: median_value} for a cluster.
            category (str): Category name ('Hot', 'Shared', 'Moderate', 'Archival').

        Returns:
            float: Score for this category.
        """
        score = 0
        for p, median_value in cluster_medians.items():
            delta = median_value - self.global_medians[p]
            expected_dir = self.directions[category][p]

            if category == "Moderate":
                if abs(delta) < 0.1:
                    score += self.weights[category][p] * self.f(1 - abs(delta))
            else:
                if expected_dir == 0 or np.sign(delta) == expected_dir:
                    score += self.weights[category][p] * self.f(abs(delta))

        return score

    def classify_cluster(self, cluster_medians):
        """
        Determine the best category for a single cluster based on scores.

        Tie-breaking:
            - If multiple categories have same max score, select one with highest replication factor.

        Args:
            cluster_medians (dict): {feature: median_value} for the cluster.

        Returns:
            str: Assigned category ('Hot', 'Shared', 'Moderate', or 'Archival')
        """
        categories = ["Hot", "Shared", "Moderate", "Archival"]
        scores = {cat: self.score_category(cluster_medians, cat) for cat in categories}

        max_score = max(scores.values())
        tied = [k for k, v in scores.items() if v == max_score]

        if len(tied) > 1:
            tied.sort(key=lambda x: self.replication_factors[x], reverse=True)
            return tied[0]

        return max(scores, key=scores.get)

    def classify(self, clusters):
         """
        Classify all clusters in the dataset.

        Steps:
            1. Compute cluster medians.
            2. Compute scores for each cluster and category.
            3. Assign best category to each cluster.

        Args:
            clusters (dict): {cluster_name: {feature: list_of_values}}

        Returns:
            dict: {cluster_name: assigned_category}
        """
        medians = self.compute_cluster_medians(clusters)
        results = {}
        for cluster_name, cluster_medians in medians.items():
            results[cluster_name] = self.classify_cluster(cluster_medians)
        return results


# -------------------------
# TEST CODE BELOW
# -------------------------

clusters = {
    "C1": {"IOPS": [100, 110, 105], "Latency": [2, 3, 2.5]},
    "C2": {"IOPS": [50, 55, 60], "Latency": [5, 6, 5.5]},
    "C3": {"IOPS": [10, 12, 11], "Latency": [8, 9, 7]},
    "C4": {"IOPS": [200, 210, 220], "Latency": [1, 1.5, 1.2]}
}

global_medians = {"IOPS": 60, "Latency": 4}

weights = {
    "Hot":      {"IOPS": 1.0, "Latency": 0.8},
    "Shared":   {"IOPS": 0.7, "Latency": 0.7},
    "Moderate": {"IOPS": 0.5, "Latency": 0.5},
    "Archival": {"IOPS": 0.9, "Latency": 1.0}
}

directions = {
    "Hot":      {"IOPS": +1, "Latency": -1},
    "Shared":   {"IOPS": +1, "Latency": +1},
    "Moderate": {"IOPS":  0, "Latency":  0},
    "Archival": {"IOPS": -1, "Latency": +1}
}

replication_factors = {
    "Hot": 3,
    "Shared": 2,
    "Moderate": 1,
    "Archival": 4
}

# ---- TESTING ----
classifier = ClusterClassifier(global_medians, weights, directions, replication_factors)

results = classifier.classify(clusters)

print("Final Category Assignments:")
for cluster, label in results.items():
    print(cluster, "→", label)
