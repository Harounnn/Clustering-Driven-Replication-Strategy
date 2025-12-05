import numpy as np

def kmeans_plusplus_init(X, k, random_state=None):
    rng = np.random.default_rng(random_state)
    n_samples, n_features = X.shape
    centroids = np.empty((k, n_features), dtype=X.dtype)

    # first centroid
    first_idx = rng.integers(0, n_samples)
    centroids[0] = X[first_idx]

    # remaining centroids
    for i in range(1, k):
        dist_sq = np.min(
            np.linalg.norm(X[:, None, :] - centroids[None, :i, :], axis=2) ** 2,
            axis=1,
        )
        probs = dist_sq / dist_sq.sum()
        next_idx = rng.choice(n_samples, p=probs)
        centroids[i] = X[next_idx]

    return centroids

def kmeans(X, k, number_of_files=100, tol=1e-4, random_state=None):
    X = np.asarray(X)
    n_samples = X.shape[0]
    centroids = kmeans_plusplus_init(X, k, random_state=random_state)

    max_iter = max(100, number_of_files / 100)

    for _ in range(max_iter):
        # assignment
        distances = np.linalg.norm(X[:, None, :] - centroids[None, :, :], axis=2)
        labels = np.argmin(distances, axis=1)

        # update
        new_centroids = np.empty_like(centroids)
        for j in range(k):
            mask = labels == j
            if np.any(mask):
                new_centroids[j] = X[mask].mean(axis=0)
            else:
                new_centroids[j] = X[np.random.randint(0, n_samples)]

        shift = np.linalg.norm(new_centroids - centroids)
        centroids = new_centroids
        if shift < tol:
            break

    return centroids, labels
