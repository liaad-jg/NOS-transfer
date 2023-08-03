# -*- coding: utf-8 -*-
"""
This is a implementation of DBMEANS: DBSCAN with KMeans presented in:

Thiago Andrade; Brais Cancela; João Gama.
"Discovering locations and habits from human mobility data".
Annals of Telecommunications (2020):
https://doi.org/10.1007/s12243-020-00807-x.

@author: Brais Cancela
@author: Thiago Andrade
"""

import numpy as np

class DBScanKmeans():

    def __init__(self, eps, MinPts):
        self.eps = eps
        self.MinPts = MinPts
        self.clusters = None
        self.clusters_labels = None

    def fit_and_predict(self, D, seed=None):
        labels = -2 * np.ones(len(D), dtype=int)

        # C is the ID of the current clusters.
        C = 0
        self.clusters = []
        invalid_clusters = []

        index = np.arange(len(D))
        np.random.seed(seed)
        np.random.shuffle(index)

        # This outer loop is just responsible for picking new seed points--a point
        # from which to grow a new clusters.
        # Once a valid seed point is found, a new clusters is created, and the
        # clusters growth is all handled by the 'expandCluster' routine.

        # For each point P in the Dataset D...
        # ('P' is the index of the datapoint, rather than the datapoint itself.)
        for P in index:

            # Only points that have not already been claimed can be picked as new
            # seed points.
            # If the point's label is not 0, continue to the next point.
            
            if labels[P] != -2:
                continue

            labels[P] = -1
            center = D[P]
            NeighborPts = self.__region_query(D, center)

            if len(NeighborPts) >= self.MinPts:
                while True:
                    NeighborPts_bak = NeighborPts
                    # Find all of P's neighboring points.
                    lneigh = len(NeighborPts)
                    center = D[NeighborPts].mean(axis=0)
                    NeighborPts = self.__region_query(D, center)
                    if len(set(NeighborPts).symmetric_difference(set(NeighborPts_bak))) == 0:
                        break
                    ulabels, counts = np.unique(labels[NeighborPts], return_counts=True)
                    ulabels = ulabels[(ulabels > -1) & (counts >= self.MinPts)]
                    if len(ulabels) > 0:
                        for ulabel in ulabels:
                            pos = np.where(labels == ulabel)[0]
                            labels[pos] = -1
                            invalid_clusters.append(ulabel)
                            NeighborPts = np.array(list(set(NeighborPts.tolist()).union(set(pos.tolist()))))

                labels[NeighborPts] = C
                C += 1
                self.clusters.append(center)

        valid_clusters = list(sorted(set(list(range(len(self.clusters)))).difference(invalid_clusters)))
        self.clusters = np.array(self.clusters)[valid_clusters]

        if len(valid_clusters) and not self.clusters is None:
            labels = self.predict(D)
            ulabels, counts = np.unique(labels, return_counts=True)
            pos = np.where((ulabels != -1) & (counts >= self.MinPts))[0]
            self.clusters = self.clusters[ulabels[pos]]
            labels = self.predict(D)
            self.clusters_labels = labels

            return labels
        return []

    def fit(self, D, seed=None):
        self.fit_and_predict(D, seed)

    def __region_query(self, D, center):
        """
        Find all points in dataset `D` within distance `eps` of point `P`.

        This function calculates the distance between a point P and every other
        point in the dataset, and then returns only those points which are within a
        threshold distance `eps`.
        """
        dist = np.sqrt(np.sum(np.square(D - center), axis = -1))
        neighbors = np.where(dist < self.eps)[0]

        return neighbors

    def predict(self, D):
        if not isinstance(self.clusters, np.ndarray) or not len(self.clusters):
            raise Exception('Model no fitted. Call fit() function first')

        dist = np.sqrt(np.sum(np.square(D[:, np.newaxis] - self.clusters[np.newaxis]), axis = -1))
        argmin = np.argmin(dist, axis=-1)
        distmin = np.min(dist, axis=-1)
        return np.array(-1 + (1 + argmin) * (distmin < self.eps), dtype=int)
