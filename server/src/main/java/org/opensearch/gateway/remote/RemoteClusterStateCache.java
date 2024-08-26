/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.coordination.ClusterStateTermVersion;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Cache to Remote Cluster State based on term-version check. The current implementation
 * caches the last highest version of cluster-state that was downloaded from cache.
 *
 * @opensearch.internal
 */
public class RemoteClusterStateCache {

    private final AtomicReference<ClusterState> clusterStateFromCache = new AtomicReference<>();

    public ClusterState getState(String clusterName, ClusterMetadataManifest manifest) {
        ClusterStateTermVersion manifestStateTermVersion = new ClusterStateTermVersion(
            new ClusterName(clusterName),
            manifest.getClusterUUID(),
            manifest.getClusterTerm(),
            manifest.getStateVersion()
        );

        ClusterState cache = clusterStateFromCache.get();
        if (cache != null) {
            ClusterStateTermVersion cacheStateTermVersion = new ClusterStateTermVersion(
                new ClusterName(clusterName),
                cache.metadata().clusterUUID(),
                cache.term(),
                cache.version()
            );
            if (manifestStateTermVersion.equals(cacheStateTermVersion)) {
                return cache;
            }
        }
        return null;
    }

    public void putState(final ClusterState newState) {
        clusterStateFromCache.getAndUpdate(current -> {
            if (current == null) {
                return newState;
            }
            if (newState.term() >= current.term() && newState.version() > current.version()) {
                return newState;
            } else {
                return current;
            }
        });
    }
}
