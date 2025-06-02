/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service;

import java.util.HashMap;
import java.util.Map;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterManagerMetrics;
import org.opensearch.cluster.coordination.ClusterStatePublisher;
import org.opensearch.cluster.coordination.ClusterStateTermVersion;
import org.opensearch.cluster.coordination.CoordinationState;
import org.opensearch.cluster.coordination.Coordinator;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.threadpool.ThreadPool;

import static java.util.Map.entry;

/**
 * Main Cluster Manager Node Service
 *
 * @opensearch.api
 */
@PublicApi(since = "2.2.0")
public class ClusterManagerService extends MasterService {

    public ClusterManagerService(Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool) {
        super(settings, clusterSettings, threadPool);
    }

    public ClusterManagerService(
        Settings settings,
        ClusterSettings clusterSettings,
        ThreadPool threadPool,
        ClusterManagerMetrics clusterManagerMetrics
    ) {
        super(settings, clusterSettings, threadPool, clusterManagerMetrics);
    }



    @FunctionalInterface
    public  static interface ClusterUpdatePublisher {
        void publish(ClusterChangedEvent clusterChangedEvent, ActionListener<Void> publishListener, ClusterStatePublisher.AckListener ackListener);

    }

    @FunctionalInterface
    public  static interface ClusterStateSupplier<T> {
        T get(ClusterStateTermVersion termVersion);

    }

    public Map<String, ClusterUpdatePublisher>getPublishersForClusterStateComponents() {
        return Map.ofEntries(
            entry("metadata", remoteS3ClusterStatePublisher::publish),
            entry("routing_table", coordinator::publish));

    }

    public Map<String, ClusterStateSupplier>getSuppliersForClusterStateComponents() {
        return Map.ofEntries(
            entry("metadata", remoteS3ClusterStatePublisher::get),
            entry("routing_table", coordinator::get));
    }


    Coordinator coordinator;
    RemoteS3ClusterStatePublisher remoteS3ClusterStatePublisher;

    public static class RemoteS3ClusterStatePublisher {
        void publish(ClusterChangedEvent clusterChangedEvent, ActionListener<Void> publishListener, ClusterStatePublisher.AckListener ackListener) {

        }

        Metadata get(ClusterStateTermVersion termVersion) {
            return null;
        }



    }
}
