/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster;

import java.util.List;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.service.ClusterManagerTaskThrottler;
import org.opensearch.common.Priority;
import org.opensearch.common.unit.TimeValue;

public class MetadataUpdateTaskExecutor implements
    ClusterStateTaskExecutor<MetadataUpdateTaskExecutor.TaskWithType>,
    ClusterStateTaskConfig,
    AckedClusterStateTaskListener {

    @Override
    public TimeValue timeout() {
        return null;
    }

    @Override
    public Priority priority() {
        return null;
    }


    public interface TaskWithType<T> {
        T execute(T metadata, ClusterState state) throws Exception;
    }
    public static interface MetadataUpdater extends TaskWithType<Metadata> {

    }
    public static interface RoutingTableUpdater extends TaskWithType<RoutingTable> {

    }

    public static interface ClusterBlocksUpdater extends TaskWithType<ClusterBlocks> {

    }



    @Override
    public ClusterTasksResult<TaskWithType> execute(
        ClusterState currentState, List<TaskWithType> tasks
    ) throws Exception {

        ClusterState previousState = currentState;
        for (TaskWithType task : tasks) {
            if (task instanceof MetadataUpdater) {
                MetadataUpdater updater = (MetadataUpdater)task;
                Metadata updatedMetadata = updater.execute(previousState.metadata(), previousState);
                previousState = ClusterState.builder(previousState).metadata(updatedMetadata).build();
            }
            if (task instanceof RoutingTableUpdater) {
                RoutingTableUpdater updater = (RoutingTableUpdater)task;
                RoutingTable updatedMetadata = updater.execute(previousState.routingTable(), previousState);
                previousState = ClusterState.builder(previousState).routingTable(updatedMetadata).build();

            }
            if (task instanceof ClusterBlocksUpdater) {
                ClusterBlocksUpdater updater = (ClusterBlocksUpdater)task;
                ClusterBlocks updatedMetadata = updater.execute(previousState.blocks(), previousState);
                previousState = ClusterState.builder(previousState).blocks(updatedMetadata).build();
            }
        }
        return ClusterTasksResult.<TaskWithType>builder().successes(tasks).build(currentState);
    }


    @Override
    public ClusterManagerTaskThrottler.ThrottlingKey getClusterManagerThrottlingKey() {
        return ClusterStateTaskExecutor.super.getClusterManagerThrottlingKey();
    }

    @Override
    public boolean mustAck(DiscoveryNode discoveryNode) {
        return false;
    }

    @Override
    public void onAllNodesAcked(Exception e) {

    }

    @Override
    public void onAckTimeout() {

    }

    @Override
    public TimeValue ackTimeout() {
        return null;
    }

    @Override
    public void onFailure(String source, Exception e) {

    }

}
