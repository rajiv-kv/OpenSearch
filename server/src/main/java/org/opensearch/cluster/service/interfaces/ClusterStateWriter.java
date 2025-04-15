/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service.interfaces;

import org.opensearch.cluster.AckedClusterStateTaskListener;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateTaskConfig;
import org.opensearch.cluster.ClusterStateTaskExecutor;
import org.opensearch.cluster.ClusterStateTaskListener;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.ack.AckedRequest;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.common.Nullable;
import org.opensearch.common.Priority;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;

import java.util.Map;

/**
 *
 * @param <S>
 */
public interface ClusterStateWriter<S> {

    public static abstract class AckedMetadataUpdateTask<Response> extends MetadataUpdateTask implements AckedClusterStateTaskListener {

        private final ActionListener<Response> listener;
        private final AckedRequest request;

        protected AckedMetadataUpdateTask(AckedRequest request, ActionListener<Response> listener) {
            this(Priority.NORMAL, request, listener);
        }

        protected AckedMetadataUpdateTask(Priority priority, AckedRequest request, ActionListener<Response> listener) {
            // super.(priority);
            this.listener = listener;
            this.request = request;
        }

        /**
         * Called to determine which nodes the acknowledgement is expected from
         *
         * @param discoveryNode a node
         * @return true if the node is expected to send ack back, false otherwise
         */
        public boolean mustAck(DiscoveryNode discoveryNode) {
            return true;
        }

        /**
         * Called once all the nodes have acknowledged the cluster state update request. Must be
         * very lightweight execution, since it gets executed on the cluster service thread.
         *
         * @param e optional error that might have been thrown
         */
        public void onAllNodesAcked(@Nullable Exception e) {
            listener.onResponse(newResponse(e == null));
        }

        protected abstract Response newResponse(boolean acknowledged);

        /**
         * Called once the acknowledgement timeout defined by
         * {@link org.opensearch.cluster.AckedClusterStateUpdateTask#ackTimeout()} has expired
         */
        public void onAckTimeout() {
            listener.onResponse(newResponse(false));
        }

        @Override
        public void onFailure(String source, Exception e) {
            listener.onFailure(e);
        }

        /**
         * Acknowledgement timeout, maximum time interval to wait for acknowledgements
         */
        public TimeValue ackTimeout() {
            return request.ackTimeout();
        }

        @Override
        public TimeValue timeout() {
            return request.masterNodeTimeout();
        }
    }

    public static class MetadataUpdateTask extends UpdateTask<Metadata> {

        @Override
        public Metadata execute(Metadata state) {
            return null;
        }

        @Override
        public void update(ClusterState currentState, Metadata updateItem) {

        }

        @Override
        public Metadata getUpdateItem(ClusterState state) {
            return null;
        }
    }

    public static class RoutingTableTask extends UpdateTask<RoutingTable> {

        @Override
        public RoutingTable execute(RoutingTable state) {
            return null;
        }

        @Override
        public void update(ClusterState currentState, RoutingTable updateItem) {

        }

        @Override
        public RoutingTable getUpdateItem(ClusterState state) {
            return null;
        }
    }

    public static abstract class UpdateTask<U> extends ClusterStateUpdateTask {
        @Override
        public final ClusterState execute(ClusterState currentState) throws Exception {
            U updateItem = getUpdateItem(currentState);
            update(currentState, updateItem);

            return null;
        }

        public abstract U execute(U state);

        public abstract void update(ClusterState currentState, U updateItem);

        public abstract U getUpdateItem(ClusterState state);

        @Override
        public void onFailure(String source, Exception e) {

        }
    }

    /**
     * Submits a cluster state update task; unlike {@link #submitStateUpdateTask(String, Object, org.opensearch.cluster.ClusterStateTaskConfig,
     * org.opensearch.cluster.ClusterStateTaskExecutor, org.opensearch.cluster.ClusterStateTaskListener)}, submitted updates will not be batched.
     *
     * @param source     the source of the cluster state update task
     * @param updateTask the full context for the cluster state update
     *                   task
     *
     */
    public <U extends ClusterStateTaskConfig & ClusterStateTaskExecutor<U> & ClusterStateTaskListener> void submitStateUpdateTask(
        String source,
        U updateTask
    );

    /**
     * Submits a cluster state update task; submitted updates will be
     * batched across the same instance of executor. The exact batching
     * semantics depend on the underlying implementation but a rough
     * guideline is that if the update task is submitted while there
     * are pending update tasks for the same executor, these update
     * tasks will all be executed on the executor in a single batch
     *
     * @param source   the source of the cluster state update task
     * @param task     the state needed for the cluster state update task
     * @param config   the cluster state update task configuration
     * @param executor the cluster state update task executor; tasks
     *                 that share the same executor will be executed
     *                 batches on this executor
     * @param listener callback after the cluster state update task
     *                 completes
     * @param <T>      the type of the cluster state update task state
     *
     */
    public <T> void submitStateUpdateTask(
        String source,
        T task,
        ClusterStateTaskConfig config,
        ClusterStateTaskExecutor<T> executor,
        ClusterStateTaskListener listener
    );

    /**
     * Submits a batch of cluster state update tasks; submitted updates are guaranteed to be processed together,
     * potentially with more tasks of the same executor.
     *
     * @param source   the source of the cluster state update task
     * @param tasks    a map of update tasks and their corresponding listeners
     * @param config   the cluster state update task configuration
     * @param executor the cluster state update task executor; tasks
     *                 that share the same executor will be executed
     *                 batches on this executor
     * @param <T>      the type of the cluster state update task state
     *
     */
    public <T> void submitStateUpdateTasks(
        final String source,
        final Map<T, ClusterStateTaskListener> tasks,
        final ClusterStateTaskConfig config,
        final ClusterStateTaskExecutor<T> executor
    );
}
