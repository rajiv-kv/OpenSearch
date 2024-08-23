/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.cluster.coordination;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateApplier;
import org.opensearch.cluster.NodeConnectionsService;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.MockEngineFactoryPlugin;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.plugins.Plugin;
import org.opensearch.tasks.Task;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;
import org.opensearch.test.store.MockFSIndexStore;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.test.transport.StubbableTransport;
import org.opensearch.transport.*;

import static org.hamcrest.Matchers.is;
import static org.opensearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_ACTION_NAME;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class NodeJoinLeftIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "test-idx-1";
    private static final String REPO_NAME = "test-repo-1";
    private static final String SNAP_NAME = "test-snap-1";

    private static final int MIN_DOC_COUNT = 500;
    private static final int MAX_DOC_COUNT = 1000;
    private static final int SHARD_COUNT = 1;
    private static final int REPLICA_COUNT = 0;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
            MockTransportService.TestPlugin.class,
            MockFSIndexStore.TestPlugin.class,
            InternalSettingsPlugin.class,
            MockEngineFactoryPlugin.class
        );
    }

    @Override
    protected void beforeIndexDeletion() throws Exception {
        super.beforeIndexDeletion();
        internalCluster().assertConsistentHistoryBetweenTranslogAndLuceneIndex();
        internalCluster().assertSeqNos();
        internalCluster().assertSameDocIdsOnShards();
    }

    public void testTransientErrorsDuringRecovery1AreRetried() throws Exception {
        final String indexName = "test";
        final Settings nodeSettings = Settings.builder()
            .put(RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING.getKey(), "100ms")
            .put(NodeConnectionsService.CLUSTER_NODE_RECONNECT_INTERVAL_SETTING.getKey(), "10s")
            .put(FollowersChecker.FOLLOWER_CHECK_TIMEOUT_SETTING.getKey(), "200ms")
            .put(FollowersChecker.FOLLOWER_CHECK_INTERVAL_SETTING.getKey(), "100ms")
            .put(FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey(), 1)
            .build();
        // start a cluster-manager node
        final String cm =internalCluster().startNode(nodeSettings);

        final String blueNodeName = internalCluster().startNode(
            Settings.builder().put("node.attr.color", "blue").put(nodeSettings).build()
        );
        final String redNodeName = internalCluster().startNode(Settings.builder().put("node.attr.color", "red").put(nodeSettings).build());

        ClusterHealthResponse response = client().admin().cluster().prepareHealth().setWaitForNodes(">=3").get();
        assertThat(response.isTimedOut(), is(false));

        client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(
                Settings.builder()
                    .put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "color", "blue")
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
            .get();

        MockTransportService cmransportService = (MockTransportService) internalCluster().getInstance(TransportService.class, cm);
        MockTransportService redTransportService =
            (MockTransportService) internalCluster().getInstance(TransportService.class, redNodeName);

        ClusterService cmClsSerive = internalCluster().getInstance(ClusterService.class, cm);
        cmClsSerive.addStateApplier(new ClusterStateApplier() {
            @Override
            public void applyClusterState(ClusterChangedEvent event) {
                if (event.nodesRemoved()) {

                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });
        cmransportService.connectionManager().addListener(new TransportConnectionListener() {

            @Override
            public void onConnectionOpened(Transport.Connection connection) {
                //                                try {
                //                                    Thread.sleep(500);
                //                                } catch (InterruptedException e) {
                //                                    throw new RuntimeException(e);
                //                                }

            }

            @Override
            public void onNodeConnected(DiscoveryNode node, Transport.Connection connection) {
                //                if (node.getName().equals("node_t2")) {
                //                    try {
                //                        Thread.sleep(250);
                //                    } catch (InterruptedException e) {
                //                        throw new RuntimeException(e);
                //                    }
                //                }
            }

            //            @Override
            //            public void onNodeDisconnected(DiscoveryNode node, Transport.Connection connection) {
            //                try {
            //                    Thread.sleep(5000);
            //                } catch (InterruptedException e) {
            //                    throw new RuntimeException(e);
            //                }
            //           }
        });
        AtomicBoolean bb = new AtomicBoolean();
        ConnectionDelay handlingBehavior = new ConnectionDelay(
            FOLLOWER_CHECK_ACTION_NAME,
            ()->{
                if (bb.get()) {
                    return;
                }
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                throw new NodeHealthCheckFailureException("non writable exception");
            });
        redTransportService.addRequestHandlingBehavior(FOLLOWER_CHECK_ACTION_NAME, handlingBehavior);

        for (int i=0 ;i < 20; i++) {
            //cmransportService.disconnectFromNode(redTransportService.getLocalDiscoNode());
            System.out.println("failing follower check");
            bb.set(true);
            Thread.sleep(1500);
            bb.set(false);
            ClusterHealthResponse response1 = client().admin().cluster().prepareHealth().setWaitForNodes("2").get();
            assertThat(response1.isTimedOut(), is(false));
            //internalCluster().stopRandomNode(InternalTestCluster.nameFilter(blueNodeName));
            ClusterHealthResponse response2 = client().admin().cluster().prepareHealth().setWaitForNodes("3").get();
            assertThat(response2.isTimedOut(), is(false));
        }

        Thread.sleep(60000);
    }
    private class ConnectionDelay implements StubbableTransport.RequestHandlingBehavior<TransportRequest> {

        private final String actionName;
        private final Runnable connectionBreaker;

        private ConnectionDelay(
            String actionName,
            Runnable connectionBreaker
        ) {
            this.actionName = actionName;
            this.connectionBreaker = connectionBreaker;
        }

        @Override
        public void messageReceived(
            TransportRequestHandler<TransportRequest> handler,
            TransportRequest request,
            TransportChannel channel,
            Task task
        ) throws Exception {


            connectionBreaker.run();
            handler.messageReceived(request, channel, task);
        }
    }
}
