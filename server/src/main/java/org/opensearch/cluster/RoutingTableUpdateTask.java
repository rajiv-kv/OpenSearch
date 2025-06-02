/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster;

import org.opensearch.cluster.ack.AckedRequest;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.common.Priority;
import org.opensearch.core.action.ActionListener;

public abstract  class RoutingTableUpdateTask extends  AckedClusterStateUpdateTask<ClusterStateUpdateResponse> {
    protected RoutingTableUpdateTask(AckedRequest request, ActionListener<ClusterStateUpdateResponse> listener) {
        super(request, listener);
    }

    public RoutingTableUpdateTask(Priority priority, AckedRequest request, ActionListener<ClusterStateUpdateResponse> listener) {
        super(priority, request, listener);
    }

    @Override
    public ClusterState execute(ClusterState currentState) throws Exception {

        RoutingTable updatedMetadata = execute(currentState.routingTable());
        return ClusterState.builder(currentState).routingTable(updatedMetadata).build();
    }

    public abstract RoutingTable execute(RoutingTable metadata) throws Exception;

    protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
        return new ClusterStateUpdateResponse(acknowledged);
    }

}
