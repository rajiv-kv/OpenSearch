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
import org.opensearch.cluster.block.ClusterBlock;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.Priority;
import org.opensearch.core.action.ActionListener;

public abstract  class ClusterBlockUpdateTask extends  AckedClusterStateUpdateTask<ClusterStateUpdateResponse> {
    protected ClusterBlockUpdateTask(AckedRequest request, ActionListener<ClusterStateUpdateResponse> listener) {
        super(request, listener);
    }

    public ClusterBlockUpdateTask(Priority priority, AckedRequest request, ActionListener<ClusterStateUpdateResponse> listener) {
        super(priority, request, listener);
    }



    @Override
    public ClusterState execute(ClusterState currentState) throws Exception {

        ClusterBlocks updatedMetadata = execute(currentState.blocks());
        return ClusterState.builder(currentState).blocks(updatedMetadata).build();
    }

    public abstract ClusterBlocks execute(ClusterBlocks block) throws Exception;

    protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
        return new ClusterStateUpdateResponse(acknowledged);
    }

}
