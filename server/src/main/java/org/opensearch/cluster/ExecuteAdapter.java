package org.opensearch.cluster;

import java.util.List;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.coordination.ClusterStatePublisher;

public interface ExecuteAdapter<T> {

    default  ClusterState execute(ClusterState currentState) throws Exception {
        return null;
    }

    default ClusterStatePublisher.ClusterStateUpdateResult executeAndResult(ClusterState state) throws Exception {
            return null;
        }
    }
