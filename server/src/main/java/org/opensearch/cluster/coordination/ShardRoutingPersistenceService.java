/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.coordination;

import java.util.ArrayList;
import java.util.List;
import org.opensearch.cluster.metadata.RemoteMetadataRef;
import org.opensearch.cluster.routing.IndexRoutingTable;

public class ShardRoutingPersistenceService {
    public RemoteMetadataRef persist(ShardRoutingPersistenceService.RoutingTableChangeEvent clusterChangedEvent) {
        return  new RemoteMetadataRef("index_routing", clusterChangedEvent.toString());
    }

    public List<IndexRoutingTable> load(RemoteMetadataRef reference) {
        return  new ArrayList<>();
    }

    public static class RoutingTableChangeEvent {
        private final String source;

        private final List<IndexRoutingTable> state;

        private final String event_type = "create_or_update";

        public RoutingTableChangeEvent(String source, List<IndexRoutingTable> state) {
            this.source = source;
            this.state = state;
        }

        @Override
        public String toString() {
            return "RoutingTableChangeEvent{" + "source='" + source + '\'' + ", state=" + state + '}';
        }
    }
}
