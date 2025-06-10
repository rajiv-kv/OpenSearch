/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.coordination;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.RemoteMetadataRef;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import static org.opensearch.common.xcontent.XContentHelper.createParser;

public class IndexMetadataPersistenceService {

    public RemoteMetadataRef persist(IndexMetadataChangeEvent clusterChangedEvent) throws IOException {
        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        for (IndexMetadata indexMetadata : clusterChangedEvent.state) {
            IndexMetadata.FORMAT.toXContent(builder, indexMetadata);
        }
        builder.endObject();
        return  new RemoteMetadataRef("index_metadata", builder.toString());
    }
    public List<IndexMetadata> load(RemoteMetadataRef remoteMetadataRef) throws IOException {

        List<IndexMetadata> indexMetadata = new ArrayList<>();
        try (
            XContentParser parser = XContentHelper.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                new BytesArray(remoteMetadataRef.arn),
                MediaTypeRegistry.JSON
            )
        ) {
            parser.nextToken();
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                indexMetadata.add(IndexMetadata.Builder.fromXContent(parser));
            }
        }
        return indexMetadata;
    }


    public static class IndexMetadataChangeEvent {
        private final String source;

        private final List<IndexMetadata> state;

        private final String event_type = "create_or_update";

        public IndexMetadataChangeEvent(String source, List<IndexMetadata> state) {
            this.source = source;
            this.state = state;
        }

        @Override
        public String toString() {
            return "IndexMetadataChangeEvent{" + "source='" + source + '\'' + ", state=" + state + '}';
        }
    }
}
