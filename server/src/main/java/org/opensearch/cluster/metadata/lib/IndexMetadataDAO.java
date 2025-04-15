/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata.lib;

/**
 *
 */
public class IndexMetadataDAO {
    // ClusterStateRepository repository;
    //
    // public IndexMetadataDAO(ClusterStateRepository repository) {
    // this.repository = repository;
    // }
    //
    //
    // public void createIndex(IndexMetadata indexMetadata) {
    // ClusterState oldState = repository.load();
    // Metadata.Builder builder = Metadata.builder(oldState.metadata()).put(indexMetadata, false);
    // ClusterState updatedState = ClusterState.builder(oldState).metadata(builder).build();
    // repository.save(updatedState);
    // }
}
