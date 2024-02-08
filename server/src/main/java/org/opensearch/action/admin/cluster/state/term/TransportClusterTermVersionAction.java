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
 *     http://www.apache.org/licenses/LICENSE-2.0
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


package org.opensearch.action.admin.cluster.state.term;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.cluster.state.term.ClusterTermVersionAction;
import org.opensearch.action.admin.cluster.state.term.ClusterTermVersionRequest;
import org.opensearch.action.admin.cluster.state.term.ClusterTermVersionResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeReadAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * Transport action for obtaining cluster term
 *
 * @opensearch.internal
 */
public class TransportClusterTermVersionAction extends TransportClusterManagerNodeReadAction<ClusterTermVersionRequest, ClusterTermVersionResponse> {

    private final Logger logger = LogManager.getLogger(getClass());

    @Inject
    public TransportClusterTermVersionAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ClusterTermVersionAction.NAME,
            false,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ClusterTermVersionRequest::new,
            indexNameExpressionResolver
        );
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    public ClusterTermVersionResponse read(StreamInput in) throws IOException {
        return new ClusterTermVersionResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterTermVersionRequest request, ClusterState state) {
        return null;
    }

    @Override
    protected void clusterManagerOperation(ClusterTermVersionRequest request, ClusterState state, ActionListener<ClusterTermVersionResponse> listener) throws Exception {
        ActionListener.completeWith(listener, () -> buildResponse(request, state));
    }

    private ClusterTermVersionResponse buildResponse(ClusterTermVersionRequest request, ClusterState state) {
        logger.trace("Serving cluster term version request using term {} and version {}", state.term(), state.version());
        return new ClusterTermVersionResponse(state.getNodes().getClusterManagerNode(), state.term(), state.getVersion());
    }
}
