/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.admin.cluster.state.term;

import org.opensearch.action.ActionType;

/**
 * Transport action for fetching cluster term
 *
 * @opensearch.internal
 */
public class GetTermVersionAction extends ActionType<GetTermVersionResponse> {

    public static final GetTermVersionAction INSTANCE = new GetTermVersionAction();
    public static final String NAME = "cluster:monitor/term";

    private GetTermVersionAction() {
        super(NAME, GetTermVersionResponse::new);
    }
}
