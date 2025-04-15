/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service.interfaces;

import org.opensearch.cluster.ClusterName;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;

/**
 * A Background Task
 */
public interface ClusterStateReader {

    ClusterName getClusterName();

    ClusterSettings getClusterSettings();

    Settings getSettings();

    String getNodeName();
}
