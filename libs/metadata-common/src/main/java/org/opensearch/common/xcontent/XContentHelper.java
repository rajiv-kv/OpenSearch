/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.xcontent;

import org.opensearch.common.collect.Tuple;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.core.xcontent.XContent;

public interface XContentHelper {

    static CompressedXContent convertToMap(XContent xContent, String filter, boolean b) {
        return null;
    }

    static Tuple<Object, Object> convertToMap(Object uncompressed, boolean b) {
        return null;

    }
}
