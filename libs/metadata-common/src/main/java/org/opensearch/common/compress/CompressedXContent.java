/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.compress;

import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

public class CompressedXContent {

    public CompressedXContent(BytesReference bytes) {}

    public CompressedXContent(byte[] bytes) {}

    public static CompressedXContent readCompressedString(StreamInput in) {
        return null;
    }

    public void writeTo(StreamOutput out) {

    }

    public boolean compressed() {
        return false;
    }

    public Object uncompressed() {
        return null;
    }
}
