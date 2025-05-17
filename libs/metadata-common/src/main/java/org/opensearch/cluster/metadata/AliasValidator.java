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

package org.opensearch.cluster.metadata;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.opensearch.OpenSearchException;
import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.common.Nullable;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.SuggestingErrorOnUnknown;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedObjectNotFoundException;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentLocation;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.indices.InvalidAliasNameException;

/**
 * Validator for an alias, to be used before adding an alias to the index metadata
 * and make sure the alias is valid
 *
 * @opensearch.internal
 */
public class AliasValidator {
    /**
     * Allows to validate an {@link org.opensearch.action.admin.indices.alias.Alias} and make sure
     * it's valid before it gets added to the index metadata. Doesn't validate the alias filter.
     * @throws IllegalArgumentException if the alias is not valid
     */
    public void validateAlias(Alias alias, String index, Map<String, IndexMetadata> metadata) {
        validateAlias(alias.name(), index, alias.indexRouting(), metadata::get);
    }

    /**
     * Allows to validate an {@link org.opensearch.cluster.metadata.AliasMetadata} and make sure
     * it's valid before it gets added to the index metadata. Doesn't validate the alias filter.
     * @throws IllegalArgumentException if the alias is not valid
     */
    public void validateAliasMetadata(AliasMetadata aliasMetadata, String index, Map<String, IndexMetadata> metadata) {
        validateAlias(aliasMetadata.alias(), index, aliasMetadata.indexRouting(), metadata::get);
    }

    /**
     * Allows to partially validate an alias, without knowing which index it'll get applied to.
     * Useful with index templates containing aliases. Checks also that it is possible to parse
     * the alias filter via {@link org.opensearch.core.xcontent.XContentParser},
     * without validating it as a filter though.
     * @throws IllegalArgumentException if the alias is not valid
     */
    public void validateAliasStandalone(Alias alias) {
        validateAliasStandalone(alias.name(), alias.indexRouting());
        if (Strings.hasLength(alias.filter())) {
            try {
                XContentHelper.convertToMap(MediaTypeRegistry.xContent(alias.filter()).xContent(), alias.filter(), false);
            } catch (Exception e) {
                throw new IllegalArgumentException("failed to parse filter for alias [" + alias.name() + "]", e);
            }
        }
    }

    /**
     * Validate a proposed alias.
     */
    public void validateAlias(String alias, String index, @Nullable String indexRouting, Function<String, IndexMetadata> indexLookup) {
        validateAliasStandalone(alias, indexRouting);

        if (Strings.hasText(index) == false) {
            throw new IllegalArgumentException("index name is required");
        }

        IndexMetadata indexNamedSameAsAlias = indexLookup.apply(alias);
        if (indexNamedSameAsAlias != null) {
            throw new InvalidAliasNameException(indexNamedSameAsAlias.getIndex(), alias, "an index exists with the same name as the alias");
        }
    }

    void validateAliasStandalone(String alias, String indexRouting) {
        if (!Strings.hasText(alias)) {
            throw new IllegalArgumentException("alias name is required");
        }
        validateIndexOrAliasName(alias, InvalidAliasNameException::new);
        if (indexRouting != null && indexRouting.indexOf(',') != -1) {
            throw new IllegalArgumentException("alias [" + alias + "] has several index routing values associated with it");
        }
    }

    public static final int MAX_INDEX_NAME_BYTES = 255;

    public static void validateIndexOrAliasName(String index, BiFunction<String, String, ? extends RuntimeException> exceptionCtor) {
        if (Strings.validFileName(index) == false) {
            throw exceptionCtor.apply(index, "must not contain the following characters " + Strings.INVALID_FILENAME_CHARS);
        }
        if (index.isEmpty()) {
            throw exceptionCtor.apply(index, "must not be empty");
        }
        if (index.contains("#")) {
            throw exceptionCtor.apply(index, "must not contain '#'");
        }
        if (index.contains(":")) {
            throw exceptionCtor.apply(index, "must not contain ':'");
        }
        if (index.charAt(0) == '_' || index.charAt(0) == '-' || index.charAt(0) == '+') {
            throw exceptionCtor.apply(index, "must not start with '_', '-', or '+'");
        }
        int byteCount = 0;
        try {
            byteCount = index.getBytes("UTF-8").length;
        } catch (UnsupportedEncodingException e) {
            // UTF-8 should always be supported, but rethrow this if it is not for some reason
            throw new OpenSearchException("Unable to determine length of index name", e);
        }
        if (byteCount > MAX_INDEX_NAME_BYTES) {
            throw exceptionCtor.apply(index, "index name is too long, (" + byteCount + " > " + MAX_INDEX_NAME_BYTES + ")");
        }
        if (index.equals(".") || index.equals("..")) {
            throw exceptionCtor.apply(index, "must not be '.' or '..'");
        }
    }
    /**
     * Validates an alias filter by parsing it using the
     * @throws IllegalArgumentException if the filter is not valid
     */
    public void validateAliasFilter(
        String alias,
        String filter,
        NamedXContentRegistry xContentRegistry
    ) {
        try (
            XContentParser parser = MediaTypeRegistry.xContent(filter)
                .xContent()
                .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, filter)
        ) {
            validateAliasFilter(parser);
        } catch (Exception e) {
            throw new IllegalArgumentException("failed to parse filter for alias [" + alias + "]", e);
        }
    }

    /**
     * Validates an alias filter by parsing it using the
     * @throws IllegalArgumentException if the filter is not valid
     */
    public void validateAliasFilter(
        String alias,
        BytesReference filter,
        NamedXContentRegistry xContentRegistry
    ) {

        try (
            InputStream inputStream = filter.streamInput();
            XContentParser parser = MediaTypeRegistry.xContentType(inputStream)
                .xContent()
                .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, filter.streamInput())
        ) {
            validateAliasFilter(parser);
        } catch (Exception e) {
            throw new IllegalArgumentException("failed to parse filter for alias [" + alias + "]", e);
        }
    }

    private static void validateAliasFilter(XContentParser parser) throws IOException {
        QueryBuilder parseInnerQueryBuilder = parseInnerQueryBuilder(parser);
        //QueryBuilder queryBuilder = Rewriteable.rewrite(parseInnerQueryBuilder, queryShardContext, true);
        //queryBuilder.toQuery(queryShardContext);
    }

    public static QueryBuilder parseInnerQueryBuilder(XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new ParsingException(parser.getTokenLocation(), "[_na] query malformed, must start with start_object");
            }
        }
        if (parser.nextToken() == XContentParser.Token.END_OBJECT) {
            // we encountered '{}' for a query clause, it used to be supported, deprecated in 5.0 and removed in 6.0
            throw new IllegalArgumentException("query malformed, empty clause found at [" + parser.getTokenLocation() + "]");
        }
        if (parser.currentToken() != XContentParser.Token.FIELD_NAME) {
            throw new ParsingException(parser.getTokenLocation(), "[_na] query malformed, no field after start_object");
        }
        String queryName = parser.currentName();
        // move to the next START_OBJECT
        if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "[" + queryName + "] query malformed, no start_object after query name");
        }
        QueryBuilder result;
        try {
            result = parser.namedObject(QueryBuilder.class, queryName, null);
        } catch (NamedObjectNotFoundException e) {
            String message = String.format(
                Locale.ROOT,
                "unknown query [%s]%s",
                queryName,
                SuggestingErrorOnUnknown.suggest(queryName, e.getCandidates())
            );
            throw new ParsingException(new XContentLocation(e.getLineNumber(), e.getColumnNumber()), message, e);
        }
        // end_object of the specific query (e.g. match, multi_match etc.) element
        if (parser.currentToken() != XContentParser.Token.END_OBJECT) {
            throw new ParsingException(
                parser.getTokenLocation(),
                "[" + queryName + "] malformed query, expected [END_OBJECT] but found [" + parser.currentToken() + "]"
            );
        }
        // end_object of the query object
        if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            throw new ParsingException(
                parser.getTokenLocation(),
                "[" + queryName + "] malformed query, expected [END_OBJECT] but found [" + parser.currentToken() + "]"
            );
        }
        return result;
    }
}
