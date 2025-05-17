/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata.service;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.lucene.util.CollectionUtil;
import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.action.admin.indices.create.CreateIndexMetadataUpdateRequest;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.AliasValidator;
import org.opensearch.cluster.metadata.ComponentTemplate;
import org.opensearch.cluster.metadata.ComposableIndexTemplate;
import org.opensearch.cluster.metadata.Context;
import org.opensearch.cluster.metadata.DataStream;
import org.opensearch.cluster.metadata.DiffableStringMap;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Template;
import org.opensearch.common.Nullable;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

public class CreateIndexService {

    public IndexMetadata validateRequest(CreateIndexMetadataUpdateRequest request) {
        return null;
    }

    public IndexMetadata createIndex(CreateIndexMetadataUpdateRequest request) {

        //validate
        //build index settings
        //template
        //template mappings
        //build metadata
        return null;
    }

    IndexMetadata buildIndexMetadata(
            String indexName,
            List<AliasMetadata> aliases,
            Settings indexSettings,
            int routingNumShards,
            @Nullable IndexMetadata sourceMetadata,
            boolean isSystem,
            Map<String, DiffableStringMap> customData,
            Context context,
            List<CompressedXContent> templateMappings,
            NamedXContentRegistry xContentRegistry
    )  {

        IndexMetadata.Builder indexMetadataBuilder = createIndexMetadataBuilder(indexName, sourceMetadata, indexSettings, routingNumShards);
        indexMetadataBuilder.system(isSystem);
        // now, update the mappings with the actual source
        Map<String, MappingMetadata> mappingsMetadata = new HashMap<>();

        for (MappingMetadata mappingMd : mappingsMetadata.values()) {
            indexMetadataBuilder.putMapping(mappingMd);
        }

        // apply the aliases in reverse order as the lower index ones have higher order
        for (int i = aliases.size() - 1; i >= 0; i--) {
            indexMetadataBuilder.putAlias(aliases.get(i));
        }

        for (Map.Entry<String, DiffableStringMap> entry : customData.entrySet()) {
            indexMetadataBuilder.putCustom(entry.getKey(), entry.getValue());
        }

        indexMetadataBuilder.context(context);

        indexMetadataBuilder.state(IndexMetadata.State.OPEN);
        return indexMetadataBuilder.build();
    }

    public static List<CompressedXContent> collectMappings(Map<String, ComposableIndexTemplate>  composableTemplate, Map<String, ComponentTemplate> componentTemplates,
        final String templateName, final String indexName)
        throws Exception {
        final ComposableIndexTemplate template = composableTemplate.get(templateName);
        assert template != null : "attempted to resolve mappings for a template ["
            + templateName
            + "] that did not exist in the cluster state";
        if (template == null) {
            return Collections.emptyList();
        }

        List<CompressedXContent> mappings = template.composedOf()
            .stream()
            .map(componentTemplates::get)
            .filter(Objects::nonNull)
            .map(ComponentTemplate::template)
            .map(Template::mappings)
            .filter(Objects::nonNull)
            .collect(Collectors.toCollection(LinkedList::new));
        // Add the actual index template's mappings, since it takes the next precedence
        Optional.ofNullable(template.template()).map(Template::mappings).ifPresent(mappings::add);
        if (template.getDataStreamTemplate() != null && indexName.startsWith(DataStream.BACKING_INDEX_PREFIX)) {
            // add a default mapping for the timestamp field, at the lowest precedence, to make bootstrapping data streams more
            // straightforward as all backing indices are required to have a timestamp field
            String timestampFieldName = template.getDataStreamTemplate().getTimestampField().getName();
            mappings.add(0, new CompressedXContent(getTimestampFieldMapping(timestampFieldName)));
        }

        // Only include timestamp mapping snippet if creating backing index.
        if (indexName.startsWith(DataStream.BACKING_INDEX_PREFIX)) {
            // Only if template has data stream definition this should be added and
            // adding this template last, since timestamp field should have highest precedence:
            Optional.ofNullable(template.getDataStreamTemplate())
                .map(ComposableIndexTemplate.DataStreamTemplate::getDataStreamMappingSnippet)
                .map(mapping -> {
                    try (XContentBuilder builder = MediaTypeRegistry.JSON.contentBuilder()) {
                        builder.value(mapping);
                        return new CompressedXContent(BytesReference.bytes(builder));
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                })
                .ifPresent(mappings::add);
        }

        // Now use context mappings which take the highest precedence
        Optional.ofNullable(template.context())
           // .map(ctx -> findContextTemplateName(state.metadata(), ctx))
            .map(name -> componentTemplates.get(name))
            .map(ComponentTemplate::template)
            .map(Template::mappings)
            .ifPresent(mappings::add);

        return Collections.unmodifiableList(mappings);
    }


    public static String findV2Template(Map<String, ComposableIndexTemplate>  composableTemplate, Map<String, ComponentTemplate> componentTemplates, String indexName, boolean isHidden) {
        final Predicate<String> patternMatchPredicate = pattern -> Regex.simpleMatch(pattern, indexName);
        final Map<ComposableIndexTemplate, String> matchedTemplates = new HashMap<>();
        for (Map.Entry<String, ComposableIndexTemplate> entry : composableTemplate.entrySet()) {
            final String name = entry.getKey();
            final ComposableIndexTemplate template = entry.getValue();
            if (isHidden == false) {
                final boolean matched = template.indexPatterns().stream().anyMatch(patternMatchPredicate);
                if (matched) {
                    matchedTemplates.put(template, name);
                }
            } else {
                final boolean isNotMatchAllTemplate = template.indexPatterns().stream().noneMatch(Regex::isMatchAllPattern);
                if (isNotMatchAllTemplate) {
                    if (template.indexPatterns().stream().anyMatch(patternMatchPredicate)) {
                        matchedTemplates.put(template, name);
                    }
                }
            }
        }

        if (matchedTemplates.size() == 0) {
            return null;
        }

        final List<ComposableIndexTemplate> candidates = new ArrayList<>(matchedTemplates.keySet());
        CollectionUtil.timSort(candidates, Comparator.comparing(ComposableIndexTemplate::priorityOrZero, Comparator.reverseOrder()));

        assert candidates.size() > 0 : "we should have returned early with no candidates";
        ComposableIndexTemplate winner = candidates.get(0);
        String winnerName = matchedTemplates.get(winner);

        // if the winner template is a global template that specifies the `index.hidden` setting (which is not allowed, so it'd be due to
        // a restored index cluster state that modified a component template used by this global template such that it has this setting)
        // we will fail and the user will have to update the index template and remove this setting or update the corresponding component
        // template that contributes to the index template resolved settings
        if (winner.indexPatterns().stream().anyMatch(Regex::isMatchAllPattern)
            && IndexMetadata.INDEX_HIDDEN_SETTING.exists(resolveSettings(composableTemplate, componentTemplates, winnerName))) {
            throw new IllegalStateException(
                "global index template ["
                    + winnerName
                    + "], composed of component templates ["
                    + String.join(",", winner.composedOf())
                    + "] defined the index.hidden setting, which is not allowed"
            );
        }

        return winnerName;
    }

    public static List<Map<String, Object>> collectV2Mappings(
        final String requestMappings,
        final List<CompressedXContent> templateMappings,
        final NamedXContentRegistry xContentRegistry
    ) throws Exception {
        List<Map<String, Object>> result = new ArrayList<>();

        for (CompressedXContent templateMapping : templateMappings) {
            Map<String, Object> parsedTemplateMapping = parseMapping(xContentRegistry, templateMapping.string());
            result.add(parsedTemplateMapping);
        }

        Map<String, Object> parsedRequestMappings = parseMapping(xContentRegistry, requestMappings);
        result.add(parsedRequestMappings);
        return result;
    }

    public static Map<String, Object> parseMapping(NamedXContentRegistry xContentRegistry, String mappingSource) throws IOException {
        try (
            XContentParser parser = MediaTypeRegistry.JSON.xContent()
                .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, mappingSource)
        ) {
            return parser.map();
        }
    }

    private static IndexMetadata.Builder createIndexMetadataBuilder(
        String indexName,
        @Nullable IndexMetadata sourceMetadata,
        Settings indexSettings,
        int routingNumShards
    ) {
        final IndexMetadata.Builder builder = IndexMetadata.builder(indexName);
        builder.setRoutingNumShards(routingNumShards);
        builder.settings(indexSettings);

        if (sourceMetadata != null) {
            /*
             * We need to arrange that the primary term on all the shards in the shrunken index is at least as large as
             * the maximum primary term on all the shards in the source index. This ensures that we have correct
             * document-level semantics regarding sequence numbers in the shrunken index.
             */
            final long primaryTerm = IntStream.range(0, sourceMetadata.getNumberOfShards())
                .mapToLong(sourceMetadata::primaryTerm)
                .max()
                .getAsLong();
            for (int shardId = 0; shardId < builder.numberOfShards(); shardId++) {
                builder.primaryTerm(shardId, primaryTerm);
            }
        }
        return builder;
    }


    public static List<AliasMetadata> resolveAndValidateAliases(
        String index,
        Set<Alias> aliases,
        List<Map<String, AliasMetadata>> templateAliases,
        Map<String, IndexMetadata>  metadata,
        AliasValidator aliasValidator,
        NamedXContentRegistry xContentRegistry
    ) {
        List<AliasMetadata> resolvedAliases = new ArrayList<>();
        for (Alias alias : aliases) {
            aliasValidator.validateAlias(alias, index, metadata);
            if (Strings.hasLength(alias.filter())) {
                aliasValidator.validateAliasFilter(alias.name(), alias.filter(), xContentRegistry);
            }
            AliasMetadata aliasMetadata = AliasMetadata.builder(alias.name())
                .filter(alias.filter())
                .indexRouting(alias.indexRouting())
                .searchRouting(alias.searchRouting())
                .writeIndex(alias.writeIndex())
                .isHidden(alias.isHidden())
                .build();
            resolvedAliases.add(aliasMetadata);
        }

        Map<String, AliasMetadata> templatesAliases = new HashMap<>();
        for (Map<String, AliasMetadata> templateAliasConfig : templateAliases) {
            // handle aliases
            for (Map.Entry<String, AliasMetadata> entry : templateAliasConfig.entrySet()) {
                AliasMetadata aliasMetadata = entry.getValue();
                // if an alias with same name came with the create index request itself,
                // ignore this one taken from the index template
                if (aliases.contains(new Alias(aliasMetadata.alias()))) {
                    continue;
                }
                // if an alias with same name was already processed, ignore this one
                if (templatesAliases.containsKey(entry.getKey())) {
                    continue;
                }

                // Allow templatesAliases to be templated by replacing a token with the
                // name of the index that we are applying it to
                if (aliasMetadata.alias().contains("{index}")) {
                    String templatedAlias = aliasMetadata.alias().replace("{index}", index);
                    aliasMetadata = AliasMetadata.newAliasMetadata(aliasMetadata, templatedAlias);
                }

                aliasValidator.validateAliasMetadata(aliasMetadata, index, metadata);
                if (aliasMetadata.filter() != null) {
                    aliasValidator.validateAliasFilter(
                        aliasMetadata.alias(),
                        aliasMetadata.filter().uncompressed(),
                        xContentRegistry
                    );
                }
                templatesAliases.put(aliasMetadata.alias(), aliasMetadata);
                resolvedAliases.add((aliasMetadata));
            }
        }
        return resolvedAliases;
    }
    private static String getTimestampFieldMapping(String timestampFieldName) {
        return "{\n"
            + "   \"_doc\": {\n"
            + "     \"properties\": {\n"
            + "       \""
            + timestampFieldName
            + "\": {\n"
            + "         \"type\": \"date\"\n"
            + "       }\n"
            + "     }\n"
            + "   }\n"
            + " }";
    }



    /**
     * Resolve the given v2 template into a collected {@link Settings} object
     */
    public static Settings resolveSettings(Map<String, ComposableIndexTemplate>  composableTemplate, Map<String, ComponentTemplate> componentTemplates, final String templateName) {
        final ComposableIndexTemplate template = composableTemplate.get(templateName);
        assert template != null : "attempted to resolve settings for a template ["
            + templateName
            + "] that did not exist in the cluster state";
        if (template == null) {
            return Settings.EMPTY;
        }
        return resolveSettings(componentTemplates, template);
    }

    private static Settings resolveSettings(Map<String, ComponentTemplate> componentTemplates, ComposableIndexTemplate template) {
        List<Settings> componentSettings = template.composedOf()
            .stream()
            .map(componentTemplates::get)
            .filter(Objects::nonNull)
            .map(ComponentTemplate::template)
            .map(Template::settings)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

        Settings.Builder templateSettings = Settings.builder();
        componentSettings.forEach(templateSettings::put);
        // Add the actual index template's settings now, since it takes the next precedence.
        Optional.ofNullable(template.template()).map(Template::settings).ifPresent(templateSettings::put);

        // Add the template referred by context since it will take the highest precedence.
       // final ComponentTemplate componentTemplate = findComponentTemplate(metadata, template.context());
       // Optional.ofNullable(componentTemplate).map(ComponentTemplate::template).map(Template::settings).ifPresent(templateSettings::put);

        return templateSettings.build();
    }


}
