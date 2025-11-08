/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.percolator.PercolatorPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.xpack.esql.analysis.AnalyzerSettings;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.spatial.SpatialPlugin;
import org.elasticsearch.xpack.unsignedlong.UnsignedLongMapperPlugin;
import org.elasticsearch.xpack.versionfield.VersionFieldPlugin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
@LuceneTestCase.SuppressFileSystems(value = "HandleLimitFS")
public class LookupJoinWildcardMatchIT extends ESIntegTestCase {

    private static final Logger logger = LogManager.getLogger(LookupJoinWildcardMatchIT.class);

    /**
     * Verification approach selection.
     * TRIE: Uses a TRIE data structure for efficient prefix matching (faster for large datasets).
     * BRUTE_FORCE: Uses nested loops to check all patterns against all folder paths (slower but simpler).
     */
    private enum VerificationApproach {
        TRIE,
        BRUTE_FORCE
    }

    /**
     * Selects the verification approach. TRIE is enabled by default for better performance.
     */
    private static final VerificationApproach VERIFICATION_APPROACH = VerificationApproach.TRIE;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            EsqlPlugin.class,
            MapperExtrasPlugin.class,
            PercolatorPlugin.class,
            SpatialPlugin.class,
            UnsignedLongMapperPlugin.class,
            VersionFieldPlugin.class
        );
    }

    /**
     * Creates the main index (test_left) with folder_path field
     */
    private void createMainIndex(String indexName) {
        CreateIndexRequestBuilder mainBuilder = prepareCreate(indexName).setMapping("""
            {
              "properties": {
                "join_key_left": {
                  "type": "keyword"
                },
                "value_left": {
                  "type": "keyword"
                },
                "folder": {
                  "type": "keyword"
                },
                "folder_path": {
                  "type": "keyword"
                }
              }
            }
            """)
            .setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.refresh_interval", "0s") // Immediate refresh instead of default 1s
                    .build()
            );
        assertAcked(mainBuilder);
    }

    /**
     * Creates the lookup index (test_right) with term_query_field and wildcard_query_field
     */
    private void createLookupIndex(String indexName) {
        CreateIndexRequestBuilder lookupBuilder = prepareCreate(indexName).setMapping("""
            {
              "properties": {
                "join_key_right": {
                  "type": "keyword"
                },
                "filter_field_kw": {
                  "type": "keyword"
                },
                "filter_field_int": {
                  "type": "integer"
                },
                "folder": {
                  "type": "keyword"
                },
                "term_query_field": {
                  "type": "keyword"
                },
                "wildcard_query_field": {
                  "type": "keyword"
                },
                "min_field": {
                  "type": "keyword"
                },
                "percolator_query": {
                  "type": "percolator"
                }
              }
            }
            """)
            .setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.mode", "lookup")
                    .put("index.refresh_interval", "0s") // Immediate refresh instead of default 1s
                    .build()
            );
        assertAcked(lookupBuilder);
    }

    /**
     * Executes a wildcard match lookup join query and returns the results
     */
    private List<List<Object>> executeWildcardMatchJoinQuery(String mainIndex, String lookupIndex) {
        String query = String.format("""
            FROM %s
            | LOOKUP JOIN %s ON join_key_left == join_key_right AND folder_path >= min_field
            | WHERE join_key_right IS NOT NULL
            | KEEP folder_path, folder, join_key_right, filter_field_kw, term_query_field, wildcard_query_field
            | SORT folder_path, term_query_field, wildcard_query_field
            | LIMIT %d
            """, mainIndex, lookupIndex, Integer.MAX_VALUE);

        logger.info("Starting ESQL query execution");
        try (var response = EsqlQueryRequestBuilder.newRequestBuilder(client()).query(query).get()) {
            // Verify we have the expected columns
            assertThat(response.response().columns().size(), equalTo(6));
            assertThat(response.response().columns().get(0).name(), equalTo("folder_path"));
            assertThat(response.response().columns().get(1).name(), equalTo("folder"));
            assertThat(response.response().columns().get(2).name(), equalTo("join_key_right"));
            assertThat(response.response().columns().get(3).name(), equalTo("filter_field_kw"));
            assertThat(response.response().columns().get(4).name(), equalTo("term_query_field"));
            assertThat(response.response().columns().get(5).name(), equalTo("wildcard_query_field"));

            // Collect the results
            List<List<Object>> values = new ArrayList<>();
            response.response().rows().forEach(iterator -> {
                List<Object> row = new ArrayList<>();
                iterator.forEach(row::add);
                values.add(row);
            });
            logger.info("Finished ESQL query execution, collected {} result rows", values.size());
            return values;
        }
    }

    /**
     * Indexes main data from a list of folder paths.
     * Each folder path becomes a document in the main index.
     *
     * @param indexName The name of the main index
     * @param folderPaths List of folder paths to index
     */
    private void indexMainDataFromPaths(String indexName, List<String> folderPaths) {
        indexMainDataFromPaths(indexName, folderPaths, 1);
    }

    /**
     * Indexes main data from a list of folder paths with a custom ID offset.
     * Each folder path becomes a document in the main index.
     * Uses bulk requests with batching to avoid memory issues with large datasets.
     *
     * @param indexName The name of the main index
     * @param folderPaths List of folder paths to index
     * @param idOffset The starting ID offset (default is 1, so IDs are 1, 2, 3...)
     */
    private void indexMainDataFromPaths(String indexName, List<String> folderPaths, int idOffset) {
        // Use bulk requests with batching to handle large numbers of records efficiently
        // Use Map-based sources instead of JSON strings to avoid memory overhead
        int batchSize = 1000;
        for (int batchStart = 0; batchStart < folderPaths.size(); batchStart += batchSize) {
            BulkRequestBuilder bulk = client().prepareBulk();
            int batchEnd = Math.min(batchStart + batchSize, folderPaths.size());
            for (int i = batchStart; i < batchEnd; i++) {
                String folderPath = folderPaths.get(i);
                int docId = i + idOffset;
                Map<String, Object> source = Map.of(
                    "join_key_left",
                    "A",
                    "value_left",
                    "text " + docId,
                    "folder",
                    folderPath,
                    "folder_path",
                    folderPath
                );
                bulk.add(client().prepareIndex(indexName).setId(String.valueOf(docId)).setSource(source));
            }
            BulkResponse bulkResponse = bulk.get();
            if (bulkResponse.hasFailures()) {
                throw new RuntimeException(
                    "Bulk indexing failed for batch starting at " + batchStart + ": " + bulkResponse.buildFailureMessage()
                );
            }
        }
    }

    /**
     * Indexes lookup data from a list of patterns.
     * Each PatternInfo becomes a document in the lookup index.
     * Uses bulk requests with batching to avoid memory issues with large datasets.
     *
     * @param indexName The name of the lookup index
     * @param patterns List of patterns to index
     */
    private void indexLookupDataFromPatterns(String indexName, List<PatternInfo> patterns) {
        // Use bulk requests with batching to handle large numbers of records efficiently
        // Use Map-based sources instead of JSON strings to avoid memory overhead
        int batchSize = 1000;
        for (int batchStart = 0; batchStart < patterns.size(); batchStart += batchSize) {
            BulkRequestBuilder bulk = client().prepareBulk();
            int batchEnd = Math.min(batchStart + batchSize, patterns.size());
            for (int i = batchStart; i < batchEnd; i++) {
                PatternInfo pattern = patterns.get(i);
                Map<String, Object> source = new HashMap<>();
                source.put("join_key_right", "A");
                source.put("filter_field_kw", "match");
                source.put("filter_field_int", 1);
                source.put("folder", pattern.folderPattern);
                source.put("min_field", "");
                if (pattern.termField != null) {
                    source.put("term_query_field", pattern.termField);
                }
                if (pattern.wildcardField != null) {
                    source.put("wildcard_query_field", pattern.wildcardField);
                }
                // Add percolator query equivalent to the right pattern
                Map<String, Object> percolatorQuery;
                if (pattern.termField != null) {
                    // Exact match: use term query
                    percolatorQuery = Map.of("term", Map.of("folder", pattern.termField));
                } else if (pattern.wildcardField != null) {
                    // Wildcard match: use prefix query with the prefix (without *)
                    percolatorQuery = Map.of("prefix", Map.of("folder", pattern.wildcardField));
                } else {
                    // Fallback: should not happen, but handle gracefully
                    percolatorQuery = Map.of("match_all", Map.of());
                }
                source.put("percolator_query", percolatorQuery);
                bulk.add(client().prepareIndex(indexName).setId(String.valueOf(pattern.id)).setSource(source));
            }
            BulkResponse bulkResponse = bulk.get();
            if (bulkResponse.hasFailures()) {
                throw new RuntimeException(
                    "Bulk indexing failed for batch starting at " + batchStart + ": " + bulkResponse.buildFailureMessage()
                );
            }
        }
    }

    /**
     * Refreshes indices and verifies that the document counts match expected values
     *
     * @param mainIndex The main index name
     * @param expectedMainCount Expected number of documents in the main index
     * @param lookupIndex The lookup index name
     * @param expectedLookupCount Expected number of documents in the lookup index
     */
    private void refreshAndVerifyCounts(String mainIndex, long expectedMainCount, String lookupIndex, long expectedLookupCount) {
        // Refresh the indices
        refresh(mainIndex, lookupIndex);

        // Verify main index count using index stats instead of search
        // Search may have limits on track_total_hits (default is often 10,000)
        var mainStatsResponse = client().admin().indices().prepareStats(mainIndex).setDocs(true).get();
        long actualMainCount = mainStatsResponse.getTotal().docs.getCount();
        assertThat(
            "Main index '" + mainIndex + "' should have " + expectedMainCount + " documents",
            actualMainCount,
            equalTo(expectedMainCount)
        );

        // Verify lookup index count using index stats instead of search
        var lookupStatsResponse = client().admin().indices().prepareStats(lookupIndex).setDocs(true).get();
        long actualLookupCount = lookupStatsResponse.getTotal().docs.getCount();
        assertThat(
            "Lookup index '" + lookupIndex + "' should have " + expectedLookupCount + " documents",
            actualLookupCount,
            equalTo(expectedLookupCount)
        );
    }

    public void testWildcardMatchJoin() throws Exception {
        String mainIndex = "test_left";
        String lookupIndex = "test_right";

        // Define folder paths and patterns
        List<String> leftFolderPaths = Arrays.asList(
            "fan",
            "fola1",
            "fold1",
            "folder1",
            "folder11",
            "folder1.folder5",
            "folder12",
            "folder2",
            "folder2.folder1",
            "folder2.folder11"
        );

        List<PatternInfo> rightPatterns = Arrays.asList(
            new PatternInfo(1, "fan", "fan", null),                      // Pattern 1: fan (exact)
            new PatternInfo(2, "fola*", null, "fola"),                   // Pattern 2: fola* (prefix)
            new PatternInfo(3, "fold*", null, "fold"),                   // Pattern 3: fold* (prefix)
            new PatternInfo(4, "folder1*", null, "folder1"),             // Pattern 4: folder1* (prefix)
            new PatternInfo(5, "folder11", "folder11", null),             // Pattern 5: folder11 (exact)
            new PatternInfo(6, "folder1.folder5", "folder1.folder5", null), // Pattern 6: folder1.folder5 (exact)
            new PatternInfo(7, "folder12", "folder12", null),             // Pattern 7: folder12 (exact)
            new PatternInfo(8, "folder2*", null, "folder2"),               // Pattern 8: folder2* (prefix)
            new PatternInfo(9, "folder2.folder1*", null, "folder2.folder1"), // Pattern 9: folder2.folder1* (prefix)
            new PatternInfo(10, "folder2.folder11", "folder2.folder11", null) // Pattern 10: folder2.folder11 (exact)
        );

        // Create indices
        createMainIndex(mainIndex);
        createLookupIndex(lookupIndex);

        // Index test data from patterns
        indexMainDataFromPaths(mainIndex, leftFolderPaths);
        indexLookupDataFromPatterns(lookupIndex, rightPatterns);

        // Refresh indices and verify counts
        refreshAndVerifyCounts(mainIndex, leftFolderPaths.size(), lookupIndex, rightPatterns.size());

        // Execute query and get results
        List<List<Object>> values = executeWildcardMatchJoinQuery(mainIndex, lookupIndex);

        // Use the shared verification algorithm
        verifyMatches(leftFolderPaths, rightPatterns, values);
    }

    public void testWildcardMatchJoinTermOnly() throws Exception {
        String mainIndex = "test_left_term";
        String lookupIndex = "test_right_term";

        // Define folder paths and patterns
        List<String> leftFolderPaths = Arrays.asList(
            "fan",
            "fola1",
            "fold1",
            "folder1",
            "folder11",
            "folder1.folder5",
            "folder12",
            "folder2",
            "folder2.folder1",
            "folder2.folder11"
        );

        List<PatternInfo> rightPatterns = Arrays.asList(
            new PatternInfo(1, "fan", "fan", null),                      // Pattern 1: fan (exact)
            new PatternInfo(2, "folder11", "folder11", null),            // Pattern 2: folder11 (exact)
            new PatternInfo(3, "folder1.folder5", "folder1.folder5", null), // Pattern 3: folder1.folder5 (exact)
            new PatternInfo(4, "folder12", "folder12", null),             // Pattern 4: folder12 (exact)
            new PatternInfo(5, "folder2.folder11", "folder2.folder11", null) // Pattern 5: folder2.folder11 (exact)
        );

        // Create indices
        createMainIndex(mainIndex);
        createLookupIndex(lookupIndex);

        // Index test data from patterns
        indexMainDataFromPaths(mainIndex, leftFolderPaths);
        indexLookupDataFromPatterns(lookupIndex, rightPatterns);

        // Refresh indices and verify counts
        refreshAndVerifyCounts(mainIndex, leftFolderPaths.size(), lookupIndex, rightPatterns.size());

        // Execute query and get results
        List<List<Object>> values = executeWildcardMatchJoinQuery(mainIndex, lookupIndex);

        // Use the shared verification algorithm
        verifyMatches(leftFolderPaths, rightPatterns, values);
    }

    public void testWildcardMatchJoinWildcardOnly() throws Exception {
        String mainIndex = "test_left_wildcard";
        String lookupIndex = "test_right_wildcard";

        // Define folder paths and patterns
        List<String> leftFolderPaths = Arrays.asList(
            "fan",
            "fola1",
            "fold1",
            "folder1",
            "folder11",
            "folder1.folder5",
            "folder12",
            "folder2",
            "folder2.folder1",
            "folder2.folder11"
        );

        List<PatternInfo> rightPatterns = Arrays.asList(
            new PatternInfo(1, "fola*", null, "fola"),                   // Pattern 1: fola* (prefix)
            new PatternInfo(2, "fold*", null, "fold"),                   // Pattern 2: fold* (prefix)
            new PatternInfo(3, "folder1*", null, "folder1"),             // Pattern 3: folder1* (prefix)
            new PatternInfo(4, "folder2*", null, "folder2"),               // Pattern 4: folder2* (prefix)
            new PatternInfo(5, "folder2.folder1*", null, "folder2.folder1") // Pattern 5: folder2.folder1* (prefix)
        );

        // Create indices
        createMainIndex(mainIndex);
        createLookupIndex(lookupIndex);

        // Index test data from patterns
        indexMainDataFromPaths(mainIndex, leftFolderPaths);
        indexLookupDataFromPatterns(lookupIndex, rightPatterns);

        // Refresh indices and verify counts
        refreshAndVerifyCounts(mainIndex, leftFolderPaths.size(), lookupIndex, rightPatterns.size());

        // Execute query and get results
        List<List<Object>> values = executeWildcardMatchJoinQuery(mainIndex, lookupIndex);

        // Use the shared verification algorithm
        verifyMatches(leftFolderPaths, rightPatterns, values);
    }

    /**
     * Minimal test to verify prefix matching behavior.
     * Tests that both "fold*" and "folder2*" match "folder2" because:
     * - "fold*" matches because "folder2" starts with "fold" (followed by "er2")
     * - "folder2*" matches because "folder2" starts with "folder2" (exact match)
     * Both should match since the * wildcard matches any number of characters.
     */
    public void testWildcardMatchJoinPrefixFiltering() throws Exception {
        String mainIndex = "test_left_minimal";
        String lookupIndex = "test_right_minimal";

        // Define folder paths and patterns
        List<String> leftFolderPaths = Arrays.asList("folder2");

        List<PatternInfo> rightPatterns = Arrays.asList(
            new PatternInfo(1, "fold*", null, "fold"),     // Pattern 1: fold* (prefix - should match folder2)
            new PatternInfo(2, "folder2*", null, "folder2")  // Pattern 2: folder2* (prefix - should match folder2)
        );

        // Create indices
        createMainIndex(mainIndex);
        createLookupIndex(lookupIndex);

        // Index test data from patterns
        indexMainDataFromPaths(mainIndex, leftFolderPaths);
        indexLookupDataFromPatterns(lookupIndex, rightPatterns);

        // Refresh indices and verify counts
        refreshAndVerifyCounts(mainIndex, leftFolderPaths.size(), lookupIndex, rightPatterns.size());

        // Execute query and get results
        List<List<Object>> values = executeWildcardMatchJoinQuery(mainIndex, lookupIndex);

        // Use the shared verification algorithm
        verifyMatches(leftFolderPaths, rightPatterns, values);
    }

    /**
     * Test with random data generation.
     * Generates random folder paths on the left and patterns on the right (exact match and wildcard).
     * Builds expected results and compares with actual results.
     */
    public void testWildcardMatchJoinRandomDataSmall() throws Exception {
        testWildcardMatchJoinWithRandomData(10_000, 10_000, 100);
    }

    public void testWildcardMatchJoinRandomDataMedium() throws Exception {
        testWildcardMatchJoinWithRandomData(20_000, 20_000, 500);
    }

    public void testWildcardMatchJoinRandomDataBig() throws Exception {
        testWildcardMatchJoinWithRandomData(100_000, 4_000, 4_000);
    }

    /*public void testWildcardMatchJoinRandomDataGiant() throws Exception {
        testWildcardMatchJoinWithRandomData(100_000, 100_000, 4_000);
    }*/

    public void testWildcardMatchJoinEmptyLeftIndex() throws Exception {
        String mainIndex = "test_left_empty";
        String lookupIndex = "test_right_empty_left";

        // Create indices
        createMainIndex(mainIndex);
        createLookupIndex(lookupIndex);

        // Left index is empty (no documents)
        List<String> leftFolderPaths = Arrays.asList();

        // Right index has some patterns
        List<PatternInfo> rightPatterns = Arrays.asList(
            new PatternInfo(1, "folder1", "folder1", null),
            new PatternInfo(2, "folder2*", null, "folder2")
        );

        // Index data (empty list for left index means no documents will be indexed)
        indexMainDataFromPaths(mainIndex, leftFolderPaths, 0);
        indexLookupDataFromPatterns(lookupIndex, rightPatterns);

        // Ensure cluster is green and all indexing operations are complete
        ensureGreen(mainIndex, lookupIndex);

        // Refresh indices and verify counts
        refreshAndVerifyCounts(mainIndex, 0, lookupIndex, rightPatterns.size());

        // Execute query - should return no results since left index is empty
        List<List<Object>> values = executeWildcardMatchJoinQuery(mainIndex, lookupIndex);

        // Verify no matches (empty left index means no matches possible)
        assertThat("Empty left index should produce no matches", values.size(), equalTo(0));
    }

    public void testWildcardMatchJoinEmptyRightIndex() throws Exception {
        String mainIndex = "test_left_empty_right";
        String lookupIndex = "test_right_empty";

        // Create indices
        createMainIndex(mainIndex);
        createLookupIndex(lookupIndex);

        // Left index has some folder paths
        List<String> leftFolderPaths = Arrays.asList("folder1", "folder2", "folder11");

        // Right index is empty (no documents)
        List<PatternInfo> rightPatterns = Arrays.asList();

        // Index data (empty list for right index means no documents will be indexed)
        indexMainDataFromPaths(mainIndex, leftFolderPaths, 0);
        indexLookupDataFromPatterns(lookupIndex, rightPatterns);

        // Ensure cluster is green and all indexing operations are complete
        ensureGreen(mainIndex, lookupIndex);

        // Refresh indices and verify counts
        refreshAndVerifyCounts(mainIndex, leftFolderPaths.size(), lookupIndex, 0);

        // Execute query - should return no results since right index is empty
        List<List<Object>> values = executeWildcardMatchJoinQuery(mainIndex, lookupIndex);

        // Verify no matches (empty right index means no matches possible)
        assertThat("Empty right index should produce no matches", values.size(), equalTo(0));
    }

    public void testWildcardMatchJoinStarPatternMatchesAll() throws Exception {
        String mainIndex = "test_left_star";
        String lookupIndex = "test_right_star";

        // Create indices
        createMainIndex(mainIndex);
        createLookupIndex(lookupIndex);

        // Left index has multiple folder paths
        List<String> leftFolderPaths = Arrays.asList("folder1", "folder2", "folder11", "folder22", "abc", "xyz", "test123");

        // Right index has a single pattern "*" that should match all
        // For "*" to match all, we use an empty wildcardField prefix which matches any prefix
        List<PatternInfo> rightPatterns = Arrays.asList(
            new PatternInfo(1, "*", null, "") // Empty prefix matches all
        );

        // Index data
        indexMainDataFromPaths(mainIndex, leftFolderPaths, 0);
        indexLookupDataFromPatterns(lookupIndex, rightPatterns);

        // Ensure cluster is green and all indexing operations are complete
        ensureGreen(mainIndex, lookupIndex);

        // Refresh indices and verify counts
        refreshAndVerifyCounts(mainIndex, leftFolderPaths.size(), lookupIndex, rightPatterns.size());

        // Execute query
        List<List<Object>> values = executeWildcardMatchJoinQuery(mainIndex, lookupIndex);

        // Use the shared verification algorithm
        verifyMatches(leftFolderPaths, rightPatterns, values);
    }

    /**
     * Generates exact match patterns for the right index.
     *
     * @param leftFolderPaths List of folder paths from left index (used as source for some patterns)
     * @param count Number of exact match patterns to generate
     * @param startId Starting pattern ID
     * @param copyProbability Percentage (0-100) probability to copy an existing pattern instead of creating new
     * @return List of generated PatternInfo objects with sequential IDs starting from startId
     */
    private List<PatternInfo> generateExactMatchPatterns(List<String> leftFolderPaths, int count, int startId, int copyProbability) {
        List<PatternInfo> patterns = new ArrayList<>();
        List<PatternInfo> existingExactPatterns = new ArrayList<>();
        int patternId = startId;

        for (int i = 0; i < count; i++) {
            PatternInfo pattern;

            // Check if we should copy an existing exact pattern
            if (randomIntBetween(1, 100) <= copyProbability && existingExactPatterns.isEmpty() == false) {
                // Copy an existing exact pattern
                PatternInfo existingPattern = existingExactPatterns.get(randomIntBetween(0, existingExactPatterns.size() - 1));
                pattern = new PatternInfo(patternId++, existingPattern.folderPattern, existingPattern.termField, null);
                existingExactPatterns.add(pattern);
            } else {
                // Create a new exact match pattern
                if (randomBoolean()) {
                    // 50%: Pick an exact existing pattern (use an existing left folder path)
                    String existingPath = leftFolderPaths.get(randomIntBetween(0, leftFolderPaths.size() - 1));
                    pattern = new PatternInfo(patternId++, existingPath, existingPath, null);
                } else {
                    // 50%: Random string exact match
                    String patternValue = randomAlphanumericOfLengthBetween(1, 20);
                    pattern = new PatternInfo(patternId++, patternValue, patternValue, null);
                }
                existingExactPatterns.add(pattern);
            }
            patterns.add(pattern);
        }

        return patterns;
    }

    /**
     * Generates wildcard match patterns for the right index.
     *
     * @param leftFolderPaths List of folder paths from left index (used as source for some patterns)
     * @param count Number of wildcard patterns to generate
     * @param startId Starting pattern ID
     * @param copyProbability Percentage (0-100) probability to copy an existing wildcard pattern instead of creating new
     * @return List of generated PatternInfo objects with sequential IDs starting from startId
     */
    private List<PatternInfo> generateWildcardPatterns(List<String> leftFolderPaths, int count, int startId, int copyProbability) {
        List<PatternInfo> patterns = new ArrayList<>();

        // With 50% probability, insert a "*" pattern (matches everything)
        // This is independent of other pattern generation
        if (randomBoolean()) {
            patterns.add(new PatternInfo(startId++, "*", null, ""));
        }

        // Track existing wildcard patterns for copying (built as we generate)
        List<PatternInfo> existingWildcardPatterns = new ArrayList<>();
        int patternId = startId;

        for (int i = 0; i < count; i++) {
            PatternInfo pattern;

            // Check if we should copy an existing wildcard pattern
            if (randomIntBetween(1, 100) <= copyProbability && existingWildcardPatterns.isEmpty() == false) {
                // Copy an existing wildcard pattern
                PatternInfo existingPattern = existingWildcardPatterns.get(randomIntBetween(0, existingWildcardPatterns.size() - 1));
                pattern = new PatternInfo(patternId++, existingPattern.folderPattern, null, existingPattern.wildcardField);
                existingWildcardPatterns.add(pattern);
            } else {
                // Create a new wildcard pattern
                int patternType = randomIntBetween(0, 2);

                if (patternType == 0) {
                    // 33%: Pick a substring of existing pattern, then make it wildcard
                    String existingPath = leftFolderPaths.get(randomIntBetween(0, leftFolderPaths.size() - 1));
                    if (existingPath.length() > 0) {
                        // Pick a random substring of the existing path
                        int startPos = randomIntBetween(0, Math.max(0, existingPath.length() - 1));
                        int endPos = randomIntBetween(startPos + 1, existingPath.length());
                        String substring = existingPath.substring(startPos, endPos);
                        pattern = new PatternInfo(patternId++, substring + "*", null, substring);
                    } else {
                        // Fallback if path is empty
                        String patternValue = randomAlphanumericOfLengthBetween(1, 20);
                        pattern = new PatternInfo(patternId++, patternValue + "*", null, patternValue);
                    }
                } else if (patternType == 1) {
                    // 33%: Random pattern wildcard
                    String patternValue = randomAlphanumericOfLengthBetween(1, 20);
                    pattern = new PatternInfo(patternId++, patternValue + "*", null, patternValue);
                } else {
                    // 33%: Prefix substring wildcard (prefix of existing path with wildcard)
                    String existingPath = leftFolderPaths.get(randomIntBetween(0, leftFolderPaths.size() - 1));
                    if (existingPath.length() > 0) {
                        // Pick a prefix substring of the existing path (starting from position 0)
                        int prefixLength = randomIntBetween(1, existingPath.length());
                        String prefix = existingPath.substring(0, prefixLength);
                        pattern = new PatternInfo(patternId++, prefix + "*", null, prefix);
                    } else {
                        // Fallback if path is empty
                        String patternValue = randomAlphanumericOfLengthBetween(1, 20);
                        pattern = new PatternInfo(patternId++, patternValue + "*", null, patternValue);
                    }
                }
                existingWildcardPatterns.add(pattern);
            }
            patterns.add(pattern);
        }

        return patterns;
    }

    private void testWildcardMatchJoinWithRandomData(int leftRows, int rightExactRows, int rightWildcardRows) throws Exception {
        String mainIndex = "test_left_random";
        String lookupIndex = "test_right_random";

        // Create indices
        createMainIndex(mainIndex);
        createLookupIndex(lookupIndex);

        // Generate random folder paths for left index
        List<String> leftFolderPaths = new ArrayList<>();
        for (int i = 0; i < leftRows; i++) {
            // Generate random alphanumeric folder path between 1 and 100 characters
            String folderPath = randomAlphanumericOfLengthBetween(1, 100);
            leftFolderPaths.add(folderPath);
        }

        if (leftFolderPaths.isEmpty()) {
            throw new IllegalArgumentException("leftFolderPaths cannot be empty");
        }

        // Generate patterns for right index
        List<PatternInfo> rightPatterns = new ArrayList<>();
        int patternId = 0;

        // Generate exact match patterns (50% from existing left paths, 50% random strings)
        // 10% probability to copy existing patterns (to test duplicate patterns)
        List<PatternInfo> exactPatterns = generateExactMatchPatterns(leftFolderPaths, rightExactRows, patternId, 10);
        rightPatterns.addAll(exactPatterns);
        patternId += exactPatterns.size();

        // Generate wildcard match patterns
        // 10% probability to copy an existing wildcard pattern (to test duplicate patterns)
        List<PatternInfo> wildcardPatterns = generateWildcardPatterns(leftFolderPaths, rightWildcardRows, patternId, 10);
        rightPatterns.addAll(wildcardPatterns);

        // Index data from patterns
        logger.info("Starting to populate main index '{}' with {} documents", mainIndex, leftFolderPaths.size());
        indexMainDataFromPaths(mainIndex, leftFolderPaths, 0);
        logger.info("Finished populating main index '{}'", mainIndex);

        logger.info("Starting to populate lookup index '{}' with {} documents", lookupIndex, rightPatterns.size());
        indexLookupDataFromPatterns(lookupIndex, rightPatterns);
        logger.info("Finished populating lookup index '{}'", lookupIndex);

        // Ensure cluster is green and all indexing operations are complete
        ensureGreen(mainIndex, lookupIndex);

        // Refresh indices and verify counts
        refreshAndVerifyCounts(mainIndex, leftFolderPaths.size(), lookupIndex, rightPatterns.size());

        // Increase the result truncation max size to allow large result sets
        // The default is 10,000 which caps LIMIT clauses
        // Maximum allowed value is 1,000,000
        Settings settings = Settings.builder().put(AnalyzerSettings.QUERY_RESULT_TRUNCATION_MAX_SIZE.getKey(), 1_000_000).build();
        ClusterUpdateSettingsRequest settingsRequest = new ClusterUpdateSettingsRequest(
            TimeValue.timeValueSeconds(30),
            TimeValue.timeValueSeconds(30)
        ).persistentSettings(settings);
        client().admin().cluster().updateSettings(settingsRequest).actionGet();

        try {
            // Execute query with WHERE filter to get only matched rows
            List<List<Object>> values = executeWildcardMatchJoinQuery(mainIndex, lookupIndex);

            // Use the shared verification algorithm
            logger.info("Starting result validation");
            verifyMatches(leftFolderPaths, rightPatterns, values);
            logger.info("Finished result validation");
        } finally {
            // Clear the persistent setting to avoid leaving cluster metadata behind
            clearPersistentSettings(AnalyzerSettings.QUERY_RESULT_TRUNCATION_MAX_SIZE);
        }
    }

    /**
     * Minimal test case to debug folder2.folder11 matching
     * This test creates only folder2.folder11 in the left index and all relevant patterns in the right index
     * Expected matches for folder2.folder11:
     * - term:folder2.folder11 (exact match)
     * - wildcard:fold (prefix match)
     * - wildcard:folder2 (prefix match)
     * - wildcard:folder2.folder1 (prefix match)
     * Total: 4 matches
     */
    public void testWildcardMatchJoinFolder2Folder11Debug() throws Exception {
        String mainIndex = "test_left_debug";
        String lookupIndex = "test_right_debug";

        List<String> leftFolderPaths = Arrays.asList("folder2.folder11");

        List<PatternInfo> rightPatterns = Arrays.asList(
            new PatternInfo(1, "fold*", null, "fold"),                   // Pattern 1: fold* (prefix)
            new PatternInfo(2, "folder2*", null, "folder2"),               // Pattern 2: folder2* (prefix)
            new PatternInfo(3, "folder2.folder1*", null, "folder2.folder1"), // Pattern 3: folder2.folder1* (prefix)
            new PatternInfo(4, "folder2.folder11", "folder2.folder11", null) // Pattern 4: folder2.folder11 (exact)
        );

        // Create indices
        createMainIndex(mainIndex);
        createLookupIndex(lookupIndex);

        // Index test data from patterns
        indexMainDataFromPaths(mainIndex, leftFolderPaths);
        indexLookupDataFromPatterns(lookupIndex, rightPatterns);

        // Refresh indices and verify counts
        refreshAndVerifyCounts(mainIndex, leftFolderPaths.size(), lookupIndex, rightPatterns.size());

        // Execute query and get results
        List<List<Object>> values = executeWildcardMatchJoinQuery(mainIndex, lookupIndex);

        // Use the shared verification algorithm
        verifyMatches(leftFolderPaths, rightPatterns, values);
    }

    /**
     * Test case that reproduces the issue where findMatchingPrefixTerms skips matching terms.
     *
     * Scenario:
     * - Search value: "abc"
     * - Wildcard terms: "a*", "ab*", "abc*"
     * - All three should match "abc"
     * - Bug: When we find "a*" at prefixLength=1, we advance to prefixLength=3 (foundTerm.length + 1 = 2 + 1)
     *   This skips checking "ab*" which should also match
     * - Fix: Should advance to prefixLength=2 (foundTerm.length) since the * doesn't count for prefix length
     */
    public void testWildcardMatchJoinSkipsTerms() throws Exception {
        String mainIndex = "test_left_skip";
        String lookupIndex = "test_right_skip";

        // Define folder paths and patterns
        List<String> leftFolderPaths = Arrays.asList("abc");

        List<PatternInfo> rightPatterns = Arrays.asList(
            new PatternInfo(1, "a*", null, "a"),                   // Pattern 1: a* (prefix)
            new PatternInfo(2, "ab*", null, "ab"),                 // Pattern 2: ab* (prefix) - This gets skipped!
            new PatternInfo(3, "abc*", null, "abc")                 // Pattern 3: abc* (prefix)
        );

        // Create indices
        createMainIndex(mainIndex);
        createLookupIndex(lookupIndex);

        // Index test data from patterns
        indexMainDataFromPaths(mainIndex, leftFolderPaths);
        indexLookupDataFromPatterns(lookupIndex, rightPatterns);

        // Refresh indices and verify counts
        refreshAndVerifyCounts(mainIndex, leftFolderPaths.size(), lookupIndex, rightPatterns.size());

        // Execute query and get results
        List<List<Object>> values = executeWildcardMatchJoinQuery(mainIndex, lookupIndex);

        // Use the shared verification algorithm
        // This should fail with the bug: expects 3 matches but only finds 2 (missing "ab*")
        verifyMatches(leftFolderPaths, rightPatterns, values);
    }

    /**
     * Verifies that query results match expected matches based on folder paths and patterns.
     * This is the core verification algorithm used by all tests.
     *
     * @param leftFolderPaths List of folder paths from left index (may contain duplicates)
     * @param rightPatterns List of patterns from right index
     * @param values Query results from ESQL query
     */
    private void verifyMatches(List<String> leftFolderPaths, List<PatternInfo> rightPatterns, List<List<Object>> values) {
        if (VERIFICATION_APPROACH == VerificationApproach.TRIE) {
            verifyMatchesWithTrie(leftFolderPaths, rightPatterns, values);
        } else {
            verifyMatchesBruteForce(leftFolderPaths, rightPatterns, values);
        }
    }

    /**
     * Verifies matches using a TRIE data structure for efficient prefix matching.
     * This approach is faster for large datasets as it avoids nested loops.
     *
     * @param leftFolderPaths List of folder paths from left index (may contain duplicates)
     * @param rightPatterns List of patterns from right index
     * @param values Query results from ESQL query
     */
    private void verifyMatchesWithTrie(List<String> leftFolderPaths, List<PatternInfo> rightPatterns, List<List<Object>> values) {
        // First, count how many times each folder path appears in leftFolderPaths
        Map<String, Integer> folderPathCounts = new HashMap<>();
        for (String folderPath : leftFolderPaths) {
            folderPathCounts.put(folderPath, folderPathCounts.getOrDefault(folderPath, 0) + 1);
        }

        // Build TRIE for wildcard patterns and exact match map for term patterns
        PatternTrie wildcardTrie = new PatternTrie();
        Map<String, List<PatternInfo>> exactMatchMap = new HashMap<>();

        for (PatternInfo pattern : rightPatterns) {
            if (pattern.termField != null) {
                exactMatchMap.computeIfAbsent(pattern.termField, k -> new ArrayList<>()).add(pattern);
            }
            if (pattern.wildcardField != null) {
                wildcardTrie.insert(pattern.wildcardField, pattern);
            }
        }

        // For each unique folder path, find all patterns that match it using TRIE
        Map<String, List<Match>> matchesPerFolderPath = new HashMap<>();
        for (String folderPath : folderPathCounts.keySet()) {
            Set<Integer> addedPatternIds = new HashSet<>();
            List<Match> matches = new ArrayList<>();

            // Check exact matches
            List<PatternInfo> exactMatches = exactMatchMap.get(folderPath);
            if (exactMatches != null) {
                for (PatternInfo pattern : exactMatches) {
                    matches.add(new Match(pattern.termField, null));
                    addedPatternIds.add(pattern.id);
                }
            }

            // Check wildcard matches using TRIE
            List<PatternInfo> wildcardMatches = wildcardTrie.findAllMatchingPrefixes(folderPath);
            for (PatternInfo pattern : wildcardMatches) {
                if (addedPatternIds.contains(pattern.id) == false) {
                    matches.add(new Match(null, pattern.wildcardField));
                    addedPatternIds.add(pattern.id);
                }
            }

            // Only add entries that have matches (unmatched rows are filtered out by WHERE clause)
            if (matches.isEmpty() == false) {
                matchesPerFolderPath.put(folderPath, matches);
            }
        }

        // Build expected matches accounting for duplicate folder paths
        // Each occurrence of a folder path in leftFolderPaths creates a separate row in results
        Map<String, List<Match>> expectedMatches = new HashMap<>();
        for (Map.Entry<String, List<Match>> entry : matchesPerFolderPath.entrySet()) {
            String folderPath = entry.getKey();
            List<Match> matches = entry.getValue();
            int count = folderPathCounts.get(folderPath);
            // Multiply matches by the number of times this folder path appears
            // Each occurrence gets the same matches
            List<Match> allMatches = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                allMatches.addAll(matches);
            }
            // Sort matches for consistent comparison
            allMatches.sort(Match.COMPARATOR);
            expectedMatches.put(folderPath, allMatches);
        }

        // Calculate expected total number of result rows (sum of all matches)
        int expectedTotalRows = expectedMatches.values().stream().mapToInt(List::size).sum();

        // Build actual results map
        Map<String, List<Match>> actualMatches = new HashMap<>();
        for (List<Object> row : values) {
            String folderPath = (String) row.get(0);
            String termField = (String) row.get(4);
            String wildcardField = (String) row.get(5);
            actualMatches.computeIfAbsent(folderPath, k -> new ArrayList<>()).add(new Match(termField, wildcardField));
        }

        // Sort matches for consistent comparison
        for (List<Match> matches : actualMatches.values()) {
            matches.sort(Match.COMPARATOR);
        }

        // Collect differences before assertions
        List<String> missingMatches = new ArrayList<>();
        List<String> extraMatches = new ArrayList<>();

        // Check for missing matches (in expected but not in actual)
        for (Map.Entry<String, List<Match>> entry : expectedMatches.entrySet()) {
            String folderPath = entry.getKey();
            List<Match> expected = entry.getValue();
            List<Match> actual = actualMatches.get(folderPath);

            if (actual == null) {
                for (Match match : expected) {
                    missingMatches.add(
                        String.format(
                            "Missing: folderPath=%s, termField=%s, wildcardField=%s",
                            folderPath,
                            match.termField,
                            match.wildcardField
                        )
                    );
                }
            } else {
                // Create copies to avoid modifying originals
                List<Match> expectedCopy = new ArrayList<>(expected);
                List<Match> actualCopy = new ArrayList<>(actual);

                // Find matches in expected but not in actual
                for (Match expectedMatch : expected) {
                    if (actualCopy.remove(expectedMatch) == false) {
                        missingMatches.add(
                            String.format(
                                "Missing: folderPath=%s, termField=%s, wildcardField=%s",
                                folderPath,
                                expectedMatch.termField,
                                expectedMatch.wildcardField
                            )
                        );
                    }
                }

                // Remaining in actualCopy are extra matches
                for (Match extraMatch : actualCopy) {
                    extraMatches.add(
                        String.format(
                            "Extra: folderPath=%s, termField=%s, wildcardField=%s",
                            folderPath,
                            extraMatch.termField,
                            extraMatch.wildcardField
                        )
                    );
                }
            }
        }

        // Check for extra folder paths (in actual but not in expected)
        for (Map.Entry<String, List<Match>> entry : actualMatches.entrySet()) {
            String folderPath = entry.getKey();
            if (expectedMatches.containsKey(folderPath) == false) {
                for (Match match : entry.getValue()) {
                    extraMatches.add(
                        String.format(
                            "Extra: folderPath=%s, termField=%s, wildcardField=%s",
                            folderPath,
                            match.termField,
                            match.wildcardField
                        )
                    );
                }
            }
        }

        // Print first 10 differences
        if (missingMatches.isEmpty() == false || extraMatches.isEmpty() == false) {
            logger.error("Found mismatches. First 10 missing matches:");
            for (int i = 0; i < Math.min(10, missingMatches.size()); i++) {
                logger.error("  {}", missingMatches.get(i));
            }
            logger.error("First 10 extra matches:");
            for (int i = 0; i < Math.min(10, extraMatches.size()); i++) {
                logger.error("  {}", extraMatches.get(i));
            }
        }

        // Verify total number of result rows equals expected total matches
        assertThat("Total number of result rows should equal expected total matches", values.size(), equalTo(expectedTotalRows));

        // Verify only matched folder paths are present in results
        assertThat("Only matched folder paths should be present in results", actualMatches.size(), equalTo(expectedMatches.size()));

        // Compare expected vs actual for each folder path
        // Note: Since we've already verified sizes match, checking all expected entries ensures no unexpected entries exist
        for (Map.Entry<String, List<Match>> entry : expectedMatches.entrySet()) {
            String folderPath = entry.getKey();
            List<Match> expected = entry.getValue();
            List<Match> actual = actualMatches.get(folderPath);

            assertThat("Folder path " + folderPath + " should be present in results", actual != null, equalTo(true));
            assertThat("Matches for folder path " + folderPath + " should match expected", actual, equalTo(expected));
        }
    }

    /**
     * Verifies matches using brute force nested loops (original implementation).
     * This approach is simpler but slower for large datasets.
     *
     * @param leftFolderPaths List of folder paths from left index (may contain duplicates)
     * @param rightPatterns List of patterns from right index
     * @param values Query results from ESQL query
     */
    private void verifyMatchesBruteForce(List<String> leftFolderPaths, List<PatternInfo> rightPatterns, List<List<Object>> values) {
        // First, count how many times each folder path appears in leftFolderPaths
        Map<String, Integer> folderPathCounts = new HashMap<>();
        for (String folderPath : leftFolderPaths) {
            folderPathCounts.put(folderPath, folderPathCounts.getOrDefault(folderPath, 0) + 1);
        }

        // For each unique folder path, find all patterns that match it
        Map<String, List<Match>> matchesPerFolderPath = new HashMap<>();
        for (String folderPath : folderPathCounts.keySet()) {
            Set<Integer> addedPatternIds = new HashSet<>();
            List<Match> matches = new ArrayList<>();
            for (PatternInfo pattern : rightPatterns) {
                // Check exact match
                if (pattern.termField != null && folderPath.equals(pattern.termField)) {
                    matches.add(new Match(pattern.termField, null));
                    addedPatternIds.add(pattern.id);
                }
                // Check wildcard match separately
                // Note: A pattern should only have termField OR wildcardField, not both
                // But we check both separately to be safe
                if (pattern.wildcardField != null && folderPath.startsWith(pattern.wildcardField)) {
                    // Only add if we didn't already add an exact match for this pattern
                    // (to avoid double-counting if a pattern somehow has both)
                    if (addedPatternIds.contains(pattern.id) == false) {
                        matches.add(new Match(null, pattern.wildcardField));
                        addedPatternIds.add(pattern.id);
                    }
                }
            }
            // Only add entries that have matches (unmatched rows are filtered out by WHERE clause)
            if (matches.isEmpty() == false) {
                matchesPerFolderPath.put(folderPath, matches);
            }
        }

        // Build expected matches accounting for duplicate folder paths
        // Each occurrence of a folder path in leftFolderPaths creates a separate row in results
        Map<String, List<Match>> expectedMatches = new HashMap<>();
        for (Map.Entry<String, List<Match>> entry : matchesPerFolderPath.entrySet()) {
            String folderPath = entry.getKey();
            List<Match> matches = entry.getValue();
            int count = folderPathCounts.get(folderPath);
            // Multiply matches by the number of times this folder path appears
            // Each occurrence gets the same matches
            List<Match> allMatches = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                allMatches.addAll(matches);
            }
            // Sort matches for consistent comparison
            allMatches.sort(Match.COMPARATOR);
            expectedMatches.put(folderPath, allMatches);
        }

        // Calculate expected total number of result rows (sum of all matches)
        int expectedTotalRows = expectedMatches.values().stream().mapToInt(List::size).sum();

        // Verify total number of result rows equals expected total matches
        assertThat("Total number of result rows should equal expected total matches", values.size(), equalTo(expectedTotalRows));

        // Build actual results map
        Map<String, List<Match>> actualMatches = new HashMap<>();
        for (List<Object> row : values) {
            String folderPath = (String) row.get(0);
            String termField = (String) row.get(4);
            String wildcardField = (String) row.get(5);
            actualMatches.computeIfAbsent(folderPath, k -> new ArrayList<>()).add(new Match(termField, wildcardField));
        }

        // Sort matches for consistent comparison
        for (List<Match> matches : actualMatches.values()) {
            matches.sort(Match.COMPARATOR);
        }

        // Verify only matched folder paths are present in results
        assertThat("Only matched folder paths should be present in results", actualMatches.size(), equalTo(expectedMatches.size()));

        // Compare expected vs actual for each folder path
        // Note: Since we've already verified sizes match, checking all expected entries ensures no unexpected entries exist
        for (Map.Entry<String, List<Match>> entry : expectedMatches.entrySet()) {
            String folderPath = entry.getKey();
            List<Match> expected = entry.getValue();
            List<Match> actual = actualMatches.get(folderPath);

            assertThat("Folder path " + folderPath + " should be present in results", actual != null, equalTo(true));
            assertThat("Matches for folder path " + folderPath + " should match expected", actual, equalTo(expected));
        }
    }

    /**
     * Generate a random ASCII alphanumeric string (a-z, A-Z, 0-9) of the specified length.
     * This ensures compatibility with lexers that may not handle Unicode characters.
     */
    private String randomAlphanumericOfLengthBetween(int minLength, int maxLength) {
        int length = randomIntBetween(minLength, maxLength);
        StringBuilder sb = new StringBuilder(length);
        String chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        for (int i = 0; i < length; i++) {
            sb.append(chars.charAt(randomIntBetween(0, chars.length() - 1)));
        }
        return sb.toString();
    }

    /**
     * Clear persistent cluster settings to avoid leaving cluster metadata behind after tests.
     */
    private void clearPersistentSettings(Setting<?>... settings) {
        Settings.Builder clearedSettings = Settings.builder();
        for (Setting<?> s : settings) {
            clearedSettings.putNull(s.getKey());
        }
        ClusterUpdateSettingsRequest clearSettingsRequest = new ClusterUpdateSettingsRequest(
            TimeValue.timeValueSeconds(30),
            TimeValue.timeValueSeconds(30)
        ).persistentSettings(clearedSettings.build());
        client().admin().cluster().updateSettings(clearSettingsRequest).actionGet();
    }

    /**
     * TRIE data structure for efficient prefix matching of wildcard patterns.
     */
    private static class PatternTrie {
        private final TrieNode root = new TrieNode();

        void insert(String prefix, PatternInfo pattern) {
            TrieNode current = root;
            for (int i = 0; i < prefix.length(); i++) {
                char c = prefix.charAt(i);
                current = current.children.computeIfAbsent(c, k -> new TrieNode());
            }
            if (current.patterns == null) {
                current.patterns = new ArrayList<>();
            }
            current.patterns.add(pattern);
        }

        /**
         * Find all prefix patterns that match the given string.
         * A prefix pattern matches if the string starts with the prefix.
         * Special case: empty prefix ("") matches everything.
         *
         * @param str The string to match against (e.g., a folder path)
         * @return List of all matching patterns
         */
        List<PatternInfo> findAllMatchingPrefixes(String str) {
            List<PatternInfo> results = new ArrayList<>();

            // Special case: empty prefix ("*" pattern) matches everything
            // Check root node first for empty prefix patterns
            if (root.patterns != null) {
                results.addAll(root.patterns);
            }

            TrieNode current = root;

            // Traverse the TRIE following the string characters
            for (int i = 0; i < str.length(); i++) {
                char c = str.charAt(i);
                TrieNode next = current.children.get(c);
                if (next == null) {
                    // No more matching prefixes
                    break;
                }
                current = next;

                // Collect all patterns at this node (prefixes that match up to this point)
                if (current.patterns != null) {
                    results.addAll(current.patterns);
                }
            }

            return results;
        }

        /**
         * TRIE node representing a character in the prefix tree.
         */
        private static class TrieNode {
            Map<Character, TrieNode> children = new HashMap<>();
            List<PatternInfo> patterns = null;  // Patterns matching this prefix (null if not a complete prefix)
        }
    }

    /**
     * Helper class to store pattern information for right index
     */
    private static class PatternInfo {
        final int id;
        final String folderPattern; // The pattern string (e.g., "folder1*" or "folder1")
        final String termField; // Exact match value, or null if wildcard
        final String wildcardField; // Wildcard prefix, or null if exact match

        PatternInfo(int id, String folderPattern, String termField, String wildcardField) {
            this.id = id;
            this.folderPattern = folderPattern;
            this.termField = termField;
            this.wildcardField = wildcardField;
        }
    }

    /**
     * Helper class to store match information (used for both expected and actual matches)
     */
    private static class Match {
        final String termField; // Exact match value, or null if wildcard match
        final String wildcardField; // Wildcard prefix, or null if exact match

        static final Comparator<Match> COMPARATOR = Comparator.comparing(
            (Match m) -> m.termField,
            Comparator.nullsFirst(Comparator.naturalOrder())
        ).thenComparing((Match m) -> m.wildcardField, Comparator.nullsFirst(Comparator.naturalOrder()));

        Match(String termField, String wildcardField) {
            this.termField = termField;
            this.wildcardField = wildcardField;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Match match = (Match) o;
            return Objects.equals(termField, match.termField) && Objects.equals(wildcardField, match.wildcardField);
        }

        @Override
        public int hashCode() {
            return Objects.hash(termField, wildcardField);
        }

        @Override
        public String toString() {
            return "Match{termField='" + termField + "', wildcardField='" + wildcardField + "'}";
        }
    }
}
