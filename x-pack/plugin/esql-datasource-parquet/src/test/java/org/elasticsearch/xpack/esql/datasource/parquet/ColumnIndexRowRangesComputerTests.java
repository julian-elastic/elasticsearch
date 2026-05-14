/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.format.BoundaryOrder;
import org.apache.parquet.format.ColumnIndex;
import org.apache.parquet.format.OffsetIndex;
import org.apache.parquet.format.PageLocation;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.test.ESTestCase;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Unit tests for {@link ColumnIndexRowRangesComputer} verifying that page-level min/max
 * evaluation produces correct RowRanges for various predicate types.
 */
public class ColumnIndexRowRangesComputerTests extends ESTestCase {

    private static final long ROW_GROUP_ROW_COUNT = 1000;
    private static final int PAGE_SIZE = 100;
    private static final int PAGE_COUNT = 10;

    public void testEqIntIncludesMatchingPage() {
        PreloadedRowGroupMetadata metadata = buildIntMetadata("id", PAGE_COUNT, PAGE_SIZE);
        FilterPredicate pred = FilterApi.eq(FilterApi.intColumn("id"), 550);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, ROW_GROUP_ROW_COUNT);

        assertTrue("Page containing 550 should be included", result.overlaps(500, 600));
        assertFalse("Page [0,100) should be excluded", result.overlaps(0, 100));
        assertFalse("Page [200,300) should be excluded", result.overlaps(200, 300));
    }

    public void testEqIntExcludesAllPages() {
        PreloadedRowGroupMetadata metadata = buildIntMetadata("id", PAGE_COUNT, PAGE_SIZE);
        FilterPredicate pred = FilterApi.eq(FilterApi.intColumn("id"), 5000);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, ROW_GROUP_ROW_COUNT);
        assertEquals("No pages should match value outside range", 0, result.selectedRowCount());
    }

    public void testGtIntIncludesHighPages() {
        PreloadedRowGroupMetadata metadata = buildIntMetadata("id", PAGE_COUNT, PAGE_SIZE);
        FilterPredicate pred = FilterApi.gt(FilterApi.intColumn("id"), 700);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, ROW_GROUP_ROW_COUNT);

        assertTrue("Page [700,800) should be included (max=799 > 700)", result.overlaps(700, 800));
        assertTrue("Page [800,900) should be included", result.overlaps(800, 900));
        assertTrue("Page [900,1000) should be included", result.overlaps(900, 1000));
        assertFalse("Page [0,100) should be excluded (max=99, not > 700)", result.overlaps(0, 100));
    }

    public void testGtEqIntIncludesMatchingPages() {
        PreloadedRowGroupMetadata metadata = buildIntMetadata("id", PAGE_COUNT, PAGE_SIZE);
        FilterPredicate pred = FilterApi.gtEq(FilterApi.intColumn("id"), 500);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, ROW_GROUP_ROW_COUNT);

        assertTrue("Page [500,600) should be included", result.overlaps(500, 600));
        assertTrue("Page [900,1000) should be included", result.overlaps(900, 1000));
        assertFalse("Page [0,100) should be excluded", result.overlaps(0, 100));
    }

    public void testLtIntIncludesLowPages() {
        PreloadedRowGroupMetadata metadata = buildIntMetadata("id", PAGE_COUNT, PAGE_SIZE);
        FilterPredicate pred = FilterApi.lt(FilterApi.intColumn("id"), 200);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, ROW_GROUP_ROW_COUNT);

        assertTrue("Page [0,100) should be included", result.overlaps(0, 100));
        assertTrue("Page [100,200) should be included (min=100 < 200)", result.overlaps(100, 200));
        assertFalse("Page [300,400) should be excluded (min=300, not < 200)", result.overlaps(300, 400));
    }

    public void testLtEqIntIncludesMatchingPages() {
        PreloadedRowGroupMetadata metadata = buildIntMetadata("id", PAGE_COUNT, PAGE_SIZE);
        FilterPredicate pred = FilterApi.ltEq(FilterApi.intColumn("id"), 199);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, ROW_GROUP_ROW_COUNT);

        assertTrue("Page [0,100) should be included", result.overlaps(0, 100));
        assertTrue("Page [100,200) should be included (min=100 <= 199)", result.overlaps(100, 200));
        assertFalse("Page [200,300) should be excluded (min=200, not <= 199)", result.overlaps(200, 300));
    }

    public void testNotEqPrunesConstantPagesMatchingValue() {
        // Half the pages are constant value 5 (the excluded value), the other half are constant 99.
        // NotEq(5) must prune the value-5 pages and keep the rest. Result density must be low
        // enough to survive the RowRanges discard heuristic, hence pruning many pages here.
        int[] pageValues = { 5, 5, 5, 5, 5, 99, 99, 99, 99, 99 };
        PreloadedRowGroupMetadata metadata = buildConstantIntMetadata("id", pageValues, PAGE_SIZE);
        FilterPredicate pred = FilterApi.notEq(FilterApi.intColumn("id"), 5);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, ROW_GROUP_ROW_COUNT);

        assertFalse("Page [0,100) (constant value 5) should be pruned by NotEq(5)", result.overlaps(0, 100));
        assertFalse("Page [400,500) (constant value 5) should be pruned by NotEq(5)", result.overlaps(400, 500));
        assertTrue("Page [500,600) (constant value 99) should survive", result.overlaps(500, 600));
        assertTrue("Page [900,1000) (constant value 99) should survive", result.overlaps(900, 1000));
        assertEquals("Exactly the value-99 pages survive", 500L, result.selectedRowCount());
    }

    public void testNotEqKeepsConstantPagesNotMatchingValue() {
        // None of the constant pages equal the excluded value, so every page survives the predicate.
        // Because the result selects all rows, RowRanges.shouldDiscard() normalizes it to the all-rows
        // sentinel, which is what isAll() observes here. The assertion verifies semantic correctness:
        // NotEq never wrongly drops rows when no page can be safely pruned.
        int[] pageValues = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        PreloadedRowGroupMetadata metadata = buildConstantIntMetadata("id", pageValues, PAGE_SIZE);
        FilterPredicate pred = FilterApi.notEq(FilterApi.intColumn("id"), 42);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, ROW_GROUP_ROW_COUNT);

        assertTrue("All pages should survive when value matches no constant page", result.isAll());
    }

    public void testNotEqKeepsPagesWithMixedValues() {
        // Pages have min < max, so NotEq cannot prune any page (only min == max == value is prunable).
        // Same isAll() / shouldDiscard() caveat as testNotEqKeepsConstantPagesNotMatchingValue.
        PreloadedRowGroupMetadata metadata = buildIntMetadata("id", PAGE_COUNT, PAGE_SIZE);
        FilterPredicate pred = FilterApi.notEq(FilterApi.intColumn("id"), 500);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, ROW_GROUP_ROW_COUNT);

        assertTrue("NotEq should keep all pages when no page has min == max == value", result.isAll());
    }

    public void testNotEqBinaryPrunesConstantPagesMatchingValue() {
        String[] pageValues = { "active", "active", "active", "active", "active", "idle", "idle", "idle", "idle", "idle" };
        PreloadedRowGroupMetadata metadata = buildConstantBinaryMetadata("status", pageValues, PAGE_SIZE);
        FilterPredicate pred = FilterApi.notEq(FilterApi.binaryColumn("status"), Binary.fromString("active"));

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, ROW_GROUP_ROW_COUNT);

        assertFalse("Page [0,100) (constant 'active') should be pruned", result.overlaps(0, 100));
        assertFalse("Page [400,500) (constant 'active') should be pruned", result.overlaps(400, 500));
        assertTrue("Page [500,600) (constant 'idle') should survive", result.overlaps(500, 600));
        assertEquals("Exactly the 'idle' pages survive", 500L, result.selectedRowCount());
    }

    public void testNotEqWithNullValueReturnsAll() {
        PreloadedRowGroupMetadata metadata = buildIntMetadata("id", PAGE_COUNT, PAGE_SIZE);
        FilterPredicate pred = FilterApi.notEq(FilterApi.intColumn("id"), null);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, ROW_GROUP_ROW_COUNT);

        assertTrue("NotEq with null value (IS NOT NULL) should conservatively return all rows", result.isAll());
    }

    public void testAndIntersectsRanges() {
        PreloadedRowGroupMetadata metadata = buildIntMetadata("id", PAGE_COUNT, PAGE_SIZE);
        FilterPredicate pred = FilterApi.and(FilterApi.gtEq(FilterApi.intColumn("id"), 300), FilterApi.lt(FilterApi.intColumn("id"), 600));

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, ROW_GROUP_ROW_COUNT);

        assertTrue("Page [300,400) should be in intersection", result.overlaps(300, 400));
        assertTrue("Page [400,500) should be in intersection", result.overlaps(400, 500));
        assertTrue("Page [500,600) should be in intersection", result.overlaps(500, 600));
        assertFalse("Page [0,100) should not be in intersection", result.overlaps(0, 100));
        assertFalse("Page [700,800) should not be in intersection", result.overlaps(700, 800));
    }

    public void testOrUnionsRanges() {
        PreloadedRowGroupMetadata metadata = buildIntMetadata("id", PAGE_COUNT, PAGE_SIZE);
        FilterPredicate pred = FilterApi.or(FilterApi.eq(FilterApi.intColumn("id"), 50), FilterApi.eq(FilterApi.intColumn("id"), 950));

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, ROW_GROUP_ROW_COUNT);

        assertTrue("Page [0,100) should be included (contains 50)", result.overlaps(0, 100));
        assertTrue("Page [900,1000) should be included (contains 950)", result.overlaps(900, 1000));
        assertFalse("Page [400,500) should be excluded", result.overlaps(400, 500));
    }

    public void testNotReturnsAllConservatively() {
        PreloadedRowGroupMetadata metadata = buildIntMetadata("id", PAGE_COUNT, PAGE_SIZE);
        FilterPredicate pred = FilterApi.not(FilterApi.eq(FilterApi.intColumn("id"), 550));

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, ROW_GROUP_ROW_COUNT);

        assertEquals(
            "NOT must conservatively return all rows (complement would drop mixed pages)",
            ROW_GROUP_ROW_COUNT,
            result.selectedRowCount()
        );
    }

    public void testInIncludesMatchingPages() {
        PreloadedRowGroupMetadata metadata = buildIntMetadata("id", PAGE_COUNT, PAGE_SIZE);
        FilterPredicate pred = FilterApi.in(FilterApi.intColumn("id"), Set.of(150, 750));

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, ROW_GROUP_ROW_COUNT);

        assertTrue("Page [100,200) should be included (contains 150)", result.overlaps(100, 200));
        assertTrue("Page [700,800) should be included (contains 750)", result.overlaps(700, 800));
        assertFalse("Page [400,500) should be excluded", result.overlaps(400, 500));
    }

    public void testMissingColumnIndexReturnsAll() {
        PreloadedRowGroupMetadata metadata = PreloadedRowGroupMetadata.empty();
        FilterPredicate pred = FilterApi.eq(FilterApi.intColumn("missing_col"), 42);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, ROW_GROUP_ROW_COUNT);

        assertTrue("Missing ColumnIndex should return all rows", result.isAll());
    }

    public void testNullPredicateReturnsAll() {
        PreloadedRowGroupMetadata metadata = buildIntMetadata("id", PAGE_COUNT, PAGE_SIZE);

        RowRanges result = ColumnIndexRowRangesComputer.compute(null, metadata, 0, ROW_GROUP_ROW_COUNT);

        assertTrue("Null predicate should return all rows", result.isAll());
    }

    public void testEqWithNullValueReturnsAll() {
        PreloadedRowGroupMetadata metadata = buildIntMetadata("id", PAGE_COUNT, PAGE_SIZE);
        FilterPredicate pred = FilterApi.eq(FilterApi.intColumn("id"), null);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, ROW_GROUP_ROW_COUNT);

        assertTrue("Eq with null value (IS NULL) should return all rows", result.isAll());
    }

    public void testLongColumnEq() {
        PreloadedRowGroupMetadata metadata = buildLongMetadata("ts", PAGE_COUNT, PAGE_SIZE);
        FilterPredicate pred = FilterApi.eq(FilterApi.longColumn("ts"), 550L);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, ROW_GROUP_ROW_COUNT);

        assertTrue("Page containing 550 should be included", result.overlaps(500, 600));
        assertFalse("Page [0,100) should be excluded", result.overlaps(0, 100));
    }

    public void testDoubleColumnGtEq() {
        PreloadedRowGroupMetadata metadata = buildDoubleMetadata("score", PAGE_COUNT, PAGE_SIZE);
        FilterPredicate pred = FilterApi.gtEq(FilterApi.doubleColumn("score"), 800.0);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, ROW_GROUP_ROW_COUNT);

        assertTrue("Page [800,900) should be included", result.overlaps(800, 900));
        assertTrue("Page [900,1000) should be included", result.overlaps(900, 1000));
        assertFalse("Page [0,100) should be excluded", result.overlaps(0, 100));
    }

    public void testBinaryColumnEq() {
        PreloadedRowGroupMetadata metadata = buildBinaryMetadata("name", PAGE_COUNT, PAGE_SIZE);
        FilterPredicate pred = FilterApi.eq(FilterApi.binaryColumn("name"), Binary.fromString("page_5"));

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, ROW_GROUP_ROW_COUNT);

        assertTrue("Page with matching min/max should be included", result.overlaps(500, 600));
    }

    public void testLtKeepsNaNStatsPagesConservatively() {
        // Four pages exercising every NaN-stats shape against `score < 0.5`:
        // page 0 [included] min=-10, max=-1 -> real stats clearly satisfy the predicate.
        // page 1 [excluded] min= 10, max= 15 -> real stats clearly fail the predicate.
        // page 2 [NaN max] min= -5, max=NaN -> the max is not a number; today's lambda
        // only looks at min for Lt, so the page is
        // kept by accident. The fix routes max=NaN
        // through the `max == null` branch and
        // keeps the page conservatively.
        // page 3 [NaN min] min=NaN, max= 5 -> the min is not a number; today's lambda
        // evaluates `Double.compare(NaN, 0.5) < 0`
        // -> false (Java orders NaN above every
        // finite value), so the page is WRONGLY
        // EXCLUDED. The fix maps NaN to null and
        // keeps the page via the conservative
        // `min == null` branch.
        // This assertion on page 3 is what fails today.
        double[] mins = { -10.0, 10.0, -5.0, Double.NaN };
        double[] maxes = { -1.0, 15.0, Double.NaN, 5.0 };
        PreloadedRowGroupMetadata metadata = buildDoubleMetadataExplicit("score", mins, maxes, PAGE_SIZE);
        FilterPredicate pred = FilterApi.lt(FilterApi.doubleColumn("score"), 0.5);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, 4L * PAGE_SIZE);

        assertTrue("Real-stats matching page must be kept", result.overlaps(0, PAGE_SIZE));
        assertFalse("Real-stats failing page must be excluded", result.overlaps(PAGE_SIZE, 2L * PAGE_SIZE));
        assertTrue("NaN-max page must be kept conservatively", result.overlaps(2L * PAGE_SIZE, 3L * PAGE_SIZE));
        assertTrue("NaN-min page must be kept conservatively", result.overlaps(3L * PAGE_SIZE, 4L * PAGE_SIZE));
    }

    public void testEqKeepsNaNStatsPagesConservatively() {
        // Same four-page layout against `score = 50.0`:
        // page 0 [included] min= 40, max= 60 -> 50 in range.
        // page 1 [excluded] min=100, max=200 -> 50 below range.
        // page 2 [NaN max] min= 40, max=NaN -> today's lambda evaluates
        // `min<=50 && 50<=NaN`. The second
        // comparison is `Double.compare(50, NaN)
        // <= 0` -> -1 <= 0 -> true, so the page is
        // kept by accident. The fix keeps it via
        // the `max == null` branch.
        // page 3 [NaN min] min=NaN, max= 60 -> today's lambda evaluates
        // `Double.compare(NaN, 50) <= 0` -> false,
        // so the page is WRONGLY EXCLUDED. The
        // fix keeps it via the `min == null` branch.
        double[] mins = { 40.0, 100.0, 40.0, Double.NaN };
        double[] maxes = { 60.0, 200.0, Double.NaN, 60.0 };
        PreloadedRowGroupMetadata metadata = buildDoubleMetadataExplicit("score", mins, maxes, PAGE_SIZE);
        FilterPredicate pred = FilterApi.eq(FilterApi.doubleColumn("score"), 50.0);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, 4L * PAGE_SIZE);

        assertTrue("Real-stats matching page must be kept", result.overlaps(0, PAGE_SIZE));
        assertFalse("Real-stats failing page must be excluded", result.overlaps(PAGE_SIZE, 2L * PAGE_SIZE));
        assertTrue("NaN-max page must be kept conservatively", result.overlaps(2L * PAGE_SIZE, 3L * PAGE_SIZE));
        assertTrue("NaN-min page must be kept conservatively", result.overlaps(3L * PAGE_SIZE, 4L * PAGE_SIZE));
    }

    public void testGtKeepsNaNStatsPagesConservatively() {
        // Same four-page layout against `score > 100.0`. For Gt the leaf lambda only inspects
        // max, so every NaN-stats shape happens to be kept today by accident:
        // page 2 [NaN max] `Double.compare(NaN, 100) > 0` -> +1 -> kept.
        // page 3 [NaN min] max ignored; real max > 100 -> kept.
        // This test therefore passes today and after the fix; it serves as a no-regression
        // guard that maps the same NaN handling to the conservative path explicitly via
        // null returns from decodeValue.
        double[] mins = { 50.0, 10.0, -5.0, Double.NaN };
        double[] maxes = { 150.0, 15.0, Double.NaN, 200.0 };
        PreloadedRowGroupMetadata metadata = buildDoubleMetadataExplicit("score", mins, maxes, PAGE_SIZE);
        FilterPredicate pred = FilterApi.gt(FilterApi.doubleColumn("score"), 100.0);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, 4L * PAGE_SIZE);

        assertTrue("Real-stats matching page must be kept", result.overlaps(0, PAGE_SIZE));
        assertFalse("Real-stats failing page must be excluded", result.overlaps(PAGE_SIZE, 2L * PAGE_SIZE));
        assertTrue("NaN-max page must be kept conservatively", result.overlaps(2L * PAGE_SIZE, 3L * PAGE_SIZE));
        assertTrue("NaN-min page must be kept conservatively", result.overlaps(3L * PAGE_SIZE, 4L * PAGE_SIZE));
    }

    public void testLtKeepsAllNaNStatsPageConservatively() {
        // Three pages exercising the all-NaN shape (e.g. a writer that emits NaN min/max as
        // a "stats unavailable" signal, or a page whose values are entirely NaN):
        // page 0 [included] min=-10, max=-1 -> real stats satisfy `score < 0.5`.
        // page 1 [excluded] min= 10, max= 15 -> real stats fail.
        // page 2 [all-NaN] min=NaN, max=NaN -> today's lambda evaluates
        // `NaN < 0.5` -> false and the page is
        // WRONGLY EXCLUDED. The fix keeps it.
        double[] mins = { -10.0, 10.0, Double.NaN };
        double[] maxes = { -1.0, 15.0, Double.NaN };
        PreloadedRowGroupMetadata metadata = buildDoubleMetadataExplicit("score", mins, maxes, PAGE_SIZE);
        FilterPredicate pred = FilterApi.lt(FilterApi.doubleColumn("score"), 0.5);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, 3L * PAGE_SIZE);

        assertTrue("Real-stats matching page must be kept", result.overlaps(0, PAGE_SIZE));
        assertFalse("Real-stats failing page must be excluded", result.overlaps(PAGE_SIZE, 2L * PAGE_SIZE));
        assertTrue("All-NaN page must be kept conservatively", result.overlaps(2L * PAGE_SIZE, 3L * PAGE_SIZE));
    }

    // -----------------------------------------------------------------------------------
    // Schema-driven decode: FLOAT vs DOUBLE disambiguation and conservative reject for
    // non-native DOUBLE backings (DECIMAL, Float16). Today's decodeValue uses
    // `ordered.remaining() == Float.BYTES` to pick the reader, which is correct for
    // well-formed FLOAT/DOUBLE files but misreads any column whose physical encoding is
    // not 4-byte float or 8-byte double. The schema-driven path replaces the
    // heuristic with a lookup against the file MessageType captured on PreloadedRowGroupMetadata.
    // -----------------------------------------------------------------------------------

    public void testFloatColumnDecodedViaFloatBuffers() {
        // No-regression guard: a real FLOAT column with 4-byte stats and a `Lt(0.5)` predicate
        // whose min/max clearly satisfy the predicate. The page must be kept both under today's
        // buffer-length heuristic and under the schema-driven decode in Phase B.
        PrimitiveType ptype = Types.required(PrimitiveType.PrimitiveTypeName.FLOAT).named("score");
        ByteBuffer min = ByteBuffer.allocate(Float.BYTES).order(ByteOrder.LITTLE_ENDIAN).putFloat(-10.0f).flip();
        ByteBuffer max = ByteBuffer.allocate(Float.BYTES).order(ByteOrder.LITTLE_ENDIAN).putFloat(-1.0f).flip();
        PreloadedRowGroupMetadata metadata = buildSinglePageMetadata("score", ptype, min, max, PAGE_SIZE);
        FilterPredicate pred = FilterApi.lt(FilterApi.doubleColumn("score"), 0.5);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, PAGE_SIZE);

        assertTrue("FLOAT-encoded page satisfying Lt(0.5) must be included", result.overlaps(0, PAGE_SIZE));
    }

    public void testDoubleColumnDecodedViaDoubleBuffers() {
        // No-regression guard: a real DOUBLE column with 8-byte stats and a `Lt(0.5)` predicate
        // whose min/max clearly fail the predicate. The page must be excluded both under today's
        // buffer-length heuristic and under the schema-driven decode in Phase B.
        PrimitiveType ptype = Types.required(PrimitiveType.PrimitiveTypeName.DOUBLE).named("score");
        PreloadedRowGroupMetadata metadata = buildSinglePageMetadata("score", ptype, doubleToBuffer(10.0), doubleToBuffer(15.0), PAGE_SIZE);
        FilterPredicate pred = FilterApi.lt(FilterApi.doubleColumn("score"), 0.5);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, PAGE_SIZE);

        assertFalse("DOUBLE-encoded page failing Lt(0.5) must be excluded", result.overlaps(0, PAGE_SIZE));
    }

    public void testDecimalEncodedDoubleColumnIsKeptConservatively() {
        // INT32-encoded DECIMAL column with scale=2 (cents). Stats represent $10.00 (min=1000)
        // and $5000.00 (max=500000). The predicate `Gt(price, 100.0)` should keep the page —
        // RECHECK in FilterExec evaluates the actual decoded values per row.
        //
        // Today's decodeValue Double branch matches `ordered.remaining() == Float.BYTES` (4 for
        // INT32) and reinterprets the int bits as a float via getFloat():
        // Float.intBitsToFloat(500000) is approximately 7.0e-40, a tiny denormal.
        // The leaf lambda for Gt then evaluates `max > 100.0` -> 7.0e-40 > 100 -> false, and
        // the page is WRONGLY EXCLUDED. Phase B detects the DECIMAL logical-type via the
        // schema and returns null from decodeValue, routing the page through the conservative
        // null branch. Fails today, passes after Phase B.
        PrimitiveType ptype = Types.required(PrimitiveType.PrimitiveTypeName.INT32)
            .as(LogicalTypeAnnotation.decimalType(2, 9))
            .named("price");
        PreloadedRowGroupMetadata metadata = buildSinglePageMetadata("price", ptype, intToBuffer(1000), intToBuffer(500000), PAGE_SIZE);
        FilterPredicate pred = FilterApi.gt(FilterApi.doubleColumn("price"), 100.0);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, PAGE_SIZE);

        assertTrue("DECIMAL-encoded column must be kept conservatively for doubleColumn predicates", result.overlaps(0, PAGE_SIZE));
    }

    public void testFloat16EncodedDoubleColumnIsKeptConservatively() {
        // Float16 column: FIXED_LEN_BYTE_ARRAY(2) + Float16 logical type.
        // Stats represent 1.0 (min=0x3C00) and 2.0 (max=0x4000).
        //
        // Today's decodeValue Double branch sees a 2-byte buffer: `ordered.remaining() == Float.BYTES`
        // (2 == 4) is false, so it falls through to ordered.getDouble(), which over-reads the
        // 2-byte buffer and throws BufferUnderflowException. The test therefore fails today with
        // an exception. Phase B detects the Float16 logical type and returns null from
        // decodeValue, routing the page through the conservative null branch. Passes after Phase B.
        PrimitiveType ptype = Types.required(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
            .length(2)
            .as(LogicalTypeAnnotation.float16Type())
            .named("ratio");
        ByteBuffer min = ByteBuffer.wrap(new byte[] { 0x00, 0x3C }); // 1.0
        ByteBuffer max = ByteBuffer.wrap(new byte[] { 0x00, 0x40 }); // 2.0
        PreloadedRowGroupMetadata metadata = buildSinglePageMetadata("ratio", ptype, min, max, PAGE_SIZE);
        FilterPredicate pred = FilterApi.lt(FilterApi.doubleColumn("ratio"), 0.5);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, PAGE_SIZE);

        assertTrue("Float16-encoded column must be kept conservatively for doubleColumn predicates", result.overlaps(0, PAGE_SIZE));
    }

    public void testBooleanColumnEq() {
        PreloadedRowGroupMetadata metadata = buildBooleanMetadata("flag", PAGE_COUNT, PAGE_SIZE);
        FilterPredicate pred = FilterApi.eq(FilterApi.booleanColumn("flag"), true);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, ROW_GROUP_ROW_COUNT);

        assertTrue("At least some pages should match true", result.selectedRowCount() > 0);
    }

    // --- Metadata builders ---

    private static PreloadedRowGroupMetadata buildIntMetadata(String columnPath, int pageCount, int pageSize) {
        List<ByteBuffer> minValues = new ArrayList<>();
        List<ByteBuffer> maxValues = new ArrayList<>();
        List<Boolean> nullPages = new ArrayList<>();
        List<PageLocation> pageLocations = new ArrayList<>();

        for (int p = 0; p < pageCount; p++) {
            int min = p * pageSize;
            int max = (p + 1) * pageSize - 1;
            minValues.add(intToBuffer(min));
            maxValues.add(intToBuffer(max));
            nullPages.add(false);
            pageLocations.add(new PageLocation(p * 1000L, pageSize * 4, p * pageSize));
        }

        ColumnIndex ci = new ColumnIndex(nullPages, minValues, maxValues, BoundaryOrder.ASCENDING);
        OffsetIndex oi = new OffsetIndex(pageLocations);

        PrimitiveType ptype = Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(columnPath);
        org.apache.parquet.internal.column.columnindex.ColumnIndex typedCi = ParquetMetadataConverter.fromParquetColumnIndex(ptype, ci);
        org.apache.parquet.internal.column.columnindex.OffsetIndex typedOi = ParquetMetadataConverter.fromParquetOffsetIndex(oi);

        return buildMetadata(columnPath, ptype, typedCi, typedOi);
    }

    /**
     * Builds metadata where page p has constant value pageValues[p] (i.e. min == max == pageValues[p]).
     * Useful for exercising NotEq pruning on low-cardinality / sorted-clustered data. Boundary order
     * is reported as UNORDERED because the per-page values are caller-supplied and may not be sorted.
     */
    private static PreloadedRowGroupMetadata buildConstantIntMetadata(String columnPath, int[] pageValues, int pageSize) {
        List<ByteBuffer> minValues = new ArrayList<>();
        List<ByteBuffer> maxValues = new ArrayList<>();
        List<Boolean> nullPages = new ArrayList<>();
        List<PageLocation> pageLocations = new ArrayList<>();

        for (int p = 0; p < pageValues.length; p++) {
            minValues.add(intToBuffer(pageValues[p]));
            maxValues.add(intToBuffer(pageValues[p]));
            nullPages.add(false);
            pageLocations.add(new PageLocation(p * 1000L, pageSize * 4, p * pageSize));
        }

        ColumnIndex ci = new ColumnIndex(nullPages, minValues, maxValues, BoundaryOrder.UNORDERED);
        OffsetIndex oi = new OffsetIndex(pageLocations);

        PrimitiveType ptype = Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(columnPath);
        org.apache.parquet.internal.column.columnindex.ColumnIndex typedCi = ParquetMetadataConverter.fromParquetColumnIndex(ptype, ci);
        org.apache.parquet.internal.column.columnindex.OffsetIndex typedOi = ParquetMetadataConverter.fromParquetOffsetIndex(oi);

        return buildMetadata(columnPath, ptype, typedCi, typedOi);
    }

    /**
     * Builds metadata where page p has constant binary value pageValues[p] (i.e. min == max == pageValues[p]).
     */
    private static PreloadedRowGroupMetadata buildConstantBinaryMetadata(String columnPath, String[] pageValues, int pageSize) {
        List<ByteBuffer> minValues = new ArrayList<>();
        List<ByteBuffer> maxValues = new ArrayList<>();
        List<Boolean> nullPages = new ArrayList<>();
        List<PageLocation> pageLocations = new ArrayList<>();

        for (int p = 0; p < pageValues.length; p++) {
            Binary value = Binary.fromString(pageValues[p]);
            minValues.add(ByteBuffer.wrap(value.getBytes()));
            maxValues.add(ByteBuffer.wrap(value.getBytes()));
            nullPages.add(false);
            pageLocations.add(new PageLocation(p * 1000L, pageSize * 10, p * pageSize));
        }

        ColumnIndex ci = new ColumnIndex(nullPages, minValues, maxValues, BoundaryOrder.UNORDERED);
        OffsetIndex oi = new OffsetIndex(pageLocations);

        PrimitiveType ptype = Types.required(PrimitiveType.PrimitiveTypeName.BINARY).named(columnPath);
        org.apache.parquet.internal.column.columnindex.ColumnIndex typedCi = ParquetMetadataConverter.fromParquetColumnIndex(ptype, ci);
        org.apache.parquet.internal.column.columnindex.OffsetIndex typedOi = ParquetMetadataConverter.fromParquetOffsetIndex(oi);

        return buildMetadata(columnPath, ptype, typedCi, typedOi);
    }

    private static PreloadedRowGroupMetadata buildLongMetadata(String columnPath, int pageCount, int pageSize) {
        List<ByteBuffer> minValues = new ArrayList<>();
        List<ByteBuffer> maxValues = new ArrayList<>();
        List<Boolean> nullPages = new ArrayList<>();
        List<PageLocation> pageLocations = new ArrayList<>();

        for (int p = 0; p < pageCount; p++) {
            long min = (long) p * pageSize;
            long max = (long) (p + 1) * pageSize - 1;
            minValues.add(longToBuffer(min));
            maxValues.add(longToBuffer(max));
            nullPages.add(false);
            pageLocations.add(new PageLocation(p * 1000L, pageSize * 8, p * pageSize));
        }

        ColumnIndex ci = new ColumnIndex(nullPages, minValues, maxValues, BoundaryOrder.ASCENDING);
        OffsetIndex oi = new OffsetIndex(pageLocations);

        PrimitiveType ptype = Types.required(PrimitiveType.PrimitiveTypeName.INT64).named(columnPath);
        org.apache.parquet.internal.column.columnindex.ColumnIndex typedCi = ParquetMetadataConverter.fromParquetColumnIndex(ptype, ci);
        org.apache.parquet.internal.column.columnindex.OffsetIndex typedOi = ParquetMetadataConverter.fromParquetOffsetIndex(oi);

        return buildMetadata(columnPath, ptype, typedCi, typedOi);
    }

    private static PreloadedRowGroupMetadata buildDoubleMetadata(String columnPath, int pageCount, int pageSize) {
        List<ByteBuffer> minValues = new ArrayList<>();
        List<ByteBuffer> maxValues = new ArrayList<>();
        List<Boolean> nullPages = new ArrayList<>();
        List<PageLocation> pageLocations = new ArrayList<>();

        for (int p = 0; p < pageCount; p++) {
            double min = (double) p * pageSize;
            double max = (double) (p + 1) * pageSize - 1;
            minValues.add(doubleToBuffer(min));
            maxValues.add(doubleToBuffer(max));
            nullPages.add(false);
            pageLocations.add(new PageLocation(p * 1000L, pageSize * 8, p * pageSize));
        }

        ColumnIndex ci = new ColumnIndex(nullPages, minValues, maxValues, BoundaryOrder.ASCENDING);
        OffsetIndex oi = new OffsetIndex(pageLocations);

        PrimitiveType ptype = Types.required(PrimitiveType.PrimitiveTypeName.DOUBLE).named(columnPath);
        org.apache.parquet.internal.column.columnindex.ColumnIndex typedCi = ParquetMetadataConverter.fromParquetColumnIndex(ptype, ci);
        org.apache.parquet.internal.column.columnindex.OffsetIndex typedOi = ParquetMetadataConverter.fromParquetOffsetIndex(oi);

        return buildMetadata(columnPath, ptype, typedCi, typedOi);
    }

    /**
     * Builds a DOUBLE column index where page {@code p} has explicit {@code mins[p]} / {@code maxes[p]}
     * stats. Used to exercise edge cases (NaN, equal min==max, etc.) that the contiguous-range
     * helper {@link #buildDoubleMetadata} cannot express.
     */
    private static PreloadedRowGroupMetadata buildDoubleMetadataExplicit(String columnPath, double[] mins, double[] maxes, int pageSize) {
        assert mins.length == maxes.length : "mins/maxes length mismatch";
        List<ByteBuffer> minValues = new ArrayList<>();
        List<ByteBuffer> maxValues = new ArrayList<>();
        List<Boolean> nullPages = new ArrayList<>();
        List<PageLocation> pageLocations = new ArrayList<>();

        for (int p = 0; p < mins.length; p++) {
            minValues.add(doubleToBuffer(mins[p]));
            maxValues.add(doubleToBuffer(maxes[p]));
            nullPages.add(false);
            pageLocations.add(new PageLocation(p * 1000L, pageSize * 8, p * pageSize));
        }

        ColumnIndex ci = new ColumnIndex(nullPages, minValues, maxValues, BoundaryOrder.UNORDERED);
        OffsetIndex oi = new OffsetIndex(pageLocations);

        PrimitiveType ptype = Types.required(PrimitiveType.PrimitiveTypeName.DOUBLE).named(columnPath);
        var typedCi = ParquetMetadataConverter.fromParquetColumnIndex(ptype, ci);
        var typedOi = ParquetMetadataConverter.fromParquetOffsetIndex(oi);

        return buildMetadata(columnPath, ptype, typedCi, typedOi);
    }

    /**
     * Single-page metadata helper for the schema-driven decode tests. The caller supplies both
     * the {@link PrimitiveType} (driving the file MessageType captured on
     * {@link PreloadedRowGroupMetadata}) and the raw min/max stats bytes — necessary because
     * DECIMAL and Float16 columns intentionally produce buffer lengths (4 and 2 bytes) that the
     * legacy {@code remaining() == Float.BYTES} heuristic mishandles, and we want to drive the
     * exact buffer the runtime would observe rather than re-encode via a Java primitive.
     */
    private static PreloadedRowGroupMetadata buildSinglePageMetadata(
        String columnPath,
        PrimitiveType primitiveType,
        ByteBuffer min,
        ByteBuffer max,
        int pageSize
    ) {
        List<ByteBuffer> minValues = List.of(min);
        List<ByteBuffer> maxValues = List.of(max);
        List<Boolean> nullPages = List.of(Boolean.FALSE);
        List<PageLocation> pageLocations = List.of(new PageLocation(0L, pageSize * 4, 0));

        ColumnIndex ci = new ColumnIndex(nullPages, minValues, maxValues, BoundaryOrder.UNORDERED);
        OffsetIndex oi = new OffsetIndex(pageLocations);

        var typedCi = ParquetMetadataConverter.fromParquetColumnIndex(primitiveType, ci);
        var typedOi = ParquetMetadataConverter.fromParquetOffsetIndex(oi);

        return buildMetadata(columnPath, primitiveType, typedCi, typedOi);
    }

    private static PreloadedRowGroupMetadata buildBinaryMetadata(String columnPath, int pageCount, int pageSize) {
        List<ByteBuffer> minValues = new ArrayList<>();
        List<ByteBuffer> maxValues = new ArrayList<>();
        List<Boolean> nullPages = new ArrayList<>();
        List<PageLocation> pageLocations = new ArrayList<>();

        for (int p = 0; p < pageCount; p++) {
            Binary min = Binary.fromString("page_" + p);
            Binary max = Binary.fromString("page_" + p + "_z");
            minValues.add(ByteBuffer.wrap(min.getBytes()));
            maxValues.add(ByteBuffer.wrap(max.getBytes()));
            nullPages.add(false);
            pageLocations.add(new PageLocation(p * 1000L, pageSize * 10, p * pageSize));
        }

        ColumnIndex ci = new ColumnIndex(nullPages, minValues, maxValues, BoundaryOrder.ASCENDING);
        OffsetIndex oi = new OffsetIndex(pageLocations);

        PrimitiveType ptype = Types.required(PrimitiveType.PrimitiveTypeName.BINARY).named(columnPath);
        org.apache.parquet.internal.column.columnindex.ColumnIndex typedCi = ParquetMetadataConverter.fromParquetColumnIndex(ptype, ci);
        org.apache.parquet.internal.column.columnindex.OffsetIndex typedOi = ParquetMetadataConverter.fromParquetOffsetIndex(oi);

        return buildMetadata(columnPath, ptype, typedCi, typedOi);
    }

    private static PreloadedRowGroupMetadata buildBooleanMetadata(String columnPath, int pageCount, int pageSize) {
        List<ByteBuffer> minValues = new ArrayList<>();
        List<ByteBuffer> maxValues = new ArrayList<>();
        List<Boolean> nullPages = new ArrayList<>();
        List<PageLocation> pageLocations = new ArrayList<>();

        for (int p = 0; p < pageCount; p++) {
            boolean min = p % 2 == 0;
            boolean max = true;
            minValues.add(ByteBuffer.wrap(new byte[] { (byte) (min ? 1 : 0) }));
            maxValues.add(ByteBuffer.wrap(new byte[] { (byte) (max ? 1 : 0) }));
            nullPages.add(false);
            pageLocations.add(new PageLocation(p * 1000L, pageSize, p * pageSize));
        }

        ColumnIndex ci = new ColumnIndex(nullPages, minValues, maxValues, BoundaryOrder.UNORDERED);
        OffsetIndex oi = new OffsetIndex(pageLocations);

        PrimitiveType ptype = Types.required(PrimitiveType.PrimitiveTypeName.BOOLEAN).named(columnPath);
        org.apache.parquet.internal.column.columnindex.ColumnIndex typedCi = ParquetMetadataConverter.fromParquetColumnIndex(ptype, ci);
        org.apache.parquet.internal.column.columnindex.OffsetIndex typedOi = ParquetMetadataConverter.fromParquetOffsetIndex(oi);

        return buildMetadata(columnPath, ptype, typedCi, typedOi);
    }

    private static PreloadedRowGroupMetadata buildMetadata(
        String columnPath,
        PrimitiveType primitiveType,
        org.apache.parquet.internal.column.columnindex.ColumnIndex ci,
        org.apache.parquet.internal.column.columnindex.OffsetIndex oi
    ) {
        Map<String, org.apache.parquet.internal.column.columnindex.ColumnIndex> columnIndexes = new HashMap<>();
        Map<String, org.apache.parquet.internal.column.columnindex.OffsetIndex> offsetIndexes = new HashMap<>();
        columnIndexes.put("0:" + columnPath, ci);
        offsetIndexes.put("0:" + columnPath, oi);
        MessageType schema = new MessageType("test", primitiveType);
        return new PreloadedRowGroupMetadata(columnIndexes, offsetIndexes, schema);
    }

    private static ByteBuffer intToBuffer(int value) {
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value).flip();
    }

    private static ByteBuffer longToBuffer(long value) {
        return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(value).flip();
    }

    private static ByteBuffer doubleToBuffer(double value) {
        return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putDouble(value).flip();
    }
}
