package com.baesystems;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.baesystems.IntraClusterReindexer;
import com.baesystems.test.util.ElasticSearchNode;

public class ReindexerTest {

    private static final String DATE_FORMAT = "yyyyMMdd";
    private static final String TIMESTAMP_FIELD = "_timestamp";
    private static final String PARENT_FIELD = "_parent";
    private static final String TIME_TYPE = "time";
    private static final String CHILDREN = "child";
    private static final int BATCH_SIZE = 2;
    private static final String DEFAULT_DATE = "20150725";
    private static final String TIMESTAMP = "timestamp";
    private static final String TYPE = "type1";
    private static final String NEW_INDEX = "testnew";
    private static final String INDEX = "test";
    private static ElasticSearchNode es;

    private Client client;
    private IntraClusterReindexer reindexer;

    @BeforeClass
    public static void beforeClass() throws Exception {
        es = new ElasticSearchNode();
        es.startNode();
    }

    @Before
    public void before() throws Exception {
        this.client = es.getClient();

        es.createIndex(INDEX);
        es.createIndex(NEW_INDEX);

        es.putMapping(INDEX, CHILDREN, getChildrenMapping());
        es.putMapping(NEW_INDEX, CHILDREN, getChildrenMapping());

        es.putMapping(INDEX, TIME_TYPE, getTimestampMapping());
        es.putMapping(NEW_INDEX, TIME_TYPE, getTimestampMapping());

        reindexer = new IntraClusterReindexer(INDEX, TYPE, TIMESTAMP, BATCH_SIZE, client, NEW_INDEX);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        es.shutdownAndClean();
    }

    @After
    public void after() throws Exception {
        es.deleteIndex(INDEX);
        es.deleteIndex(NEW_INDEX);
    }

    @Test
    public void documentWithinOneDayIsReindexedCorrecly() throws Exception {

        DateTime date = parseStringToDate(DEFAULT_DATE);
        Map<String, Object> document = generateDocument(date.withHourOfDay(5));

        String id = es.indexDocument(INDEX, TYPE, document);

        this.reindexer.reindex(date, date.plusDays(1));

        Map<String, Object> source = es.getDocumentById(NEW_INDEX, TYPE, id);

        checkDocument(document, source);

    }

    @Test
    public void documentAreReindexedInclusivelyToFromDate() throws Exception {

        DateTime date = parseStringToDate(DEFAULT_DATE);
        Map<String, Object> document = generateDocument(date.withTime(0, 0, 0, 0));

        String id = es.indexDocument(INDEX, TYPE, document);

        this.reindexer.reindex(date, date.plusDays(1));

        Map<String, Object> source = es.getDocumentById(NEW_INDEX, TYPE, id);

        checkDocument(document, source);

    }

    @Test
    public void documentAreReindexedExclusivlyToToDate() throws Exception {

        DateTime date = parseStringToDate(DEFAULT_DATE);
        Map<String, Object> document = generateDocument(date);

        String id = es.indexDocument(INDEX, TYPE, document);

        this.reindexer.reindex(date.minusDays(1), date);

        Map<String, Object> source = es.getDocumentById(NEW_INDEX, TYPE, id);

        assertThat(source, equalTo(null));

    }

    @Test
    public void documentAreReindexedInBatches() throws Exception {
        DateTime date = parseStringToDate(DEFAULT_DATE);
        Map<String, Object> document = generateDocument(date.withHourOfDay(5));

        String id1 = es.indexDocument(INDEX, TYPE, document);
        String id2 = es.indexDocument(INDEX, TYPE, document);
        String id3 = es.indexDocument(INDEX, TYPE, document);
        String id4 = es.indexDocument(INDEX, TYPE, document);

        this.reindexer.reindex(date, date.plusDays(1));

        Map<String, Object> source = es.getDocumentById(NEW_INDEX, TYPE, id1);
        checkDocument(document, source);

        source = es.getDocumentById(NEW_INDEX, TYPE, id2);
        checkDocument(document, source);

        source = es.getDocumentById(NEW_INDEX, TYPE, id3);
        checkDocument(document, source);

        source = es.getDocumentById(NEW_INDEX, TYPE, id4);
        checkDocument(document, source);
    }

    @Test
    public void documentContainsParentField() throws Exception {
        DateTime date = parseStringToDate(DEFAULT_DATE);
        Map<String, Object> document = generateDocument(date.withHourOfDay(5));

        String parent = es.indexDocument(INDEX, TYPE, document);
        String child = es.indexDocument(INDEX, CHILDREN, parent, document);

        this.reindexer = new IntraClusterReindexer(INDEX, CHILDREN, TIMESTAMP, BATCH_SIZE, client, NEW_INDEX);
        this.reindexer.reindex(date, date.plusDays(1));

        Map<String, Object> source = es.getDocumentById(NEW_INDEX, CHILDREN, child, parent);

        checkDocument(document, source);

        assertThat(es.getDocumentParent(NEW_INDEX, CHILDREN, child, parent).toString(), equalTo(parent));

    }

    @Test
    public void documentContainsTimestampField() throws Exception {

        DateTime date = parseStringToDate(DEFAULT_DATE);
        Map<String, Object> document = generateDocument(date.withHourOfDay(5));
        document.remove("timestamp");

        String id = es.indexDocument(INDEX, TIME_TYPE, document);

        this.reindexer = new IntraClusterReindexer(INDEX, TIME_TYPE, TIMESTAMP_FIELD, BATCH_SIZE, client, NEW_INDEX);

        DateTime currentTime = new DateTime();
        this.reindexer.reindex(currentTime.minusDays(1), currentTime.plusDays(1));
        Map<String, Object> source = es.getDocumentById(NEW_INDEX, TIME_TYPE, id);

        checkDocument(document, source);

        assertThat(es.getDocumentTimestamp(NEW_INDEX, TIME_TYPE, id),
                equalTo(es.getDocumentTimestamp(INDEX, TIME_TYPE, id)));
    }

    private DateTime parseStringToDate(final String dateString) {
        DateTimeFormatter fmt = DateTimeFormat.forPattern(DATE_FORMAT);
        DateTime date = fmt.parseDateTime(dateString);
        return date;
    }

    private Map<String, Object> generateDocument(final DateTime date) {
        Map<String, Object> document = new HashMap<String, Object>();
        document.put("filename", "test-file.pdf");
        document.put("name", "test-name");
        document.put(TIMESTAMP, date);
        return document;
    }

    private void checkDocument(Map<String, Object> expected, Map<String, Object> actual) {
        for (Entry<String, Object> entry : expected.entrySet()) {

            if (entry.getValue() instanceof DateTime) {
                String expectedDate = entry.getValue().toString();
                String acctualDate = (String) actual.get(entry.getKey());

                assertThat(acctualDate, equalTo(expectedDate));

            } else {
                assertThat(actual, hasEntry(entry.getKey(), entry.getValue()));
            }

        }
    }

    private XContentBuilder getChildrenMapping() throws IOException {
        XContentBuilder builder = jsonBuilder().startObject().startObject(CHILDREN).startObject(PARENT_FIELD)
                .field("type", TYPE).endObject().endObject().endObject();

        return builder;
    }

    private XContentBuilder getTimestampMapping() throws IOException {
        XContentBuilder builder = jsonBuilder().startObject().startObject(TIME_TYPE).startObject(TIMESTAMP_FIELD)
                .field("enabled", true).field("store", true).endObject().endObject().endObject();

        return builder;
    }
}
