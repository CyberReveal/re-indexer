/*
 * Software Copyright BAE Systems plc 2015. All Rights Reserved.
 * BAE SYSTEMS, DETICA and CYBERREVEAL are trademarks of BAE Systems
 * plc and may be registered in certain jurisdictions.
 */
package com.baesystems;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.internal.InternalSearchHitField;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Class Reindexer.
 */
public class Reindexer {

    private static final String TIMESTAMP_FIELD = "_timestamp";

    private static final String SOURCE_FIELD = "_source";

    private static final String PARENT_FIELD = "_parent";

    private static final int TIMEOUT = 60000;

    private static final Logger LOG = LoggerFactory.getLogger(Reindexer.class);

    private final Client client;
    private final String index;
    private final String type;
    private final String field;
    private final int batchSize;

    /**
     * Instantiates a new reindexer.
     *
     * @param index Elasticsearch source index
     * @param type document type
     * @param field the field which is used for re-index
     * @param batchSize number of document pulled from ES in batch
     * @param client Elasticsearch client
     */
    public Reindexer(final String index, final String type, final String field, final int batchSize, final Client client) {
        this.client = client;
        this.index = index;
        this.type = type;
        this.field = field;
        this.batchSize = batchSize;
    }

    /**
     * Returns number of documents in given time. NOTE 'to' date is exclusive.
     *
     * @param from the from
     * @param to the to
     * @return the document number in period
     */
    public long getDocumentNumberInPeriod(final DateTime from, final DateTime to) {

        RangeQueryBuilder range = QueryBuilders.rangeQuery(this.field).gte(from).lt(to);

        CountResponse response = this.client.prepareCount(this.index).setTypes(this.type).setQuery(range).get();

        return response.getCount();
    }

    /**
     * Copying data in batches from old index to new index.
     *
     * @param from Starting date
     * @param to End date
     * @param newIndexName destination Elasticsearch index
     */
    public void reindex(final DateTime from, final DateTime to, final String newIndexName) {

        LOG.info("Start re-indexing for data between {} and {}", from, to);

        RangeQueryBuilder range = QueryBuilders.rangeQuery(this.field).gte(from).lt(to);

        SearchResponse response = this.client.prepareSearch(this.index).setTypes(this.type)
                .setSearchType(SearchType.SCAN).setScroll(new TimeValue(TIMEOUT))
                .addFields(PARENT_FIELD, SOURCE_FIELD, TIMESTAMP_FIELD).setQuery(range).setSize(this.batchSize).get();

        int number = 0;

        while (true) {
            BulkRequestBuilder bulkRequest = this.client.prepareBulk();

            for (SearchHit hit : response.getHits()) {
                LOG.debug("Document retrieved {}", hit.getId());

                IndexRequestBuilder indexBuilder = this.client.prepareIndex().setIndex(newIndexName).setType(this.type)
                        .setId(hit.getId()).setSource(hit.getSource());

                addFieldIfExist(hit, indexBuilder, PARENT_FIELD);
                addFieldIfExist(hit, indexBuilder, TIMESTAMP_FIELD);

                bulkRequest.add(indexBuilder);
                number++;

            }

            LOG.info("This batch inserted {} documents.", number);

            if (response.getHits().getHits().length > 0) {
                BulkResponse bulkResponse = bulkRequest.get();

                if (bulkResponse.hasFailures()) {
                    LOG.error(bulkResponse.buildFailureMessage());
                    LOG.error("Problem with inserting data.");
                }
            }

            response = this.client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(TIMEOUT)).get();

            if (response.getHits().getHits().length == 0) {
                break;
            }
        }

        LOG.info("Inserted {} documents", number);

    }

    /**
     * Adds optional data field to document.
     *
     * @param hit the hit
     * @param indexBuilder the index builder
     * @param fieldName the field name
     */
    private void addFieldIfExist(final SearchHit hit, final IndexRequestBuilder indexBuilder, final String fieldName) {
        Object object = hit.getFields().get(fieldName);
        if (object != null && object instanceof InternalSearchHitField) {

            InternalSearchHitField field = (InternalSearchHitField) object;

            LOG.debug("Added {} '{}' to document {}", fieldName, field.getValue(), hit.getId());
            if (TIMESTAMP_FIELD.equals(fieldName)) {
                indexBuilder.setTimestamp(field.getValue().toString());
            } else if (PARENT_FIELD.equals(fieldName)) {
                indexBuilder.setParent(field.getValue().toString());
            }
        }
    }
}
