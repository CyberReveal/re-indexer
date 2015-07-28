/*
 * Software Copyright BAE Systems plc 2015. All Rights Reserved.
 * BAE SYSTEMS, DETICA and CYBERREVEAL are trademarks of BAE Systems
 * plc and may be registered in certain jurisdictions.
 */
package com.baesystems.test.util;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for testing. This class starts Elasticsearch node.
 */
public class ElasticSearchNode {

    private static final String PARENT_FIELD = "_parent";
    private static final String TIMESTAMP_FIELD = "_timestamp";
    private static final int WAITING_FOR_ES_TIME = 2000;
    private final static Logger LOG = LoggerFactory.getLogger(ElasticSearchNode.class);
    private final static String DEFAULT_NAME = "es-data";
    private final Path path;

    private final Client client;
    private final Node node;

    /**
     * Instantiates a new elastic search node.
     *
     * @throws InterruptedException the interrupted exception
     */
    public ElasticSearchNode() throws InterruptedException {
        LOG.debug(String.format("Created class %s.", getClass().getName()));

        try {
            this.path = Files.createTempDirectory(DEFAULT_NAME);
            LOG.debug(String.format("Path: {}", this.path.toString()));
        } catch (IOException e) {
            throw new RuntimeException("Cannot create data directory.", e);
        }

        ImmutableSettings.Builder elasticsearchSettings = ImmutableSettings.settingsBuilder()
                .put("http.enabled", "false").put("path.data", this.path.toString())
                .put("index.routing.allocation.disable_allocation", "false");

        this.node = nodeBuilder().local(true).settings(elasticsearchSettings).node();

        Thread.sleep(WAITING_FOR_ES_TIME);

        this.client = this.node.client();

    }

    /**
     * Creates new elasticsearch index.
     *
     * @param index new index
     * @return true, if successful
     * @throws Exception the exception
     */
    public boolean createIndex(final String index) throws Exception {
        Settings indexSettings = ImmutableSettings.settingsBuilder().put("number_of_shards", 1)
                .put("number_of_replicas", 0).build();

        CreateIndexRequestBuilder builder = this.client.admin().indices().prepareCreate(index)
                .setSettings(indexSettings);

        CreateIndexResponse response = builder.get();

        return response.isAcknowledged();
    }

    /**
     * Update elasticsearch index mapping
     *
     * @param index the index
     * @param type the type
     * @param mapping the mapping
     * @return true, if successful
     */
    public boolean putMapping(final String index, final String type, final XContentBuilder mapping) {
        PutMappingRequestBuilder mappingBuilder = this.client.admin().indices().preparePutMapping(index).setType(type);

        PutMappingResponse response = mappingBuilder.setSource(mapping).get();

        return response.isAcknowledged();
    }

    /**
     * Delete elasticsearch index.
     *
     * @param index the index
     * @return true, if successful
     * @throws Exception the exception
     */
    public boolean deleteIndex(final String index) throws Exception {
        DeleteIndexRequest deleteRequest = new DeleteIndexRequest(index);

        DeleteIndexResponse response = this.client.admin().indices().delete(deleteRequest).get();

        return response.isAcknowledged();
    }

    /**
     * Index new document.
     *
     * @param index the index
     * @param type the type
     * @param document the document
     * @return the string
     * @throws Exception the exception
     */
    public String indexDocument(final String index, final String type, final Map<String, Object> document)
            throws Exception {
        return indexDocument(index, type, null, document);
    }

    /**
     * Index new document.
     *
     * @param index the index
     * @param type the type
     * @param parent the parent
     * @param document the document
     * @return the string
     * @throws Exception the exception
     */
    public String indexDocument(final String index, final String type, final String parent,
            final Map<String, Object> document) throws Exception {
        IndexRequestBuilder indexBuilder = this.client.prepareIndex().setIndex(index).setType(type).setRefresh(true)
                .setSource(document);

        if (parent != null) {
            indexBuilder.setParent(parent);
        }

        IndexResponse response = indexBuilder.get();

        this.client.admin().indices().refresh(new RefreshRequest(index)).get();

        return response.getId();
    }

    /**
     * Start elasticsearch node
     *
     * @throws Exception the exception
     */
    public void startNode() throws Exception {
        LOG.info("Started Elastic Search node.");
        this.node.start();
    }

    /**
     * Shutdown elasticsearch node.
     * Remove data directory.
     *
     * @throws Exception the exception
     */
    public void shutdownAndClean() throws Exception {
        LOG.info("Close ElasticSearch node.");
        this.node.close();

        LOG.info("Close ElasticSearch node.");
        new File(this.path.toString()).deleteOnExit();
    }

    /**
     * Returns elasticsearch client
     *
     * @return the client
     */
    public Client getClient() {
        return this.client;
    }

    /**
     * Gets the document by id.
     *
     * @param index the index
     * @param type the type
     * @param id the id
     * @return the document by id
     */
    public Map<String, Object> getDocumentById(final String index, final String type, final String id) {
        return getDocumentById(index, type, id, null);
    }

    /**
     * Gets the document by id.
     *
     * @param index the index
     * @param type the type
     * @param id the id
     * @param parent the parent
     * @return the document by id
     */
    public Map<String, Object> getDocumentById(final String index, final String type, final String id,
            final String parent) {

        GetRequestBuilder builder = this.client.prepareGet(index, type, id);

        if (parent != null) {
            builder.setParent(parent);
        }

        return builder.get().getSource();
    }

    /**
     * Returns parent for particular document if exist
     *
     * @param index the index
     * @param type the type
     * @param id the id
     * @param parent the parent
     * @return the document parent
     */
    public Object getDocumentParent(final String index, final String type, final String id, final String parent) {

        GetRequestBuilder builder = this.client.prepareGet(index, type, id).setFields(PARENT_FIELD);

        if (parent != null) {
            builder.setParent(parent);
        }

        return builder.get().getField(PARENT_FIELD).getValue();
    }

    /**
     * Returns timestamp for document if exist
     *
     * @param index the index
     * @param type the type
     * @param id the id
     * @return the document timestamp
     */
    public long getDocumentTimestamp(final String index, final String type, final String id) {
        GetResponse response = this.client.prepareGet(index, type, id).setFields(TIMESTAMP_FIELD).get();

        return (Long) response.getField(TIMESTAMP_FIELD).getValue();

    }

}
