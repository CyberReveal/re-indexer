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

public class ElasticSearchNode {

    private final static Logger LOG = LoggerFactory.getLogger(ElasticSearchNode.class);
    private final static String DEFAULT_NAME = "es-data";
    private final Path path;

    private final Client client;
    private final Node node;

    public ElasticSearchNode() throws InterruptedException {
        LOG.debug(String.format("Created class %s.", getClass().getName()));

        try {
            path = Files.createTempDirectory(DEFAULT_NAME);
            LOG.info(String.format("Path: %s", path.toString()));
        } catch (IOException e) {
            throw new RuntimeException("Cannot create data firectory.", e);
        }

        ImmutableSettings.Builder elasticsearchSettings = ImmutableSettings.settingsBuilder()
                .put("http.enabled", "false").put("path.data", path.toString())
                .put("index.routing.allocation.disable_allocation", "false");

        this.node = nodeBuilder().local(true).settings(elasticsearchSettings).node();

        Thread.sleep(2000);

        this.client = node.client();

    }

    public boolean createIndex(final String index) throws Exception {
        Settings indexSettings = ImmutableSettings.settingsBuilder().put("number_of_shards", 1)
                .put("number_of_replicas", 0).build();

        CreateIndexRequestBuilder builder = this.client.admin().indices().prepareCreate(index)
                .setSettings(indexSettings);

        CreateIndexResponse response = builder.get();

        return response.isAcknowledged();
    }

    public boolean putMapping(final String index, final String type, final XContentBuilder mapping) {
        PutMappingRequestBuilder mappingBuilder = this.client.admin().indices().preparePutMapping(index).setType(type);

        PutMappingResponse response = mappingBuilder.setSource(mapping).get();

        return response.isAcknowledged();
    }

    public boolean deleteIndex(final String index) throws Exception {
        DeleteIndexRequest deleteRequest = new DeleteIndexRequest(index);

        DeleteIndexResponse response = this.client.admin().indices().delete(deleteRequest).get();

        return response.isAcknowledged();
    }

    public String indexDocument(final String index, final String type, final Map<String, Object> document)
            throws Exception {
        return indexDocument(index, type, null, document);
    }

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

    public void afterPropertiesSet() throws Exception {
        LOG.info("Started Elastic Search node.");
        this.node.start();
    }

    public void destroy() throws Exception {
        LOG.info("Close ElasticSearch node.");
        this.node.close();

        LOG.info("Close ElasticSearch node.");
        new File(path.toString()).deleteOnExit();
    }

    public Client getClient() {
        return this.client;
    }

    public Map<String, Object> getDocumentById(final String index, final String type, final String id) {
        return getDocumentById(index, type, id, null);
    }

    public Map<String, Object> getDocumentById(final String index, final String type, final String id,
            final String parent) {

        GetRequestBuilder builder = this.client.prepareGet(index, type, id);

        if (parent != null) {
            builder.setParent(parent);
        }

        return builder.get().getSource();
    }

    public Object getDocumentParent(final String index, final String type, final String id, final String parent) {

        GetRequestBuilder builder = this.client.prepareGet(index, type, id).setFields("_parent");

        if (parent != null) {
            builder.setParent(parent);
        }

        return builder.get().getField("_parent").getValue();
    }

    public long getDocumentTimestamp(String index, String type, String id) {
        GetResponse response = this.client.prepareGet(index, type, id).setFields("_timestamp").get();

        return (Long) response.getField("_timestamp").getValue();

    }

}
