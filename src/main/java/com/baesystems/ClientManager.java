/*
 * Software Copyright BAE Systems plc 2015. All Rights Reserved.
 * BAE SYSTEMS, DETICA and CYBERREVEAL are trademarks of BAE Systems
 * plc and may be registered in certain jurisdictions.
 */
package com.baesystems;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for creating elasticsearch client.
 */
public class ClientManager {

    private static final Logger LOG = LoggerFactory.getLogger(ClientManager.class);

    private final Client client;

    public ClientManager(final String clusterName, final String... hosts) {

        LOG.info("Creating new client manager.");
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).build();
        TransportClient client = new TransportClient(settings);

        for (String host : hosts) {
            LOG.info("Adding new '{}' host.", host);
            client.addTransportAddress(new InetSocketTransportAddress(host, 9300));
        }

        this.client = client;

    }

    /**
     * Returns Elasticsearch client.
     *
     * @return the client
     */
    public Client getClient() {
        return client;
    }
}
