/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.marriot.poc.es.crud.utils;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 *
 */
public class ElasticSearchClient {

    private static final Logger log = Logger.getLogger("ElasticSearchClient");
    private TransportClient client = null;
    private Properties elasticPro = null;

    public ElasticSearchClient() {
        try {
            elasticPro = new Properties();
            elasticPro.load(ElasticSearchClient.class.getResourceAsStream(ElasticSearchConstants.ELASTIC_PROPERTIES));
            log.info(elasticPro.getProperty("host"));
        } catch (IOException ex) {
            log.info("Exception occurred while load elastic properties : " + ex);
        }
    }

    public TransportClient getInstant() {
        if (client == null) {
            client = getElasticClient();
        }
        return client;
    }

    private TransportClient getElasticClient() {
        try {

            Settings setting = Settings.builder()
                    .put("cluster.name", elasticPro.getProperty("cluster"))
                    .put("client.transport.sniff", Boolean.valueOf(elasticPro.getProperty("transport.sniff"))).build();

            client = new PreBuiltTransportClient(setting)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName(elasticPro.getProperty("host")), Integer.valueOf(elasticPro.getProperty("port"))));

        } catch (UnknownHostException ex) {
            log.severe("Exception occurred while getting Client : " + ex);
        }
        return client;
    }

    public void closeClient(TransportClient client) {
        if (client != null) {
            client.close();
        }
    }
}
