/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.es.rollover.poc.crud.impl;

import com.es.rollover.poc.crud.ElasticSearchCrud;
import com.es.rollover.poc.crud.utils.ElasticSearchClient;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHits;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 */
public class ElasticSearchCrudImpl implements ElasticSearchCrud {

    private static final Logger log = Logger.getLogger("ElasticSearchCrudImpl");
    private ElasticSearchClient ESclient = null;
    private TransportClient client = null;

    public ElasticSearchCrudImpl() {
        ESclient = new ElasticSearchClient();
    }

    /**
     * This method Create the Index and insert the document(s)
     */
    @Override
    public void CreateDocument(String id) {

        try {
            client = ESclient.getInstant();

            if (!client.admin().indices().aliasesExist(new GetAliasesRequest().aliases("school")).actionGet().isExists()) {
                System.out.println("Creating alias");

                client.admin().indices().create(new CreateIndexRequest().alias(new Alias("school")).index("school-000001")).actionGet();

                /*RestHighLevelClient restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));

                RolloverRequest rolloverRequest = new RolloverRequest("school", "school-000002");

                rolloverRequest.addMaxIndexAgeCondition(new TimeValue(5, TimeUnit.SECONDS));
                rolloverRequest.addMaxIndexDocsCondition(3);
                rolloverRequest.addMaxIndexSizeCondition(new ByteSizeValue(100, ByteSizeUnit.KB));
                //rolloverRequest.getCreateIndexRequest().alias(new Alias("school"));

                restHighLevelClient.indices().rollover(rolloverRequest, RequestOptions.DEFAULT);*/

            }

            client.admin().indices().prepareRolloverIndex("school")
                    .addMaxIndexAgeCondition(new TimeValue(5, TimeUnit.SECONDS))
                    .addMaxIndexDocsCondition(3)
                    .addMaxIndexSizeCondition(new ByteSizeValue(5, ByteSizeUnit.KB))
                    .get();

            IndexResponse response = client.prepareIndex("school", "tenth", id)
                    .setSource(jsonBuilder()
                            .startObject()
                            .field("name", "Sundar")
                            .endObject()
                    ).get();

            if (response != null) {
                String _index = response.getIndex();
                String _type = response.getType();
                String _id = response.getId();
                long _version = response.getVersion();
                RestStatus status = response.status();
                log.info("Index has been created successfully with Index: " + _index + " / Type: " + _type + "ID: " + _id);
            }
        } catch (IOException ex) {
            log.severe("Exception occurred while Insert Index : " + ex);
        }
    }

    /**
     * This method get the matched document
     */
    @Override
    public void getDocument() {
        try {
            client = ESclient.getInstant();
            GetResponse response = client.prepareGet("school", "tenth", "1")
                    .get();
            if (response != null) {
                Map<String, DocumentField> FieldsMap = response.getFields();
                log.info("Response Data : " + FieldsMap.toString());
            }
        } catch (Exception ex) {
            log.severe("Exception occurred while get Document : " + ex);
        }
    }

    /**
     * This method delete the matched Document
     */
    @Override
    public void deleteDocument() {
        try {
            client = ESclient.getInstant();
            DeleteResponse deleteResponse = client.prepareDelete("school", "tenth", "1")
                    //                    .setOperationThreaded(false)
                    .get();
            if (deleteResponse != null) {
                deleteResponse.status();
                deleteResponse.toString();
                log.info("Document has been deleted...");
            }
        } catch (Exception ex) {
            log.severe("Exception occurred while delete Document : " + ex);
        }
    }

    /**
     * This method updated the matched Document
     */
    @Override
    public void updateDocument() {
        try {
            client = ESclient.getInstant();
            UpdateRequest updateReguest = new UpdateRequest("school", "tenth", "1")
                    .doc(jsonBuilder()
                            .startObject()
                            .field("name", "Sundar S")
                            .endObject());
            UpdateResponse updateResponse = client.update(updateReguest).get();
            if (updateResponse != null) {
                updateResponse.getGetResult();
                log.info("Index has been updated successfully...");
            }
        } catch (IOException | InterruptedException | ExecutionException ex) {
            log.severe("Exception occurred while update Document : " + ex);
        }
    }

    /**
     * This method get the multiple Documents
     */
    @Override
    public void getMultipleDocument() {
        try {
            client = ESclient.getInstant();
            MultiGetResponse multipleItems = client.prepareMultiGet()
                    .add("school", "tenth", "1")
                    .add("school", "nineth", "1", "2", "3", "4")
                    .add("college", "be", "1")
                    .get();

            multipleItems.forEach(multipleItem -> {
                GetResponse response = multipleItem.getResponse();
                if (response.isExists()) {
                    String json = response.getSourceAsString();
                    log.info("Respense Data : " + json);
                }
            });
        } catch (Exception ex) {
            log.severe("Exception occurred while get Multiple Document : " + ex);
        }
    }

    /**
     * This method insert the more than one Document at a time
     */
    @Override
    public void insertMultipleDocument() {
        try {
            client = ESclient.getInstant();
            BulkRequestBuilder bulkDocument = client.prepareBulk();

            // either use client#prepare, or use Requests# to directly build index/delete requests
            bulkDocument.add(client.prepareIndex("school", "tenth", "1")
                    .setSource(jsonBuilder()
                            .startObject()
                            .field("name", "Sundar")
                            .field("age", "23")
                            .field("gender", "Male")
                            .endObject()
                    )
            );

            bulkDocument.add(client.prepareIndex("school", "tenth", "2")
                    .setSource(jsonBuilder()
                            .startObject()
                            .field("name", "Sundar S")
                            .field("age", "23")
                            .field("gender", "Male")
                            .endObject()
                    )
            );
            BulkResponse bulkResponse = bulkDocument.get();
            if (bulkResponse.hasFailures()) {
                // process failures by iterating through each bulk response item
            } else {
                log.info("All Documents inserted successfully...");
            }

        } catch (IOException ex) {
            log.severe("Exception occurred while get Multiple Document : " + ex);
        }
    }

    /**
     * This method Search the available Document
     */
    @Override
    public void searchDocument() {
        SearchHits hits = null;
        try {
            client = ESclient.getInstant();
            SearchResponse response = client.prepareSearch("school", "college")
                    .setTypes("tenth", "be")
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setQuery(QueryBuilders.termQuery("name", "sundar"))
                    .setPostFilter(QueryBuilders.rangeQuery("age").from(15).to(24))
                    .setFrom(0).setSize(60).setExplain(true)
                    .get();

            if (response != null) {
                hits = response.getHits();
            }
            if (hits != null) {

                while (hits.iterator().hasNext()) {
                    hits.iterator().next();
                }
            }
        } catch (Exception ex) {
            log.severe("Excption occurred while Search Document : " + ex);
        }
    }
}
