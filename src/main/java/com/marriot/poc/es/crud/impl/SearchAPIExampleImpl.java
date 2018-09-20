/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.marriot.poc.es.crud.impl;

import com.marriot.poc.es.crud.SearchAPIExample;
import com.marriot.poc.es.crud.utils.ElasticSearchClient;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;

/**
 *
 */
public class SearchAPIExampleImpl implements SearchAPIExample {

    private static final Logger log = Logger.getLogger("SearchAPIExampleImpl");
    private final ElasticSearchClient ESclient = null;
    private TransportClient client = null;

    /**
     * This method used to get the document using scroll concept
     */
    @Override
    public void getDocumentUsingScroll() {
        try {
            client = ESclient.getInstant();
            QueryBuilder query = termQuery("name", "sundar");
            SearchResponse scrollResp = client.prepareSearch("school", "college")
                    .setTypes("student")
                    .addSort("name", SortOrder.ASC)
                    .setScroll(new TimeValue(60000))
                    .setQuery(query)
                    .setSize(100)
                    .get(); //max of 100 hits will be returned for each scroll, Scroll until no hits are returned
            List<String> list = new ArrayList<>();
            do {
                scrollResp.getHits().forEach(hit -> {
                    //Handle the hit...
                    list.add(hit.field("name").toString());
                });

                scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
            }
            while (scrollResp.getHits().getHits().length != 0); // Zero hits mark the end of the scroll and the while loop.
            log.info("Search List : " + list);
        } catch (Exception ex) {
            log.info("Exception occurred while Scroll Document : " + ex);
        }
    }

    /**
     * This method get the all documents and return all fields(column)
     */
    @Override
    public void searchAll() {
        try {
            client = ESclient.getInstant();
            String[] index = {"school", "college"};
            SearchResponse response = client.prepareSearch(index)
                    .setTypes("student")
                    .setQuery(QueryBuilders.matchAllQuery())
                    .setFetchSource(true)
                    .execute()
                    .actionGet();
            response.getHits().forEach(action -> {
                log.info("Name : " + action.field("name"));
            });
        } catch (Exception ex) {
            log.severe("Exception occurred while Search All : " + ex);
        }
    }

    /**
     * This method get all Document and return specific fields(column)
     */
    @Override
    public void getSpecificFields() {
        try {
            client = ESclient.getInstant();
            String[] index = {"school", "college"};
            String[] requiredFields = {"name", "age"};
            SearchResponse response = client.prepareSearch(index)
                    .setTypes("student")
                    .setQuery(QueryBuilders.matchAllQuery())
                    .setFetchSource(requiredFields, null)
                    .execute()
                    .actionGet();
            Map<String, Integer> stdMap = new HashMap<>();
            response.getHits().forEach(action -> {
                stdMap.put(action.field("name").toString(), Integer.parseInt(action.field("age").toString()));
            });
            log.info("std : " + stdMap);
        } catch (Exception ex) {
            log.severe("Exception occurred while get specific fields : " + ex);
        }
    }

    @Override
    public void SearchByMustQuery() {
        try {
            client = ESclient.getInstant();
            String[] index = {"school", "college"};
            String[] requiredFields = {"name", "age"};
            BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

            boolQuery.must(QueryBuilders.termsQuery("name", "sundar"));
            boolQuery.must(QueryBuilders.termQuery("age", 24));
            QueryBuilder query = QueryBuilders.boolQuery().filter(boolQuery);

            SearchResponse response = client.prepareSearch(index)
                    .setTypes("student")
                    .setQuery(query)
                    .setFetchSource(requiredFields, null)
                    .execute()
                    .actionGet();
            Map<String, Integer> stdMap = new HashMap<>();
            response.getHits().forEach(action -> {
                stdMap.put(action.field("name").toString(), Integer.parseInt(action.field("age").toString()));
            });
            log.info("std : " + stdMap);
        } catch (Exception ex) {
            log.severe("Exception occurred while run MUST Query : " + ex);
        }
    }

    @Override
    public void SearchBySouldQuery() {
        try {
            client = ESclient.getInstant();
            String[] index = {"school", "college"};
            String[] requiredFields = {"name", "age"};
            BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

            boolQuery.should(QueryBuilders.termsQuery("name", "sundar"));
            boolQuery.should(QueryBuilders.termQuery("age", 24));
            QueryBuilder query = QueryBuilders.boolQuery().filter(boolQuery);

            SearchResponse response = client.prepareSearch(index)
                    .setTypes("student")
                    .setQuery(query)
                    .setFetchSource(requiredFields, null)
                    .execute()
                    .actionGet();
            Map<String, Integer> stdMap = new HashMap<>();
            response.getHits().forEach(action -> {
                stdMap.put(action.field("name").toString(), Integer.parseInt(action.field("age").toString()));
            });
            log.info("std : " + stdMap);
        } catch (Exception ex) {
            log.severe("Exception occurred while run MUST Query : " + ex);
        }
    }

    @Override
    public void SearchByAgregationQuery() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
