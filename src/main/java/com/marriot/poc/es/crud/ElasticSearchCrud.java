/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.marriot.poc.es.crud;

/**
 *
 */
public interface ElasticSearchCrud {

    public void CreateDocument(String id);

    public void getDocument();

    public void updateDocument();

    public void deleteDocument();

    public void getMultipleDocument();

    public void insertMultipleDocument();

    public void searchDocument();

}
