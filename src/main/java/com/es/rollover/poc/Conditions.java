package com.es.rollover.poc;

import java.util.HashMap;
import java.util.Map;

public class Conditions {

    private String maxAge;
    private Integer maxDocs;
    private String maxSize;
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    public String getMaxAge() {
        return maxAge;
    }

    public void setMaxAge(String maxAge) {
        this.maxAge = maxAge;
    }

    public Integer getMaxDocs() {
        return maxDocs;
    }

    public void setMaxDocs(Integer maxDocs) {
        this.maxDocs = maxDocs;
    }

    public String getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(String maxSize) {
        this.maxSize = maxSize;
    }

    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

}

