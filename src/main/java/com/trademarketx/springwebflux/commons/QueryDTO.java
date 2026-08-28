package com.trademarketx.springwebflux.commons;

import java.util.List;
import java.util.Map;

public class QueryDTO {
    private Map<String, Object> associations;
    private Map<String, Object> results;
    private String search;
    private Map<String, List<String>> searchFields;
    private Map<String, Object> filters;
    private String filtersOperator;
    private Map<String, List<Map<String, String>>> sorting;
    private Integer size;
    private Long cursor;

    public Map<String, Object> getAssociations() { return associations; }
    public void setAssociations(Map<String, Object> associations) { this.associations = associations; }

    public Map<String, Object> getResults() { return results; }
    public void setResults(Map<String, Object> results) { this.results = results; }

    public String getSearch() { return search; }
    public void setSearch(String search) { this.search = search; }

    public Map<String, List<String>> getSearchFields() { return searchFields; }
    public void setSearchFields(Map<String, List<String>> searchFields) { this.searchFields = searchFields; }

    public Map<String, Object> getFilters() { return filters; }
    public void setFilters(Map<String, Object> filters) { this.filters = filters; }

    public String getFiltersOperator() { return filtersOperator; }
    public void setFiltersOperator(String filtersOperator) { this.filtersOperator = filtersOperator; }

    public Map<String, List<Map<String, String>>> getSorting() { return sorting; }
    public void setSorting(Map<String, List<Map<String, String>>> sorting) { this.sorting = sorting; }

    public Integer getSize() { return size; }
    public void setSize(Integer size) { this.size = size; }

    public Long getCursor() { return cursor; }
    public void setCursor(Long cursor) { this.cursor = cursor; }
}
