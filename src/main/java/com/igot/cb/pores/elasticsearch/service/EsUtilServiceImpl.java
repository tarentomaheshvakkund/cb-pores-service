package com.igot.cb.pores.elasticsearch.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.pores.elasticsearch.dto.FacetDTO;
import com.igot.cb.pores.elasticsearch.dto.SearchCriteria;
import com.igot.cb.pores.elasticsearch.dto.SearchResult;
import com.igot.cb.pores.util.Constants;
import com.networknt.schema.JsonSchemaFactory;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class EsUtilServiceImpl implements EsUtilService {

  @Autowired
  private RestHighLevelClient elasticsearchClient;
  @Autowired
  private ObjectMapper objectMapper;

  @Value("${elastic.required.field.json.path}")
  private String requiredJsonFilePath;

  @Override
  public RestStatus addDocument(
      String esIndexName, String type, String id, Map<String, Object> document) {
    try {
      JsonSchemaFactory schemaFactory = JsonSchemaFactory.getInstance();
      InputStream schemaStream = schemaFactory.getClass().getResourceAsStream(requiredJsonFilePath);
      Map<String, Object> map = objectMapper.readValue(schemaStream,
          new TypeReference<Map<String, Object>>() {
          });
      Iterator<Entry<String, Object>> iterator = document.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<String, Object> entry = iterator.next();
        String key = entry.getKey();
        if (!map.containsKey(key)) {
          iterator.remove();
        }
      }
      IndexRequest indexRequest =
          new IndexRequest(esIndexName, type, id).source(document, XContentType.JSON);
      IndexResponse response = elasticsearchClient.index(indexRequest, RequestOptions.DEFAULT);
      return response.status();
    } catch (Exception e) {
      log.error("Issue while Indexing to es: {}", e.getMessage());
      return null;
    }
  }

  @Override
  public RestStatus updateDocument(
      String index, String indexType, String entityId, Map<String, Object> updatedDocument) {
    try {
      JsonSchemaFactory schemaFactory = JsonSchemaFactory.getInstance();
      InputStream schemaStream = schemaFactory.getClass().getResourceAsStream(requiredJsonFilePath);
      Map<String, Object> map = objectMapper.readValue(schemaStream,
          new TypeReference<Map<String, Object>>() {
          });
      Iterator<Map.Entry<String, Object>> iterator = updatedDocument.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<String, Object> entry = iterator.next();
        String key = entry.getKey();
        if (!map.containsKey(key)) {
          iterator.remove();
        }
      }
      IndexRequest indexRequest =
          new IndexRequest(index)
              .id(entityId)
              .source(updatedDocument)
              .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
      IndexResponse response = elasticsearchClient.index(indexRequest, RequestOptions.DEFAULT);
      return response.status();
    } catch (IOException e) {
      return null;
    }
  }

  @Override
  public void deleteDocument(String documentId, String esIndexName) {
    try {
      DeleteRequest request = new DeleteRequest(esIndexName, Constants.INDEX_TYPE, documentId);
      DeleteResponse response = elasticsearchClient.delete(request, RequestOptions.DEFAULT);
      if (response.getResult() == DocWriteResponse.Result.DELETED) {
        log.info("Document deleted successfully from elasticsearch.");
      } else {
        log.error("Document not found or failed to delete from elasticsearch.");
      }
    } catch (Exception e) {
      log.error("Error occurred during deleting document in elasticsearch");
    }
  }

  @Override
  public SearchResult searchDocuments(String esIndexName, SearchCriteria searchCriteria) {
    SearchSourceBuilder searchSourceBuilder = buildSearchSourceBuilder(searchCriteria);
    SearchRequest searchRequest = new SearchRequest(esIndexName);
    searchRequest.source(searchSourceBuilder);
    try {
      if (searchSourceBuilder != null) {
        int pageNumber = searchCriteria.getPageNumber();
        int pageSize = searchCriteria.getPageSize();
        searchSourceBuilder.from(pageNumber);
        if (pageSize != 0) {
          searchSourceBuilder.size(pageSize);
        }
      }
      SearchResponse paginatedSearchResponse =
          elasticsearchClient.search(searchRequest, RequestOptions.DEFAULT);
      List<Map<String, Object>> paginatedResult = extractPaginatedResult(paginatedSearchResponse);
      Map<String, List<FacetDTO>> fieldAggregations =
          extractFacetData(paginatedSearchResponse, searchCriteria);
      SearchResult searchResult = new SearchResult();
      searchResult.setData(objectMapper.valueToTree(paginatedResult));
      searchResult.setFacets(fieldAggregations);
      searchResult.setTotalCount(paginatedSearchResponse.getHits().getTotalHits().value);
      return searchResult;
    } catch (IOException e) {
      log.error("Error while fetching details from elastic search");
      return null;
    }
  }

  private Map<String, List<FacetDTO>> extractFacetData(
      SearchResponse searchResponse, SearchCriteria searchCriteria) {
    Map<String, List<FacetDTO>> fieldAggregations = new HashMap<>();
    if (searchCriteria.getFacets() != null) {
      for (String field : searchCriteria.getFacets()) {
        Terms fieldAggregation = searchResponse.getAggregations().get(field + "_agg");
        List<FacetDTO> fieldValueList = new ArrayList<>();
        for (Terms.Bucket bucket : fieldAggregation.getBuckets()) {
          if (!bucket.getKeyAsString().isEmpty()) {
            FacetDTO facetDTO = new FacetDTO(bucket.getKeyAsString(), bucket.getDocCount());
            fieldValueList.add(facetDTO);
          }
        }
        fieldAggregations.put(field, fieldValueList);
      }
    }
    return fieldAggregations;
  }

  private List<Map<String, Object>> extractPaginatedResult(SearchResponse paginatedSearchResponse) {
    SearchHit[] hits = paginatedSearchResponse.getHits().getHits();
    List<Map<String, Object>> paginatedResult = new ArrayList<>();
    for (SearchHit hit : hits) {
      paginatedResult.add(hit.getSourceAsMap());
    }
    return paginatedResult;
  }

  private SearchSourceBuilder buildSearchSourceBuilder(SearchCriteria searchCriteria) {
    log.info("Building search query");
    if (searchCriteria == null || searchCriteria.toString().isEmpty()) {
      log.error("Search criteria body is missing");
      return null;
    }
    BoolQueryBuilder boolQueryBuilder = buildFilterQuery(searchCriteria.getFilterCriteriaMap());
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(boolQueryBuilder);
    addSortToSearchSourceBuilder(searchCriteria, searchSourceBuilder);
    addRequestedFieldsToSearchSourceBuilder(searchCriteria, searchSourceBuilder);
    addQueryStringToFilter(searchCriteria.getSearchString(), boolQueryBuilder);
    addFacetsToSearchSourceBuilder(searchCriteria.getFacets(), searchSourceBuilder);
    return searchSourceBuilder;
  }

  private BoolQueryBuilder buildFilterQuery(Map<String, Object> filterCriteriaMap) {
    BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
    if (filterCriteriaMap != null) {
      filterCriteriaMap.forEach(
          (field, value) -> {
            if (value instanceof Boolean) {
              boolQueryBuilder.must(QueryBuilders.termQuery(field, value));
            } else if (value instanceof ArrayList) {
              boolQueryBuilder.must(
                  QueryBuilders.termsQuery(
                      field + Constants.KEYWORD, ((ArrayList<?>) value).toArray()));
            } else if (value instanceof String) {
              boolQueryBuilder.must(QueryBuilders.termsQuery(field + Constants.KEYWORD, value));
            }
          });
    }
    return boolQueryBuilder;
  }

  private void addSortToSearchSourceBuilder(
      SearchCriteria searchCriteria, SearchSourceBuilder searchSourceBuilder) {
    if (isNotBlank(searchCriteria.getOrderBy()) && isNotBlank(searchCriteria.getOrderDirection())) {
      SortOrder sortOrder =
          Constants.ASC.equals(searchCriteria.getOrderDirection()) ? SortOrder.ASC : SortOrder.DESC;
      searchSourceBuilder.sort(
          SortBuilders.fieldSort(searchCriteria.getOrderBy() + Constants.KEYWORD).order(sortOrder));
    }
  }

  private void addRequestedFieldsToSearchSourceBuilder(
      SearchCriteria searchCriteria, SearchSourceBuilder searchSourceBuilder) {
    if (searchCriteria.getRequestedFields() == null) {
      // Get all fields in response
      searchSourceBuilder.fetchSource(null);
    } else {
      if (searchCriteria.getRequestedFields().isEmpty()) {
        log.error("Please specify at least one field to include in the results.");
      }
      searchSourceBuilder.fetchSource(
          searchCriteria.getRequestedFields().toArray(new String[0]), null);
    }
  }

  private void addQueryStringToFilter(String searchString, BoolQueryBuilder boolQueryBuilder) {
    if (isNotBlank(searchString)) {
      boolQueryBuilder.must(
          QueryBuilders.boolQuery()
              .should(new WildcardQueryBuilder("searchTags.keyword", "*" + searchString + "*")));
    }
  }

  private void addFacetsToSearchSourceBuilder(
      List<String> facets, SearchSourceBuilder searchSourceBuilder) {
    if (facets != null) {
      for (String field : facets) {
        searchSourceBuilder.aggregation(
            AggregationBuilders.terms(field + "_agg").field(field + ".keyword").size(250));
      }
    }
  }

  private boolean isNotBlank(String value) {
    return value != null && !value.trim().isEmpty();
  }

  @Override
  public void deleteDocumentsByCriteria(String esIndexName, SearchSourceBuilder sourceBuilder) {
    try {
      SearchHits searchHits = executeSearch(esIndexName, sourceBuilder);
      if (searchHits.getTotalHits().value > 0) {
        BulkResponse bulkResponse = deleteMatchingDocuments(esIndexName, searchHits);
        if (!bulkResponse.hasFailures()) {
          log.info("Documents matching the criteria deleted successfully from Elasticsearch.");
        } else {
          log.error("Some documents failed to delete from Elasticsearch.");
        }
      } else {
        log.info("No documents match the criteria.");
      }
    } catch (Exception e) {
      log.error("Error occurred during deleting documents by criteria from Elasticsearch.", e);
    }
  }

  private SearchHits executeSearch(String esIndexName, SearchSourceBuilder sourceBuilder)
      throws IOException {
    SearchRequest searchRequest = new SearchRequest(esIndexName);
    searchRequest.source(sourceBuilder);
    SearchResponse searchResponse =
        elasticsearchClient.search(searchRequest, RequestOptions.DEFAULT);
    return searchResponse.getHits();
  }

  private BulkResponse deleteMatchingDocuments(String esIndexName, SearchHits searchHits)
      throws IOException {
    BulkRequest bulkRequest = new BulkRequest();
    searchHits.forEach(
        hit -> bulkRequest.add(new DeleteRequest(esIndexName, Constants.INDEX_TYPE, hit.getId())));
    return elasticsearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
  }
}

