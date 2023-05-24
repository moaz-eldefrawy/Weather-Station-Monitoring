package com.example.centralstation;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import com.example.weatherstation.models.Status;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;

public class ElasticSearchStatusWriter {
  RestClient restClient = RestClient.builder(
      new HttpHost(Constants.ELASTIC_SEARCH_SERVER.split(":")[0],
          Integer.valueOf(Constants.ELASTIC_SEARCH_SERVER.split(
              ":")[1])))
      .build();

  ElasticsearchTransport transport = new RestClientTransport(
      restClient, new JacksonJsonpMapper());

  ElasticsearchClient client = new ElasticsearchClient(transport);

  ElasticSearchStatusWriter() {

  }

  public void write(Status status) throws ElasticsearchException, IOException {
    String json = convertStatusToJson(status);
    System.out.println(json);
    Reader input = new StringReader(json);
    IndexRequest<JsonData> request = IndexRequest.of(i -> i
        .index("status")
        .withJson(input));

    IndexResponse response = client.index(request);
    System.out.println(response.toString());
  }

  private Map<String, Object> convertStatusToMap(Status status) {
    Map<String, Object> map = new HashMap<>();
    map.put("stationId", status.getStationId());
    map.put("batteryStatus", status.getBatteryStatus());
    map.put("sNo", status.getsNo());
    map.put("statusTimestamp", status.getStatusTimestamp());
    return map;
  }

  private String convertMapToJson(Map<String, Object> map) throws JsonProcessingException {
    ObjectMapper objectMapper = new ObjectMapper();
    String json = objectMapper.writeValueAsString(map);
    return json;
  }

  private String convertStatusToJson(Status status) throws JsonProcessingException {
    Map<String, Object> map = convertStatusToMap(status);
    String json = null;
    json = convertMapToJson(map);
    return json;
  }
}
