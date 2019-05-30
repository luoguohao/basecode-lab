package com.luogh.learning.lab.elasticsearch;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import com.alibaba.fastjson.JSONObject;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkIndexByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

@Slf4j
public class ApiApp {

  TransportClient client;

  public ApiApp() {
    Settings settings = Settings.builder()
        .put("cluster.name", "wifianalytics.es")
        .put("client.transport.sniff", true)
        .put("client.transport.ping_timeout", 10, TimeUnit.SECONDS)
        .put("client.transport.nodes_sampler_interval", 5, TimeUnit.SECONDS)
        .build();
    try {
      client = new PreBuiltTransportClient(settings)
          .addTransportAddress(
              new InetSocketTransportAddress(InetAddress.getByName("elasticsearch01.td.com"), 9300))
          .addTransportAddress(
              new InetSocketTransportAddress(InetAddress.getByName("elasticsearch02.td.com"),
                  9300));
    } catch (UnknownHostException e) {
      throw new RuntimeException("init elastic search client failed", e);
    }
  }


  public static void main(String[] args) throws Exception {
    ApiApp bulkLoadApp = new ApiApp();
    bulkLoadApp.index();
    bulkLoadApp.get();
    bulkLoadApp.deleteByQuery();
    bulkLoadApp.update();
    bulkLoadApp.upsert();
    bulkLoadApp.multiGet();
    bulkLoadApp.bulkRequest();
    bulkLoadApp.bulkProcessor();
    bulkLoadApp.search();
  }


  public void index() throws Exception {
    IndexResponse indexResponse = client.prepareIndex("index_test", "index_test_type", "1")
        .setSource(jsonBuilder()
            .startObject()
            .field("user", "kimchy")
            .field("date", "2018-08-01")
            .field("message", "trying out Elasticsearch")
            .endObject()).get();

    RestStatus status = indexResponse.status();
    log.info("rest status response code:{}", status.getStatus());
    log.info("rest status response index:{}", indexResponse.getIndex());
    log.info("rest status response id:{}", indexResponse.getId());
    log.info("rest status response version:{}", indexResponse.getVersion());
  }


  public void get() throws Exception {
    GetResponse response = client.prepareGet("index_test", "index_test_type", "1")
        .setOperationThreaded(true)
        .get();

    Map<String, Object> sourceAsMap = response.getSourceAsMap();
    log.info("rest status response :{}", new JSONObject(sourceAsMap).toJSONString());
  }


  public void deleteByQuery() throws Exception {
    DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
        .filter(QueryBuilders.matchQuery("user", "kimchy"))
        .source("index_test")
        .execute(new ActionListener<BulkIndexByScrollResponse>() {
          @Override
          public void onResponse(BulkIndexByScrollResponse bulkIndexByScrollResponse) {
            long deleted = bulkIndexByScrollResponse.getDeleted();
            log.info("delete:{}", deleted);
          }

          @Override
          public void onFailure(Exception e) {
            log.error("batch failed, ", e);
          }
        });
  }


  public void update() throws Exception {
    UpdateResponse response = client.prepareUpdate("index_test", "index_test_type", "1")
        .setDoc(jsonBuilder()
            .startObject()
            .field("gender", "male")
            .endObject())
        .get();

    log.info(response.toString());

    UpdateResponse response2 = client.prepareUpdate("index_test", "index_test_type", "1")
        .setScript(new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG,
            "ctx._source.gender = \"female\"", new HashMap<>()))
        .get();
    log.info(response2.toString());
  }


  public void upsert() throws Exception {
    IndexRequest indexRequest = new IndexRequest("index_test", "index_test_type", "2")
        .source(jsonBuilder()
            .startObject()
            .field("name", "Joe Smith")
            .field("gender", "male")
            .endObject());
    UpdateRequest updateRequest = new UpdateRequest("index_test", "index_test_type", "2")
        .doc(jsonBuilder()
            .startObject()
            .field("gender", "male")
            .endObject())
        .upsert(indexRequest);
    UpdateResponse response = client.update(updateRequest).get();
    log.info("update with response: {}", response);
  }


  public void multiGet() throws Exception {
    MultiGetResponse multiGetItemResponses = client.prepareMultiGet()
        .add("index_test", "index_test_type", "1")
        .add("index_test", "index_test_type", "2")
        .get();

    for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
      GetResponse response = itemResponse.getResponse();
      if (response.isExists()) {
        log.info(response.getSourceAsString());
      }
    }
  }


  public void bulkRequest() throws Exception {
    BulkRequestBuilder bulkRequest = client.prepareBulk();
    bulkRequest.add(client.prepareIndex("index_test", "index_test_type")
        .setSource(jsonBuilder()
            .startObject()
            .field("user", "kimchy")
            .field("postDate", new Date())
            .field("message", "trying out Elasticsearch")
            .endObject()
        )
    );

    bulkRequest.add(client.prepareIndex("index_test", "index_test_type")
        .setSource(jsonBuilder()
            .startObject()
            .field("user", "kimchy")
            .field("postDate", new Date())
            .field("message", "another post")
            .endObject()
        )
    );

    BulkResponse bulkResponse = bulkRequest.get();
    if (bulkResponse.hasFailures()) {
      log.error("bulk with failed, {}", bulkResponse.buildFailureMessage());
    }
  }


  public void bulkProcessor() throws Exception {

    BulkProcessor bulkProcessor = BulkProcessor.builder(
        client,
        new BulkProcessor.Listener() {
          @Override
          public void beforeBulk(long executionId,
              BulkRequest request) {
            log.info("before bulk, with executionId:{} and request number of actions:{}",
                executionId, request.numberOfActions());
          }

          @Override
          public void afterBulk(long executionId,
              BulkRequest request,
              BulkResponse response) {
            log.info(
                "after bulk, with executionId:{} and request number of actions:{} and response took:{} ms",
                executionId, request.numberOfActions(), response.getTookInMillis());
          }

          @Override
          public void afterBulk(long executionId,
              BulkRequest request,
              Throwable failure) {
            log.error(
                "after bulk, with executionId:{} and request number of actions:{} and response with failed.",
                executionId, request.numberOfActions(),
                failure.fillInStackTrace().getLocalizedMessage());
          }
        })
        .setBulkActions(400) // execute the bulk every 400 requests
        .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))  // flush the bulk every 5mb
        .setFlushInterval(TimeValue
            .timeValueSeconds(5)) //  flush the bulk every 5 seconds whatever the number of requests
        // Set the number of concurrent requests. A value of 0 means that only a single request will
        // be allowed to be executed. A value of 1 means 1 concurrent request is allowed to be executed
        // while accumulating new bulk requests.
        .setConcurrentRequests(1)
        // Set a custom backoff policy which will initially wait for 100ms, increase exponentially
        // and retries up to three times. A retry is attempted whenever one or more bulk item requests
        // have failed with an EsRejectedExecutionException which indicates that there were too little
        // compute resources available for processing the request. To disable backoff,
        // pass BackoffPolicy.noBackoff().
        .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
        .build();

    IntStream.range(0, 1500).forEach(index -> {
      try {
        bulkProcessor.add(new IndexRequest("index_test", "index_test_type")
            .source(jsonBuilder()
                .startObject()
                .field("user", "tset2" + index)
                .field("postDate", new Date())
                .field("message", "another post" + index)
                .endObject())
        );
      } catch (IOException e) {
        log.error("build index request failed.", e);
      }
    });

    bulkProcessor.flush();
    bulkProcessor.awaitClose(10, TimeUnit.MINUTES);
  }


  public void search() throws Exception {
    SearchResponse response = client.prepareSearch("index_test")
        .setTypes("index_test_type")
        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
        .setQuery(QueryBuilders.termQuery("user", "tset2"))                 // Query
        .setSource(SearchSourceBuilder.searchSource().fetchSource("postDate", null))
        .get();
    log.info(response.toString());
  }
}
