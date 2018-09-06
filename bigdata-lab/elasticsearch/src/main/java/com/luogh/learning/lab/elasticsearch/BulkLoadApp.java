package com.luogh.learning.lab.elasticsearch;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Stopwatch;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

@Slf4j
public class BulkLoadApp {

  TransportClient client;

  public BulkLoadApp() {
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
    Options options = new Options();
    options.addRequiredOption("p", "inputPath", true, "bulk load data file path");
    options.addRequiredOption("i", "index", true, "which index to load");
    options.addOption("b", "batchSize", true, "bulk load batch size, default 1000");
    options.addOption("f", "flushInterval", true, "flush interval in seconds, default 5");
    options.addOption("c", "concurrentRequest", true, "concurrent request, default 1");

    options.addOption("h", "help", false, "usage");

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);
    if (cmd.hasOption("h")) {
      HelpFormatter hf = new HelpFormatter();
      hf.setWidth(110);
      hf.printHelp("bulk load data into elasticsearch", options, true);
    }

    String inputPath = cmd.getOptionValue("p");
    String index = cmd.getOptionValue("i");
    Integer batchSize = Integer.parseInt(cmd.getOptionValue("b", "1000"));
    Integer flushInterval = Integer.parseInt(cmd.getOptionValue("f", "5"));
    Integer concurrentRequests = Integer.parseInt(cmd.getOptionValue("c", "1"));

    if (StringUtils.isEmpty(index) || StringUtils.isEmpty(inputPath) || batchSize < 1) {
      throw new IllegalArgumentException("invalid parameters");
    }

    Stopwatch stopwatch = Stopwatch.createStarted();
    int resultCount = new BulkLoadApp()
        .bulkload(inputPath, index, batchSize, flushInterval, concurrentRequests);

    log.info("bulk load data into es using {} seconds for {} records in path: {}.",
        stopwatch.elapsed(TimeUnit.SECONDS), resultCount, inputPath);
  }

  public int bulkload(String inputPath, String index, Integer batchSize, Integer flushInterval,
      Integer concurrentRequests) throws Exception {
    AtomicInteger recordCount = new AtomicInteger(0);

    BulkProcessor bulkProcessor = BulkProcessor.builder(
        client,
        new BulkProcessor.Listener() {
          @Override
          public void beforeBulk(long executionId,
              BulkRequest request) {
            log.debug("before bulk, with executionId:{} and request number of actions:{}",
                executionId, request.numberOfActions());
          }

          @Override
          public void afterBulk(long executionId,
              BulkRequest request,
              BulkResponse response) {
            log.info(
                "after bulk, with executionId:{} and request number of actions:{} and response took:{} ms",
                executionId, request.numberOfActions(), response.getTookInMillis());
            if (response.hasFailures()) {
              log.error("bulk with failures: {}", response.buildFailureMessage());
            }
          }

          @Override
          public void afterBulk(long executionId,
              BulkRequest request,
              Throwable failure) {
            log.error(
                "after bulk, with executionId:{} and request number of actions:{} and response with failed {}.",
                executionId, request.numberOfActions(),
                failure.fillInStackTrace().getLocalizedMessage());
          }
        })
        .setBulkActions(batchSize) // execute the bulk every 400 requests
        .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))  // flush the bulk every 5mb
        .setFlushInterval(TimeValue.timeValueSeconds(flushInterval))
        .setConcurrentRequests(concurrentRequests)
        .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
        .build();

    Files.lines(Paths.get(inputPath))
        .filter(StringUtils::isNotBlank)
        .forEach(line -> {
          recordCount.incrementAndGet();
          String id = JSONObject.parseObject(line).getString("userId");
          bulkProcessor.add(new IndexRequest(index, index).id(id).source(line));
        });

    bulkProcessor.flush();
    bulkProcessor.awaitClose(10, TimeUnit.MINUTES);
    return recordCount.get();
  }
}