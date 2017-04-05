package com.luogh.learning.lab.producer;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ProducerTest {
	private static final Log LOG = LogFactory.getLog(ProducerTest.class);
	
	private ProducerConfig createProducerConfig(){
		Properties props = new Properties();
		props.put("metadata.broker.list", "kafka01:9092,kafka02:9092");
		props.put("serializer.class","kafka.serializer.StringEncoder");
		props.put("partitioner.class","com.lgh.kafka.starter.producer.SimplePartitioner");
		props.put("request.required.acks","1");
		props.put("compression.codec", "gzip");
		props.put("producer.type","async");
		return new ProducerConfig(props);
	}
	
	public void producer() {
		ProducerConfig producerConfig = createProducerConfig();
		Producer<String,String> producer = new Producer<String,String>(producerConfig);
		long events = Long.MAX_VALUE;
		Random rand = new Random();
		for(;;){
			long runtime = new Date().getTime();
			String ip = "192.168.2."+rand.nextInt(255);
			String message = runtime + ",www.baidu.com,"+ ip;
			KeyedMessage<String, String> data = new KeyedMessage<String,String>("page_visits",ip,message);
			producer.send(data);
			LOG.debug("send the message :"+ message);
		}
		
		//producer.close();
	}
	
	public static void main(String[] args) {
		ProducerTest test = new ProducerTest();
		test.producer();
	}
}
