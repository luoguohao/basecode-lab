package com.luogh.learning.lab.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;



public class HighLevelConsumerGroupTest {
	private static final Log LOG = LogFactory.getLog(HighLevelConsumerGroupTest.class);
	
	private final ConsumerConnector consumerConector;
	private final String topic;
	private ExecutorService excutorService;
	
	
	public HighLevelConsumerGroupTest(String zookeeper,String groupId,String topic){
		consumerConector = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(zookeeper,groupId));
		this.topic = topic;
	}
	
	
	public void shutdown(){
		if(this.consumerConector!=null) this.consumerConector.shutdown();
		if(this.excutorService!=null) this.excutorService.shutdown();
		try {
			if(!excutorService.awaitTermination(5000, TimeUnit.MILLISECONDS)){
				LOG.error("Timeout waiting for consumer threads to shut down,exiting unclearly");
			}
		} catch(InterruptedException e){
			LOG.error("Interrpted during shutdown ,exiting unclearly");
		}
	}
	
	
	public void run(int numThreads){
		Map<String,Integer> topicCountMap = new HashMap<String,Integer>();
		topicCountMap.put(this.topic,new Integer(numThreads));
		Map<String,List<KafkaStream<byte[],byte[]>>> consumerMap = this.consumerConector.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[],byte[]>> streams = consumerMap.get(topic);
		
		//launch all the threads
		this.excutorService = Executors.newFixedThreadPool(numThreads);
		
		//create an object to consume the messages
		int threadNumber = 0;
		for(final KafkaStream stream:streams){
			this.excutorService.execute(new HighLevelConsumer(stream,threadNumber));
			threadNumber++;
			LOG.info("-------------> current stream is :"+stream.toString()+" and current threadNumber is :"+threadNumber);
		}
		
	}
	
	
	private static ConsumerConfig createConsumerConfig(String zookeeper,String groupId) {
		Properties props = new Properties();
		props.put("zookeeper.connect", zookeeper); //Kafka uses ZooKeeper to store offsets of messages consumed for a specific topic and partition by this Consumer Group
		props.put("group.id",groupId);
		props.put("zookeeper.session.timeout.ms","400");
		props.put("zookeeper.sync.time.ms", "200");
	    props.put("auto.commit.interval.ms", "1000");
	    //props.put("auto.offset.reset", "smallest");
	    return new ConsumerConfig(props);
	}
	
	
	class HighLevelConsumer implements Runnable{
		
		private KafkaStream stream;
		private int threadNumber;
		
		public HighLevelConsumer(KafkaStream stream,int threadNumber) {
			this.stream = stream;
			this.threadNumber = threadNumber;
			
		}
		public void run() {
			ConsumerIterator<byte[],byte[]> iter = stream.iterator();
			while(iter.hasNext()){
				MessageAndMetadata<byte[],byte[]> mess = iter.next();
				
				LOG.info("Thread Index:"+ threadNumber+ " topic:"+mess.topic()+
											"  partition index:"+mess.partition()+
											"  offset:"+mess.offset()+
											"  key:"+new String(mess.key())+
											"  meesage :"+new String(mess.message()));
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			LOG.info("Shutting down Thread:"+threadNumber);
		}
		
	}
	
	
	
	public static void main(String[] args) {
		String zookeeper = "zoo1:2181,zoo2:2181,zoo3:2181/kafka";
		String groupId = "highLevelGroup";
		String topic = "page_visits";
		int threads = 2;
		HighLevelConsumerGroupTest test = new HighLevelConsumerGroupTest(zookeeper,groupId,topic);
		test.run(threads);
		
		try{
			Thread.sleep(1000*3600*24);
		} catch(InterruptedException e){
			
		}
		test.shutdown();
	}

}


