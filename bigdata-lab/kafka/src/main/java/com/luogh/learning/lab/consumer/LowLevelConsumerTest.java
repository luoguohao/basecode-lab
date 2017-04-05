package com.luogh.learning.lab.consumer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * The main reason to use a SimpleConsumer implementation is you want greater control over partition consumption than Consumer Groups give you.
	For example you want to:
		1)Read a message multiple times
		2)Consume only a subset of the partitions in a topic in a process
		3)Manage transactions to make sure a message is processed once and only once
		
	Downsides of using SimpleConsumer
		The SimpleConsumer does require a significant amount of work not needed in the Consumer Groups:
			1)You must keep track of the offsets in your application to know where you left off consuming.
			2)You must figure out which Broker is the lead Broker for a topic and partition
			3)You must handle Broker leader changes
			
	Steps for using a SimpleConsumer
		1)Find an active Broker and find out which Broker is the leader for your topic and partition
		2)Determine who the replica Brokers are for your topic and partition
		3)Build the request defining what data you are interested in
		4)Fetch the data
		5)Identify and recover from leader changes
 * 
 * @author Kaola
 *
 */
public class LowLevelConsumerTest {

	private static final Log LOG = LogFactory.getLog(LowLevelConsumerTest.class);
	
	private List<String> replicaBrokers = new ArrayList<String>();
	
	
	/**
	 * Finding the Lead Broker for a Topic and Partition
	 */
	private PartitionMetadata findLeader(List<String> seedBrokers,int port,String topic,int partition) {
		PartitionMetadata metaData = null;
		loop:
		for(String seedBroker : seedBrokers) {
			SimpleConsumer consumer = null;
			try {
				consumer = new SimpleConsumer(seedBroker,port,100000,64*1024,"leaderLookup");
				
				List<String> topics = Collections.singletonList(topic);
				TopicMetadataRequest request = new TopicMetadataRequest(topics);
				TopicMetadataResponse response = consumer.send(request);
				
				List<TopicMetadata> metadatas = response.topicsMetadata();
				
				for(TopicMetadata metadata : metadatas){
					for(PartitionMetadata partionMetadata : metadata.partitionsMetadata()){
						if(partionMetadata.partitionId() == partition){
							metaData = partionMetadata;
							break loop;
						}
					}
				}
			} catch(Exception e){
				LOG.error("Error Communicating with Broker ["+seedBroker+"] to find Leader for ["+topic+","+partition+" ] Reason :"+ e);
			} finally {
				if(consumer!=null) consumer.close();
			}
		}
		if(metaData!=null){
			replicaBrokers.clear();
			for(Broker replica : metaData.replicas()){
				replicaBrokers.add(replica.host());
			}
		}
		return metaData;
	}
	
	
	private String findNewLeader(String oldLeader,String topic,int partition,int port) throws Exception {
		for(int i = 0;i < 3;i++) {
			boolean goToSleep = false;
			PartitionMetadata metadata = findLeader(replicaBrokers,port,topic,partition);
			if(metadata == null) {
				goToSleep = true;
			} else if (metadata.leader() == null) {
				goToSleep = true;
			} else if (oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
				 // first time through if the leader hasn't changed give ZooKeeper a second to recover
                // second time, assume the broker did recover before failover, or it was a non-Broker issue
				goToSleep = true;
			} else {
				return metadata.leader().host();
			}
			if(goToSleep){
				try {
					Thread.sleep(1000);
				} catch (Exception e) {
				}
			}
		}
		LOG.error("Unable to find new leader after Broker failure . Exiting");
		throw new Exception("Unable to find new Leader after Broker failure. Exiting");
	}
	/**
	 * finding starting offset for reads
	 */
	public static long getLastOffset(SimpleConsumer consumer,String topic,int partition,long whichTime,String clientName){
		TopicAndPartition topicAndPartion = new TopicAndPartition(topic,partition);
		Map<TopicAndPartition,PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition,PartitionOffsetRequestInfo>();
		/**
		 * PartitionOffsetRequestInfo �е� �����ֶκ��壺
		 * 	 time�ֶ�
		 *   ������ kafka.api.OffsetRequest.LatestTime()��kafka.api.OffsetRequest.EarliestTime()��������
		 *   �Զ����ʱ����������֣����ֶα�ʾ��ȡ��ǰtopicָ��partition�����µ�offset�������offset���Լ���Ӧpartition����ʱ��С�ڵ��ڵ�ǰָ��ʱ�����
		 *   Segment�ļ�����ʼoffset��
		 *   maxNumOffsets�ֶ�
		 *   ���ڵ���0��maxNumOffsets��ʾȡ�����ֶ�time�ĸ���segment����ʼoffset�ĸ���
		 *   
		 *   ��kafka�ڲ���ʵ����ά����һ��topicָ��partition������segment����ʼoffset�͸���segment��Ӧ��������ʱ����б��Լ���ǰpartition
		 *   ���µ�offset�Լ�����Ӧ�ĸ���ʱ��(��Ϊ��ǰʱ�� System.currentTimeMillis())�� ���������partition������segment,�ṹ���£�
		 *      	��־����			  ����ʱ��	
		 *   	00000000000.log		1458271111 
		 *   	00000010112.log		1458292222 
		 *      00000020224.log		1458373333 
		 *   ��ô���б�OffsetArr�Ľṹ���£�
		 *   [ 0->1458271111 , 10112->1458292222 , 20224->1458373333 , 20555->1458374444 ]
		 *   �����б������һ��Ԫ�ص�key:20555��ʾ��ǰ���µ�offset,value��ʾ��ǰ����offset��Ӧ��ʱ���(Ϊ��ǰʱ��).
		 *   
		 *   1)��PartitionOffsetRequestInfo��time=OffsetRequest.LatestTime(),��maxNumOffsets=1:
		 *   	OffsetResponse���ص�offset��ΪOffsetArr�����һ��Ԫ�ؼ���ǰ���µ�Offset:[20555].
		 *   2)��PartitionOffsetRequestInfo��time=OffsetRequest.EarliestTime()����maxNumOffsets=1:
		 *   	OffsetResponse���ص�offset��ΪOffsetArr�ĵ�һ��Ԫ�ؼ�  [0].
		 *   3)��PartitionOffsetRequestInfo��time=����ʱ���ֵ(���赱ǰΪ1458373311)����maxNumOffsets=1:
		 *   	OffsetResponse���ص�offset��ΪoffsetArr�д�������Ƚ�time�ֶ�С�ڵ���1458373311��һ��offset(��Ϊ�˴�maxNumoffsets=1,����ֻ��Ҫȡһ��),
		 *      �˴���Ϊ10112,����OffsetResponse���ص�offset��Ϊ [10112].
		 *   
		 *   
		 *   1)��PartitionOffsetRequestInfo��time=OffsetRequest.LatestTime(),��maxNumOffsets=2:
		 *   	OffsetResponse���ص�offset��ΪOffsetArr�����һ��Ԫ�ؼ���ǰ���µ�Offset:20555�Լ������ڶ���Ԫ��Offset:20224,
		 *   	���������� [20555,20224]
		 *   2)��PartitionOffsetRequestInfo��time=OffsetRequest.EarliestTime()����maxNumOffsets=2:
		 *   	OffsetResponse���ص�offset��ΪOffsetArr�ĵ�һ��Ԫ�ؼ� 0,����������ȻmaxNumOffsets=2,��ֻ��OffsetArr��һ��Ԫ�أ�
		 *   	���������� [0]
		 *   3)��PartitionOffsetRequestInfo��time=����ʱ���ֵ(���赱ǰΪ1458373311)����maxNumOffsets=2:
		 *   	OffsetResponse���ص�offset��ΪoffsetArr�д�������Ƚ�time�ֶ�С�ڵ���1458373311��һ��offset,
		 *      �˴���Ϊ10112,ͬʱmaxNumOffsets=2,��ʱ��������ȡ��
		 *      ���������� [10112,0]
		 *      
		 *   ��ϸ���е��ʼǣ�A Closer Look at Kafka OffsetRequest
		 */
		requestInfo.put(topicAndPartion,new PartitionOffsetRequestInfo(whichTime,1));
//		requestInfo.put(topicAndPartion,new PartitionOffsetRequestInfo(System.currentTimeMillis()-10000,1));
		OffsetRequest request = new OffsetRequest(requestInfo,kafka.api.OffsetRequest.CurrentVersion(),clientName);
		OffsetResponse response = consumer.getOffsetsBefore(request);
		
		if(response.hasError()){
			LOG.error("Error fetching data offset data from the broker. Reason : "+response.errorCode(topic, partition));
			return 0;
		}
		long[] offsets = response.offsets(topic, partition);
		return offsets[0];
	}
	
	
	public void run(long maxReads,String topic,int partition,List<String> seedBrokers,int port) throws Exception{
		//find the meta data about the topic and partition we are interested in 
		PartitionMetadata metadata = findLeader(seedBrokers,port,topic,partition);
		if(metadata == null){
			LOG.error("can`t find metadata fro topic and the partition .exiting");
			return;
		}
		if(metadata.leader()==null){
			LOG.error("can`t find Leader for Topic and Partition . Exiting");
			return;
		}
		String leadBroker = metadata.leader().host();
		String clientName = "Client_01_"+topic+"_"+partition;
		
		SimpleConsumer consumer = new SimpleConsumer(leadBroker,port,10000,64*1024,clientName);
		long readOffset = getLastOffset(consumer, topic, partition, kafka.api.OffsetRequest.LatestTime(), clientName);
		int numErrors = 0;
		while(maxReads > 0){
			LOG.info("current readOffset is :"+readOffset+" and the lastOffset is :"+getLastOffset(consumer,topic,partition,kafka.api.OffsetRequest.LatestTime(),clientName));
			if(consumer == null){
				consumer = new SimpleConsumer(leadBroker,port,10000,64*1024,clientName);
			}
			FetchRequest request = new FetchRequestBuilder()
										.clientId(clientName)
										.addFetch(topic, partition, readOffset, 100000) // Note: this fetchSize of 100000 might need to be increased if large batches are written to Kafka
										.build();
			FetchResponse fetchResponse = consumer.fetch(request);
			
			if(fetchResponse.hasError()){
				numErrors++;
				short code = fetchResponse.errorCode(topic, partition);
				LOG.error("Error fetching data from the Broker:"+leadBroker+" Reason:"+code);
				if(numErrors > 5) break;
				if(code == ErrorMapping.OffsetOutOfRangeCode()){
					// We asked for an invalid offset. For simple case ask for the last element to reset
					LOG.info("the offset we ask is out of range, now we just reset the offset.");
					readOffset = getLastOffset(consumer,topic,partition,kafka.api.OffsetRequest.LatestTime(),clientName);
					continue;
				}
				consumer.close();
				consumer = null;
				leadBroker = findNewLeader(leadBroker,topic,partition,port);
				continue;
			}
			numErrors = 0;
			
			long numRead = 0;
			for(MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partition)){
				long currentOffset = messageAndOffset.offset();
				if(currentOffset < readOffset){
					LOG.error("Found an old offset:"+currentOffset +"Expecting :"+ readOffset);
					continue;
				}
				readOffset = messageAndOffset.nextOffset();
				ByteBuffer payload = messageAndOffset.message().payload();
				byte[] bytes = new byte[payload.limit()];
				payload.get(bytes);
				LOG.info("the message info :numRead : "+numRead+" ,"+String.valueOf(messageAndOffset.offset())+":"+new String(bytes,"UTF-8"));
				numRead++;
				if((--maxReads)<=0){
					LOG.info("we have get all message we wanted . so ,it`s time to stop.");
					break;
				}
			}
			if(numRead == 0) {
				try {
					Thread.sleep(1000);
				} catch (Exception e) {
				}
			}
		}
		LOG.info("all the message has been consumed. and the total read number is :"+maxReads);
		if(consumer != null) consumer.close();
	}
	
	
	public static void main(String[] args) {
		LowLevelConsumerTest test = new LowLevelConsumerTest();
		long maxReads = 10000000000000000L;
		String topic = "basis_msg";
		int partition = 0;
		List<String> seeds = Collections.singletonList("hadoop2");
		int port = 9092;
		try {
			test.run(maxReads, topic, partition, seeds, port);
		} catch (Exception e) {
			LOG.error("Oops:"+e);
		}
	}
}
