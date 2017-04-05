package com.luogh.learning.lab.producer;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class SimplePartitioner implements Partitioner {

	public SimplePartitioner(VerifiableProperties props) {
		
	}
	
	public int partition(Object key, int numPartition) {
		int partition = 0 ;
		String stringKey = (String)key;
		int offset = stringKey.lastIndexOf(".");
		if(offset>0){
			partition = Integer.parseInt(stringKey.substring(offset+1)) % numPartition;
		}
		return partition;
	}

}
