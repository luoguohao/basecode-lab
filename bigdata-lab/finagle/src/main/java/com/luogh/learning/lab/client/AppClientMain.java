package com.luogh.learning.lab.client;

import com.luogh.learning.lab.services.Hello;
import com.twitter.finagle.ServiceFactory;
import com.twitter.finagle.Thrift;
import com.twitter.finagle.thrift.ThriftClientRequest;
import com.twitter.util.Duration;
import com.twitter.util.Try;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.Assert;

/**
 * Created by Kaola on 2016/4/1.
 */
public class AppClientMain {

	public static void main(String args[]) {
		String zkHosts = "zk1:2181,zk2:2181,zk3:2181";
		String zkPath = "/soa/test/finagle";

		System.out.println("zkHosts:"+zkHosts +"\t zkPath:"+zkPath);

		String zkFullPath = String.format("zk!%s!%s",zkHosts,zkPath);
		ServiceFactory<ThriftClientRequest,byte[]> factory = (ServiceFactory<ThriftClientRequest, byte[]>) Thrift.newClient(zkFullPath);

		for( int i=0;i<100;i++){
			Hello.ServiceIface helloClient = new Hello.ServiceToClient(factory.toService(),new TBinaryProtocol.Factory());
			Try<String> ret = helloClient.helloString("test").get(Duration.fromSeconds(3));
			Assert.assertEquals("test", ret.get());
			System.out.println("invoke service complete times:"+i);
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}
}
