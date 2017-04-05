package com.luogh.learning.lab.server;

import com.luogh.learning.lab.services.Hello;
import com.luogh.learning.lab.services.HelloImpl;
import com.twitter.finagle.ListeningServer;
import com.twitter.finagle.Thrift;
import com.twitter.util.Await;

import java.net.InetSocketAddress;

/**
 * Created by Kaola on 2016/4/1.
 */
public class AppServerMain {
	static ListeningServer server;

	public static void main(String args[]){
		Integer port = 9802; // finagle 真正的监听地址
		String zkHosts = "zk1:2181,zk2:2181,zk3:2181"; // zk servers
		String zkPath = "/soa/test/finagle";

		System.out.println("zkHosts:"+zkHosts +"\t zkPath:"+zkPath+" \t tport:"+port);
		Hello.ServiceIface iface = new HelloImpl();
		server = Thrift.serveIface(new InetSocketAddress(port),iface);

		String zkFullPath = String.format("zk!%s!%s!0",zkHosts,zkPath);
		System.out.println("zkFullPath:"+zkFullPath);
		server.announce(zkFullPath);

		System.out.println("finagle server start");

		Runtime.getRuntime().addShutdownHook(new Thread(){
			@Override
			public void run(){
				AppServerMain.close();
			}
		});

		try {
			Await.ready(server);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}


	public static void close(){
		System.out.println("finagle server shutdown");
		server.close();
	}
}
