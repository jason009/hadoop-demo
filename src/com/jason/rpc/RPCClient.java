package com.jason.rpc;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class RPCClient {

	public static void main(String[] args) throws Exception {
		Bizable proxy = RPC.getProxy(Bizable.class, 10010, new InetSocketAddress("192.168.8.100",9527), new Configuration());
		String resualt = proxy.sayHi("tomcat");
		System.out.println(resualt);
	}

}
