package cn.jon.hadoop.rpc;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class RPCClient {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		MyBizable proxy = RPC.getProxy(MyBizable.class, 123456,new InetSocketAddress("192.168.8.100", 8077) , new Configuration());
		String result = proxy.doSomething("·þÎñ¶Ë");
		System.out.println(result);
		RPC.stopProxy(proxy);
	}

}
