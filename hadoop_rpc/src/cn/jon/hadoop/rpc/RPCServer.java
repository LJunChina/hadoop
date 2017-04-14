package cn.jon.hadoop.rpc;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;

public class RPCServer implements MyBizable {

	@Override
	public String doSomething(String str) {
		return str;
	}
	/**
	 * @param args
	 * @throws Exception 
	 * @throws  
	 */
	public static void main(String[] args) throws  Exception {
		Server server = new RPC.Builder(new Configuration())		
		.setProtocol(MyBizable.class)
		.setInstance(new RPCServer())
		.setBindAddress("192.168.8.100")
		.setPort(8077)
		.build();
		server.start();
	}

}
