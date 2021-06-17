package wiki.hadoop.es;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;


import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * ES Cleint class
 */
public class ESClient {

	public static RestHighLevelClient client;
	
	private static final Log log = LogFactory.getLog(ESClient.class);
	/**
	 * init ES client
	 */
	public static void initEsClient() throws UnknownHostException {
		log.info("初始化es连接开始");
		
		System.setProperty("es.set.netty.runtime.available.processors", "false");
		
		Settings esSettings = Settings.builder()
				.put("cluster.name", "APP_ELK_UAT")//设置ES实例的名称
				.put("client.transport.sniff", true)
				.build();
		
//		client = new  PreBuiltTransportClient(esSettings)
//				.addTransportAddress(new TransportAddress(InetAddress.getByName("10.239.160.127"), 9300))
//				.addTransportAddress(new TransportAddress(InetAddress.getByName("10.239.160.128"), 9300))
//				.addTransportAddress(new TransportAddress(InetAddress.getByName("10.239.160.129"), 9300))
//				.addTransportAddress(new TransportAddress(InetAddress.getByName("10.239.160.120"), 9300))
//				.addTransportAddress(new TransportAddress(InetAddress.getByName("10.239.160.121"), 9300))
//				.addTransportAddress(new TransportAddress(InetAddress.getByName("10.239.160.122"), 9300))
//
//		;

		 client = new RestHighLevelClient(
				RestClient.builder(
						new HttpHost("10.239.160.127", 9200, "http"),
						new HttpHost("10.239.160.128", 9200, "http"),
						new HttpHost("10.239.160.129", 9200, "http"),
						new HttpHost("10.239.160.120", 9200, "http"),
						new HttpHost("10.239.160.121", 9200, "http"),
						new HttpHost("10.239.160.122", 9200, "http")
				));
		
		log.info("初始化es连接完成");

	}

	/**
	 * Close ES client
	 */
	public static void closeEsClient() throws IOException {
		client.close();
		log.info("es连接关闭");
	}
}
