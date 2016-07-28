package Utils;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import java.io.IOException;
import java.io.Serializable;

import org.elasticsearch.action.admin.indices.optimize.OptimizeResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequestBuilder;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.node.Node;


public class ESNode implements Serializable  {
	public static String index;

	static Settings settings = 
			System.getProperty("file.separator").equals("/") ? ImmutableSettings.settingsBuilder()
				.put("cluster.name","mudrod")
				.put("http.enabled", "false")
				.put("transport.tcp.port", "9300-9400")
				.put("discovery.zen.ping.multicast.enabled", "false")
				//.put("discovery.zen.ping.unicast.hosts", "localhost")
				.putArray("discovery.zen.ping.unicast.hosts", "192.168.0.82", "192.168.0.142","192.168.0.94", "192.168.0.238")
				.build() : ImmutableSettings.settingsBuilder().put("http.enabled", false).build();

	//public static String clustername = "aist";
	public static String clustername = System.getProperty("file.separator").equals("/") ? "mudrod" : "aist";
	public static Node node = nodeBuilder().client(true).settings(settings).clusterName(clustername).node();
	public static Client client = node.client();
	
	public ESNode() throws IOException{
		this.InitiateIndex("podaacsession");
	}
	
	public ESNode(String Index) throws IOException{
		this.InitiateIndex(Index);
	}

	public void InitiateIndex(String Index) throws IOException{
		this.index = Index;
		MappingConfig conf = new MappingConfig();
		//conf.putMapping(index);
	}
	
	public static void customIndex(String indexname){
		
		Settings settings = ImmutableSettings.settingsBuilder()
				.put("refresh_interval", "-1")  // turn off refreshing completely
				.put("number_of_replicas", "0") 
				.put("index.translog.flush_threshold_size", "1g")  //delaying flushes
				.put("index.translog.interval", "30s")  //delaying flushes
				.put("index.warmer.enabled", "false") 
				.put("index.compound_on_flush", "false")  //change segment merge policy at the cost of more file handles
				.put("index.compound_format", "false")    //change segment merge policy at the cost of more file handles
				//.put("index.translog.durability", "async")  //20% of the total heap allocated to a node will be used as the indexing buffer size
				.build(); 

				UpdateSettingsRequestBuilder usrb = client.admin().indices() 
				.prepareUpdateSettings(); 
				usrb.setIndices(indexname); 
				usrb.setSettings(settings); 
				usrb.execute().actionGet(); 
	}
	
	public static void optimizeIndex(String index){
		  OptimizeResponse response = client.admin().indices() 
			       .prepareOptimize(index) 
			         .setMaxNumSegments(1) 
			         .setFlush(true) 
			         .setOnlyExpungeDeletes(false) 
			       .execute().actionGet();
	}
	
	public static void resetIndex(String indexname){
		
		Settings settings = ImmutableSettings.settingsBuilder()
				.put("refresh_interval", "-1") 
				.put("number_of_replicas", "1") 
				//.put("index.translog.flush_threshold_size", "1g") 
				//.put("index.translog.interval", "1m") 
				.build(); 

		UpdateSettingsRequestBuilder usrb = client.admin().indices() 
				.prepareUpdateSettings(); 
				usrb.setIndices(indexname); 
				usrb.setSettings(settings); 
				usrb.execute().actionGet(); 
	}
	
	public static void RefreshIndex(){
		node.client().admin().indices().prepareRefresh().execute().actionGet();
	}
	
	/*public static void RefreshIndex(String index){
		node.client().admin().indices().prepareRefresh(index).execute().actionGet();
	}*/
	
	public static BulkProcessor bulkProcessor = BulkProcessor.builder(
			client,
			new BulkProcessor.Listener() {
	            public void beforeBulk(long executionId,
	                                   BulkRequest request) {/*System.out.println("New request, you can do it!");*/} 

	            public void afterBulk(long executionId,
	                                  BulkRequest request,
	                                  BulkResponse response) {/*System.out.println("Well done");*/} 

	            public void afterBulk(long executionId,
	                                  BulkRequest request,
	                                  Throwable failure) {
	            	throw new RuntimeException("Caught exception in bulk: " + request + ", failure: " + failure, failure);
	            } 
	        }
	        )
	        //.setBulkActions(500) 
	        .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB)) 
			//.setBulkSize(new ByteSizeValue(100, ByteSizeUnit.MB)) 
	        .setConcurrentRequests(8) 
	        .build();
}
