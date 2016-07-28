package Utils;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.optimize.OptimizeResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequestBuilder;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.node.Node;


public class ESNodeClient implements Serializable  {
	public String index;
	public Node node;
	public Client client;
	public BulkProcessor bulkProcessor;
	public boolean bRouting;
	
	public ESNodeClient(Map<String,String> config) throws IOException{
		
		Settings settings = System.getProperty("file.separator").equals("/") ? ImmutableSettings.settingsBuilder()
					.put("cluster.name",config.get("clusterName"))
					.put("cluster.name",config.get("clusterName"))
					.put("http.enabled", "false")
					.put("transport.tcp.port", config.get("transportcpport"))
					.put("discovery.zen.ping.multicast.enabled", "false")
					.put("discovery.zen.ping.unicast.hosts", config.get("unicast.hosts"))
					//.putArray("discovery.zen.ping.unicast.hosts", "192.168.0.82", "192.168.0.142","192.168.0.94", "192.168.0.238")
					.build() : ImmutableSettings.settingsBuilder()
					.put("http.enabled", false)
					.put("cluster.name",config.get("clusterName"))
					.build();;
					
		 node = nodeBuilder().client(true).settings(settings).node();
		 client = node.client();
		 bulkProcessor = BulkProcessor.builder(
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
			        //.setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB)) 
					.setBulkSize(new ByteSizeValue(Integer.parseInt(config.get("BulkSize")), ByteSizeUnit.MB)) 
			        .setConcurrentRequests(Integer.parseInt(config.get("ConcurrentRequests"))) 
			        .build();
		 
		 if(config.get("routing") == null){
			 this.bRouting = false;
		 }else{
			 this.bRouting = config.get("routing").equals("true") ? true : false;
		 }
	}
	
	/*public ESNodeClient(String Index) throws IOException{
		this.InitiateIndex(Index);
	}*/

	public void InitiateIndex(Map<String,String> configMap) throws IOException{
		this.index = configMap.get("indexName");
		MappingConfig conf = new MappingConfig();
		int shard = Integer.parseInt(configMap.get("number_of_shards"));
		conf.putMapping(this,index,shard);
	}
	
	public void customIndex(String indexname, Map<String,String> configMap){
		
	/*	Settings settings = ImmutableSettings.settingsBuilder()
				.put("refresh_interval", "-1")  // turn off refreshing completely
				.put("number_of_replicas", "0") 
				.put("index.translog.flush_threshold_size", "1g")  //delaying flushes
				.put("index.translog.interval", "30s")  //delaying flushes
				.put("index.warmer.enabled", "false") 
				.put("index.compound_on_flush", "false")  //change segment merge policy at the cost of more file handles
				.put("index.compound_format", "false")    //change segment merge policy at the cost of more file handles
				.build(); */
		
		Settings settings = ImmutableSettings.settingsBuilder()
				.put("refresh_interval", configMap.get("refresh_interval"))  // turn off refreshing completely
				.put("number_of_replicas", Integer.parseInt(configMap.get("number_of_replicas"))) 
				.put("index.translog.flush_threshold_size", configMap.get("index.translog.flush_threshold_size"))  //delaying flushes
				.put("index.translog.interval", configMap.get("index.translog.interval"))  //delaying flushes
				.put("index.warmer.enabled", configMap.get("index.warmer.enabled")) 
				.put("index.compound_on_flush",configMap.get("index.compound_on_flush"))  //change segment merge policy at the cost of more file handles
				.put("index.compound_format", configMap.get("index.compound_format"))    //change segment merge policy at the cost of more file handles
				.put("index.routing.allocation.include.tag", configMap.get("nodetag"))
				.build(); 

				UpdateSettingsRequestBuilder usrb = client.admin().indices() 
				.prepareUpdateSettings(); 
				usrb.setIndices(indexname); 
				usrb.setSettings(settings); 
				usrb.execute().actionGet(); 
	}
	
	public void optimizeIndex(String index){
		  OptimizeResponse response = client.admin().indices() 
			       .prepareOptimize(index) 
			         .setMaxNumSegments(1) 
			         .setFlush(true) 
			         .setOnlyExpungeDeletes(false) 
			       .execute().actionGet();
	}
	
	public void resetIndex(String indexname){
		
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
	
	public void RefreshIndex(){
		node.client().admin().indices().prepareRefresh().execute().actionGet();
	}
	
	/*public static void RefreshIndex(String index){
		node.client().admin().indices().prepareRefresh(index).execute().actionGet();
	}*/
	
	public void openIndex(String indexname){
		client.admin().indices().open(new OpenIndexRequest(indexname));
	}
	
	public void closeIndex(String indexname){
		client.admin().indices().close(new CloseIndexRequest(indexname));
	}
	
	public ArrayList<String> getTypeListWithPrefix(String index, String type_prefix)
	{
		ArrayList<String> type_list = new ArrayList<String>();
		GetMappingsResponse res;
		try {
			res = client.admin().indices().getMappings(new GetMappingsRequest().indices(index)).get();
			ImmutableOpenMap<String, MappingMetaData> mapping  = res.mappings().get(index);
			for (ObjectObjectCursor<String, MappingMetaData> c : mapping) {
				//System.out.println(c.key+" = "+c.value.source());
				if(c.key.startsWith(type_prefix))
				{
					type_list.add(c.key);
				}
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return type_list;	    
	}
	
	public ArrayList<String> getIndexListWithPrefix(String index_predix)
	{
		ArrayList<String> index_list = new ArrayList<String>();
		GetMappingsResponse res;
		res = client.admin().indices().prepareGetMappings(new GetMappingsRequest().indices()).get();
		ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mapping  = res.mappings();
		for (ObjectObjectCursor<String, ImmutableOpenMap<String, MappingMetaData>> c : mapping) {
			//System.out.println(c.key+" = "+c.value.source());
			if(c.key.startsWith(index_predix))
			{
				index_list.add(c.key);
			}
		}
		return index_list;	    
	}
}
