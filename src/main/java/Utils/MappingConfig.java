package Utils;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;



import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class MappingConfig{

	public void putMapping(ESNodeClient esnode, String index, int shard) throws IOException {

		boolean exists = esnode.client.admin().indices().prepareExists(index).execute().actionGet().isExists();
		if(exists){
			return;
		}

 		//set up dynamic setting, can be updated with close/ open function
		esnode.client.admin().indices().prepareCreate(index).setSettings(ImmutableSettings.settingsBuilder().loadFromSource(jsonBuilder()
                .startObject()
                		//.field("indices.memory.index_buffer_size", "50%")
						.field("number_of_shards", shard) 
						//.field("indices.store.throttle.type", "none") 
	                .startObject("analysis")
		                .startObject("filter")
		                	.startObject("cody_stop")
					              .field("type", "stop")
								  .field("stopwords", "_english_")
					           .endObject()
					           .startObject("cody_stemmer")
						              .field("type", "stemmer")
									  .field("stopwords", "light_english")
							   .endObject()
					    .endObject()
	                    .startObject("analyzer")
	                        .startObject("csv")
						              .field("type", "pattern")
									  .field("pattern", ",")
						     .endObject()
	                         .startObject("cody")
						              .field("tokenizer", "standard")
						              .field("filter", new String[]{"lowercase", "cody_stop", "cody_stemmer"})
						     .endObject()
	                    .endObject()
	                .endObject()
            .endObject().string())).execute().actionGet();
		
		
		//set up mapping
		XContentBuilder SessionMapping =  jsonBuilder()
									.startObject()
											.startObject("_default_")
												/*.startObject("_routing")
													.field("required", "true")
													.field("path", "_routing")
												.endObject()*/
												.startObject("properties")
													.startObject("LogType")
														.field("type", "string")
														.field("index", "not_analyzed")
													.endObject()
													.startObject("IP")
														.field("type", "string")
														.field("index", "not_analyzed")
													.endObject()
													.startObject("Browser")
														.field("type", "string")
														.field("index", "not_analyzed")
													.endObject()
													.startObject("Referer")
														.field("type", "string")
														.field("index", "not_analyzed")
													.endObject()
													.startObject("SessionID")
														.field("type", "string")
														.field("index", "not_analyzed")
													.endObject()
													.startObject("Response")
														.field("type", "string")
														.field("index", "not_analyzed")
													.endObject()
													.startObject("Request")
														.field("type", "string")
														.field("index", "not_analyzed")
													.endObject()
													.startObject("RequestUrl")
														.field("type", "string")
														.field("index", "not_analyzed")
													.endObject()
													.startObject("keywords")
														.field("type", "string")
														.field("index_analyzer", "csv")														
													.endObject()
													.startObject("views")
														.field("type", "string")
														.field("index_analyzer", "csv")	
													.endObject()
													.startObject("downloads")
														.field("type", "string")
														.field("index_analyzer", "csv")	
													.endObject()
													.startObject("Coordinates")
														.field("type", "geo_point")
													.endObject()
												.endObject()
											.endObject()
									.endObject();
		

		esnode.client.admin().indices()
							  .preparePutMapping(index)
                              .setType("_default_")
                              .setSource(SessionMapping)
                              .execute().actionGet();

	}
	
}