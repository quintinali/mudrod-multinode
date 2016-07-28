package MUDROD.Datamining.Metadata;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.suggest.SuggestRequestBuilder;
import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionFuzzyBuilder;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import MUDROD.Datamining.DataMiner;
import MUDROD.Datamining.intergration.LinkageGenerator;
import Utils.DeleteRedType;
import Utils.ESNode;


public class ESdriver {

   public  String index = "metadata";
    String completionType = "completion";
  public  String contentType = "metadataSource";
    final Integer MAX_CHAR = 500;
    
    String SessionType = "sessionstats";
		
	
    public ESdriver(){
    	
    	
    }
    
	
    public void putMapping() throws IOException{

		boolean exists = ESNode.client.admin().indices().prepareExists(index).execute().actionGet().isExists();
		if(exists){
			return;
		}
		
        String settings_json = "{\r\n    \"analysis\": {\r\n      \"filter\": {\r\n        \"cody_stop\": {\r\n          \"type\":        \"stop\",\r\n          \"stopwords\": \"_english_\"  \r\n        },\r\n        \"cody_stemmer\": {\r\n          \"type\":       \"stemmer\",\r\n          \"language\":   \"light_english\" \r\n        }       \r\n      },\r\n      \"analyzer\": {\r\n        \"cody\": {\r\n          \"tokenizer\": \"standard\",\r\n          \"filter\": [ \r\n            \"lowercase\",\r\n            \"cody_stop\",\r\n            \"cody_stemmer\"\r\n          ]\r\n        }\r\n      }\r\n    }\r\n  }";		
		
        //String mapping_json = "{\r\n      \"_default_\": {\r\n         \"properties\": {            \r\n            \"fullName\": {\r\n                \"type\": \"string\",\r\n               \"index\": \"not_analyzed\"\r\n            },\r\n            \"shortName\": {\r\n                \"type\" : \"string\", \r\n                \"analyzer\": \"cody\"\r\n            },\r\n            \"name_suggest\" : {\r\n                \"type\" :  \"completion\"\r\n            },\r\n            \"content\": {\r\n               \"type\": \"string\",\r\n               \"analyzer\": \"cody\"\r\n            },\r\n            \"metaAuthor\": {\r\n               \"type\": \"string\",\r\n               \"analyzer\": \"cody\"\r\n            },\r\n            \"metaContent\": {\r\n               \"type\": \"string\",\r\n               \"analyzer\": \"cody\"\r\n            }\r\n         }\r\n      }\r\n   }";        
		String mapping_json = "{\r\n      \"_default_\": {\r\n         \"properties\": {            \r\n            \"shortName\": {\r\n                \"type\" : \"string\", \r\n                \"analyzer\": \"cody\"\r\n            },\r\n            \"longName\": {\r\n                \"type\" : \"string\", \r\n                \"analyzer\": \"cody\"\r\n            },\r\n            \"keyword\": {\r\n               \"type\": \"string\",\r\n               \"analyzer\": \"cody\"\r\n            },\r\n            \"term\": {\r\n               \"type\": \"string\",\r\n               \"analyzer\": \"cody\"\r\n            },\r\n            \"topic\": {\r\n               \"type\": \"string\",\r\n               \"analyzer\": \"cody\"\r\n            },\r\n            \"variable\": {\r\n               \"type\": \"string\",\r\n               \"analyzer\": \"cody\"\r\n            },\r\n             \"abstract\": {\r\n               \"type\": \"string\",\r\n               \"analyzer\": \"cody\"\r\n            },\r\n            \"name_suggest\" : {\r\n                \"type\" :  \"completion\"\r\n            }\r\n         }\r\n      }\r\n   } ";
        //set up mapping
		ESNode.client.admin().indices().prepareCreate(index).setSettings(ImmutableSettings.settingsBuilder().loadFromSource(settings_json)).execute().actionGet();
		ESNode.client.admin().indices()
								.preparePutMapping(index)
						        .setType("_default_")				            
						        .setSource(mapping_json)
						        .execute().actionGet();
	}

	
	
    public void addCompletion() throws IOException{
		SearchResponse sr = ESNode.client.prepareSearch("podaacsession")                          
				.setTypes(SessionType+"01",SessionType+"02",SessionType+"04",SessionType+"05",SessionType+"06",SessionType+"07",
						SessionType+"08",SessionType+"09",SessionType+"10",SessionType+"11",SessionType+"12")
				.setQuery(QueryBuilders.matchAllQuery())
				.setSize(0)
				.addAggregation(AggregationBuilders.terms("IPs")
						.field("keywords").size(0))  //important to set size 0, sum_other_doc_count
				.execute().actionGet();
		Terms IPs = sr.getAggregations().get("IPs");
		for (Terms.Bucket entry : IPs.getBuckets()) {
			IndexRequest ir = new IndexRequest(index, completionType).source(jsonBuilder()
					 .startObject()	
					 	.field("name_suggest", entry.getKey())
						
					 .endObject());
			
			ESNode.bulkProcessor.add(ir);
		} 
	}
	
	public void importToES(String fileName) throws IOException, InterruptedException, JSONException{
		
		DeleteRedType drt = new DeleteRedType();
		drt.deleteAllByQuery(index, contentType, QueryBuilders.matchAllQuery());
		ESNode.RefreshIndex();
		
		BufferedReader br = new BufferedReader(new FileReader(fileName));
		int count =0;

		try {
			String line = br.readLine();
		    while (line != null) {
		    	JSONObject jsonData = new JSONObject(line);
		    	
		    	IndexRequest ir = new IndexRequest(index, contentType).source(jsonBuilder()
						 .startObject()	
						 	.field("shortName", jsonData.getString("shortName"))
							.field("longName", jsonData.getString("longName"))
							.field("keyword", jsonData.getString("keywordStr"))
							.field("term", jsonData.getString("termStr"))
							.field("topic", jsonData.getString("topicStr"))
							.field("variable", jsonData.getString("variableStr"))
							
						/*	.field("isotopic", jsonData.getString("isotopic"))
							.field("sensor", jsonData.getString("sensor"))
							.field("source", jsonData.getString("source"))
							.field("project", jsonData.getString("project"))*/
							
							.field("abstract", URLDecoder.decode(jsonData.getString("abstractStr"), "UTF-8"))
						 .endObject());
		    	
	    	
		    	ESNode.bulkProcessor.add(ir);
		    	
		    	line = br.readLine();
		    	count++;
		    }
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
		    br.close();
		    //bulkProcessor.close();
		    System.out.print(count);
		    System.out.print("\ndone");
		}
	}
	
	public String searchByQuery(String query) throws IOException, InterruptedException, ExecutionException{
		boolean exists = ESNode.node.client().admin().indices().prepareExists(index).execute().actionGet().isExists();	
		if(!exists){
			return null;
		}
		
		DataMiner prep = new DataMiner();
		LinkageGenerator lg= new LinkageGenerator(prep);
		
		String semantic_query = lg.TransformQuerySem(query, 3);
		
		QueryBuilder qb = QueryBuilders.queryStringQuery(semantic_query); 
		SearchResponse response = ESNode.client.prepareSearch(index)
		        .setTypes(contentType)		        
		        .setQuery(qb)
		        .setSize(500)
		        .execute()
		        .actionGet();
        
        Gson gson = new Gson();		
        List<JsonObject> fileList = new ArrayList<JsonObject>();
        float highest_relevance = -9999;
        for (SearchHit hit : response.getHits().getHits()) {
        	Map<String,Object> result = hit.getSource();
        	float relevance = hit.getScore();
        	if(highest_relevance == -9999){
        		highest_relevance = hit.getScore();
        	}
        	String shortName = (String) result.get("shortName");
        	String longName = (String) result.get("longName");
        	String keyword = (String) result.get("keyword");
        	String content = (String) result.get("abstract");
        	
        	/*int maxLength = (content.length() < MAX_CHAR)?content.length():MAX_CHAR;
        	content = content.trim().substring(0, maxLength-1) + "...";*/
        	
        	JsonObject file = new JsonObject();
        	file.addProperty("Relevance", String.format("%.2f", relevance/highest_relevance * 100) + "%");
    		file.addProperty("Short Name", shortName);
    		file.addProperty("Long Time", longName);
    		file.addProperty("Keyword", keyword);
    		file.addProperty("Abstract", content);
    		fileList.add(file);       	
        	          
        }
        JsonElement fileList_Element = gson.toJsonTree(fileList);
        
        JsonObject PDResults = new JsonObject();
        PDResults.add("PDResults", fileList_Element);
		System.out.print("Search results returned." + "\n");
		return PDResults.toString();
	}
	
	public List<String> autoComplete(String chars){
		boolean exists = ESNode.node.client().admin().indices().prepareExists(index).execute().actionGet().isExists();	
		if(!exists){
			return null;
		}
		
		List<String> SuggestList = new ArrayList<String>();
		
		CompletionSuggestionFuzzyBuilder suggestionsBuilder = new CompletionSuggestionFuzzyBuilder("completeMe");
	    suggestionsBuilder.text(chars);
	    suggestionsBuilder.size(10);
	    suggestionsBuilder.field("name_suggest");
	    suggestionsBuilder.setFuzziness(Fuzziness.fromEdits(2));  
	    
	    SuggestRequestBuilder suggestRequestBuilder =
	    		ESNode.client.prepareSuggest(index).addSuggestion(suggestionsBuilder);


	    SuggestResponse suggestResponse = suggestRequestBuilder.execute().actionGet();

	    Iterator<? extends Suggest.Suggestion.Entry.Option> iterator =
	            suggestResponse.getSuggest().getSuggestion("completeMe").iterator().next().getOptions().iterator();

	    while (iterator.hasNext()) {
	        Suggest.Suggestion.Entry.Option next = iterator.next();
	        SuggestList.add(next.getText().string());
	    }
	    return SuggestList;
		
	}
	
	
	

	
	public static void main(String[] args) throws IOException, InterruptedException, JSONException {
		// TODO Auto-generated method stub
		ESdriver esd = new ESdriver();
		/*esd.putMapping();
		esd.importToES();*/
		
		esd.addCompletion();
		
		ESNode.bulkProcessor.awaitClose(20, TimeUnit.MINUTES);
		ESNode.node.close();
	

	}

}

