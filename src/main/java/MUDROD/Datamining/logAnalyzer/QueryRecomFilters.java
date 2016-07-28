package MUDROD.Datamining.logAnalyzer;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortOrder;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import MUDROD.Datamining.DataMiner;
import Utils.ESNode;
import Utils.StringTool;

public class QueryRecomFilters {
	DataMiner prep = null;
	public String index ="";
	public String SessionType = null;
	public String queryFilterType = "queryFilter";
	public String queryFilterRuleType = "queryFilterRule";
	
	public QueryRecomFilters(DataMiner test) throws IOException
	{
		prep = test;
		index = test.index;
		SessionType = test.SessionStats;
		putMapping();
	}
	
	 public void putMapping() throws IOException{
	    	XContentBuilder Mapping =  jsonBuilder()
					.startObject()
						.startObject(queryFilterType)
							.startObject("properties")
								.startObject("Query_A")
									.field("type", "string")
									.field("index", "not_analyzed")
								.endObject()
								.startObject("Filter_B")
									.field("type", "string")
									.field("index", "not_analyzed")
								.endObject()
							.endObject()
						.endObject()
					.endObject();

	    	ESNode.client.admin().indices()
			  .preparePutMapping(this.index)
	          .setType(queryFilterType)
	          .setSource(Mapping)
	          .execute().actionGet();
	        
	    	XContentBuilder Mapping2 =  jsonBuilder()
					.startObject()
						.startObject(queryFilterRuleType)
							.startObject("properties")
								.startObject("Query_A")
									.field("type", "string")
									.field("index", "not_analyzed")
								.endObject()
								.startObject("Filter_B")
									.field("type", "string")
									.field("index", "not_analyzed")
								.endObject()
							.endObject()
						.endObject()
					.endObject();

	    	ESNode.client.admin().indices()
			  .preparePutMapping(this.index)
	          .setType(queryFilterRuleType)
	          .setSource(Mapping2)
	          .execute().actionGet();
	}

	
	
	public void associationRule(int type) throws IOException{
		SearchResponse sr = ESNode.client.prepareSearch(prep.index)
				.setTypes(queryFilterType)
				.setScroll(new TimeValue(60000))
				.setQuery(QueryBuilders.matchAllQuery())
				.setSize(0)
				.addAggregation(AggregationBuilders.terms("by_KeyA")
				           .field("Query_A").size(0).subAggregation(AggregationBuilders.terms("by_KeyB")
						           .field("Filter_B").size(0)))
				.execute().actionGet();
		
		Terms KeyAs = sr.getAggregations().get("by_KeyA");
		Map<String, Boolean> existingRules = new HashMap<String, Boolean>();
		
		for (Terms.Bucket KeyA : KeyAs.getBuckets()) {
			String A = KeyA.getKey();                    // bucket key
			long ACount = 0;
			
			FilterBuilder type_filter = FilterBuilders.boolFilter()
					.must(FilterBuilders.termFilter("Query_A",A))
					.must(FilterBuilders.termFilter("Type",type));
			QueryBuilder type_query_search = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), type_filter);

			SearchResponse typescrollResp = ESNode.client.prepareSearch(prep.index)
					.setTypes(queryFilterType).setScroll(new TimeValue(60000)).setQuery(type_query_search).setSize(100)
					.execute().actionGet();

			while (true) {
				for (SearchHit hit : typescrollResp.getHits().getHits()) {
					Map<String, Object> result = hit.getSource();
					double weights = (double) result.get("Weight");
					ACount +=  weights;
				}
				
				typescrollResp = ESNode.client.prepareSearchScroll(typescrollResp.getScrollId())
						.setScroll(new TimeValue(600000)).execute().actionGet();
				if (typescrollResp.getHits().getHits().length == 0) {
					break;
				}
			}
			
			Terms KeyBs = KeyA.getAggregations().get("by_KeyB");
			for (Terms.Bucket KeyB : KeyBs.getBuckets()) {
				String B = KeyB.getKey();                    // bucket key
			    DateTime dt = new DateTime();
			    double BCount = 0.0;
				FilterBuilder filter_search = FilterBuilders.boolFilter()
						.must(FilterBuilders.termFilter("Query_A",A))
						.must(FilterBuilders.termFilter("Filter_B",B))
						.must(FilterBuilders.termFilter("Type",type));
				QueryBuilder query_search = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), filter_search);

				SearchResponse scrollResp = ESNode.client.prepareSearch(prep.index)
						.setTypes(queryFilterType).setScroll(new TimeValue(60000)).setQuery(query_search).setSize(100)
						.execute().actionGet();

				while (true) {
					for (SearchHit hit : scrollResp.getHits().getHits()) {
						Map<String, Object> result = hit.getSource();
						double weights = (double) result.get("Weight");
						BCount +=  weights;
					}
					
					scrollResp = ESNode.client.prepareSearchScroll(scrollResp.getScrollId())
							.setScroll(new TimeValue(600000)).execute().actionGet();
					// Break condition: No hits are returned
					if (scrollResp.getHits().getHits().length == 0) {
						break;
					}
				}

			    double conf = (double)BCount/ACount;
			    
		
			    //put a list in memory here
			    
			    if(existingRules == null || existingRules.get(B+","+A) == null){
			    	existingRules.put(A+","+B, true);

			    IndexRequest ir = new IndexRequest(prep.index, queryFilterRuleType).source(jsonBuilder()
						 .startObject()	
						 .field("Time", dt)
						 .field("Query_A", A)
						 .field("Filter_B", B)
						 .field("Support_A", ACount)
						 .field("Confidence", conf)
						 .field("Count", BCount)
						 .field("Type", type)
						 .endObject());

			    ESNode.bulkProcessor.add(ir);
			    }
			}
		}	
	}
	
	public void exportRules(int type, String output) throws IOException{
		File file = new File(output);
		if (!file.exists()) {
			file.createNewFile();
		}

		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		
		FilterBuilder filter_search = FilterBuilders.boolFilter()
				.must(FilterBuilders.termFilter("Type",type));
		QueryBuilder query_search = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), filter_search);

	    SearchResponse scrollResp = ESNode.client.prepareSearch(prep.index)
	    		.setTypes(queryFilterRuleType)
	            .setScroll(new TimeValue(60000))
	            .setQuery(query_search)
	            .setSize(100).execute().actionGet(); 

	    while (true) {
	    	for (SearchHit hit : scrollResp.getHits().getHits()) {
	    		Map<String,Object> result = hit.getSource();     		
				String A = (String) result.get("Query_A");
				String B = (String) result.get("Filter_B");
				Integer Supp = (Integer) result.get("Support_A");
				Object Conf_o = result.get("Confidence");
				Double Conf = new Double(Conf_o.toString());
				double Count = (double) result.get("Count");
				DecimalFormat numberFormat = new DecimalFormat("#.00");
				
				if(Count>=1)
				{
				bw.write( "\"" + A + "\"" + ","+  "\"" + B + "\"" + ","+ Supp + ","+ numberFormat.format(Conf) + ","+ Count + "\n");	
				}								
	        }
	        		         		        
	        scrollResp = ESNode.client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();

	        if (scrollResp.getHits().getHits().length == 0) {
	            break;
	        }
	    }
				
		bw.close();
		System.out.print("\ndone");
	}

	public JsonObject getRecomFilter(String word) {
		FilterBuilder filter_search = FilterBuilders.boolFilter()
				.must(FilterBuilders.termFilter("Type",0))
				.must(FilterBuilders.termFilter("Query_A", word));
		QueryBuilder query_search = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), filter_search);

		SearchResponse sr = ESNode.client.prepareSearch(prep.index)
				.setTypes(queryFilterRuleType)
		        .setQuery(query_search)
	            .addSort("Confidence", SortOrder.DESC) 
	            .setSize(5).execute().actionGet(); 
		
	    HashMap wordweights = new HashMap<String, Double>();
		double weight = 0;
		String wordB = null;
		for (SearchHit hit : sr.getHits().getHits()) 
		{
			Map<String, Object> result = hit.getSource();
			wordB = (String) result.get("Filter_B");
			weight = (double) result.get("Confidence");
			wordweights.put(wordB, weight);	        	   
	    }
	
		JsonObject weightJson = this.MapToJson(word, wordweights);
		System.out.println(weightJson);
		
		return weightJson;
	}
	
	private JsonObject MapToJson(String word, HashMap filterWeights) {
		Gson gson = new Gson();
		JsonObject json = new JsonObject();
		List<JsonObject> filters = new ArrayList<JsonObject>();
		
		Map<String, Double> sortedMap = sortMapByValue(filterWeights);

		for (Map.Entry<String, Double> entry : sortedMap.entrySet()) {
			JsonObject filterJson = new JsonObject();
			String key = entry.getKey();
		    Object value = entry.getValue();
			filterJson.addProperty("filter", key);
			filterJson.addProperty("weight", String.valueOf(value));
			filters.add(filterJson);
		}
		JsonElement filterElement = gson.toJsonTree(filters);
		json.add("filters", filterElement);

		return json;
	}
	
	public LinkedHashMap sortMapByValue(HashMap passedMap) {
		   List mapKeys = new ArrayList(passedMap.keySet());
		   List mapValues = new ArrayList(passedMap.values());
		   Collections.sort(mapValues, Collections.reverseOrder());
		   Collections.sort(mapKeys,Collections.reverseOrder());

		   LinkedHashMap sortedMap = new LinkedHashMap();

		   Iterator valueIt = mapValues.iterator();
		   while (valueIt.hasNext()) {
		       Object val = valueIt.next();
		       Iterator keyIt = mapKeys.iterator();

		       while (keyIt.hasNext()) {
		           Object key = keyIt.next();
		           String comp1 = passedMap.get(key).toString();
		           String comp2 = val.toString();

		           if (comp1.equals(comp2)){
		               passedMap.remove(key);
		               mapKeys.remove(key);
		               sortedMap.put((String)key, (Double)val);
		               break;
		           }

		       }

		   }
		   return sortedMap;
		}
	
	public static void main(String[] args) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		long startTime=System.currentTimeMillis(); 	   
		
		DataMiner prep = new DataMiner("podacc");
		QueryRecomFilters ck = new QueryRecomFilters(prep);
		//ck.createKeywordSet();
		//ck.associationRule(0);
		//ck.associationRule(1);
		//ck.exportRules(0, "D:/query_recom_filter_rules.csv");
		
		ck.getRecomFilter("ocean winds");
		
		ESNode.bulkProcessor.awaitClose(20, TimeUnit.MINUTES);
		ESNode.node.close();
		long endTime=System.currentTimeMillis();
		System.out.println("Preprocessing is done!" + "Time elapsed： "+ (endTime-startTime)/1000+"s");
	}
}
