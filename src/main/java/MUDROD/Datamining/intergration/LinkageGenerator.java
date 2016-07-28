package MUDROD.Datamining.intergration;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortOrder;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import MUDROD.Datamining.DataMiner;
import MUDROD.Datamining.tools.MatrixTool;
import MUDROD.Datamining.tools.TermTriple;
import Utils.ESNode;
import Utils.ESNodeClient;
import Utils.StringTool;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.CSVLoader;
import Utils.DeleteRedType;

public class LinkageGenerator implements Serializable {
	//DataMiner prep = null;
	DecimalFormat df = new DecimalFormat("#.00");
	List<LinkedTerm> termList = new ArrayList<LinkedTerm>();
	String SessionType = null;
	String index = "";
	private ESNodeClient esnode;
	
	class LinkedTerm
	{
		String term = null;
		double weight = 0;
		String model  = null;
		
		public LinkedTerm(String str, double w, String m){
			term = str;
			weight = w;
			model  = m;
		}
	}
	
	public LinkageGenerator(DataMiner test) throws IOException
	{
		this.index = test.index;
		SessionType = test.SessionStats;
		putSWEETMapping();
	}
	
	 public void putSWEETMapping() throws IOException{
	    	XContentBuilder Mapping =  jsonBuilder()
					.startObject()
						.startObject("SWEET")
							.startObject("properties")
								.startObject("concept_A")
									.field("type", "string")
									.field("index", "not_analyzed")
								.endObject()
								.startObject("concept_B")
									.field("type", "string")
									.field("index", "not_analyzed")
								.endObject()
								
							.endObject()
						.endObject()
					.endObject();


	        ESNode.client.admin().indices()
			  .preparePutMapping(this.index)
	          .setType("SWEET")
	          .setSource(Mapping)
	          .execute().actionGet();
	    }

	public double cosSim(Instance arg0, Instance arg1) {  // this is not dist
		double dotProduct = 0.0;
	    double normA = 0.0;
	    double normB = 0.0;
       
	    int attribute_length = arg0.numAttributes();
	    for (int i = 1; i < attribute_length; i++) {
	        dotProduct += arg0.value(i) * arg1.value(i);
	        normA += Math.pow(arg0.value(i), 2);
	        normB += Math.pow(arg1.value(i), 2);
	    }  
  
	    double cos = dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
	    return cos;
	}
	
	public boolean zeroVec(Instance arg0){
		double norm = 0.0;
		int attribute_length = arg0.numAttributes();
		for (int i = 1; i < attribute_length; i++) {
	        norm += Math.pow(arg0.value(i), 2);
	    } 
		
		if(norm==0){
			return true;
		}else{
			return false;
		}	
		
	}
	
	public void importToES1(String inputFileName, String type) throws Exception
	{
		DeleteRedType drt = new DeleteRedType();
		drt.deleteAllByQuery(this.index, type, QueryBuilders.matchAllQuery());
		
		CSVLoader loader = new CSVLoader();
    
		loader.setSource(new File(inputFileName));
        loader.setOptions(new String[] {"-S", "first"});  // if the first attribute is a string, numAttributes still equals numValues, but the first one is replaced by 0, so the index should start with 1
        Instances data = loader.getDataSet();
    
		for(int i =0; i<data.numInstances();i++)
		{
			if(!zeroVec(data.instance(i))){
				for(int j=i;j<data.numInstances();j++){
					if(!zeroVec(data.instance(j))){
						double cosinSim = cosSim(data.instance(i), data.instance(j));
						double weight = Double.parseDouble(df.format(cosinSim));
						
						String keywords = data.attribute(0).value(i) + "," + data.attribute(0).value(j);
						IndexRequest ir = new IndexRequest(this.index, type).source(jsonBuilder()
								.startObject()
								.field("keywords", keywords)
								.field("weight", weight)	
								.endObject());
						ESNode.bulkProcessor.add(ir);
					}
		        }
			}
		}
	}
	
	public void importToES(ESNodeClient esnode,String csvFileName, String type) throws Exception
	{
		DeleteRedType drt = new DeleteRedType(esnode);
		drt.deleteAllByQuery(this.index, type, QueryBuilders.matchAllQuery());
		
		JavaPairRDD<String, Vector> importRDD = MatrixTool.loadVectorFromCSV(csvFileName);
		CoordinateMatrix simMatirx = MatrixTool.calVecsimilarity(importRDD.values());
		List<TermTriple> triples = MatrixTool.toTriples(importRDD.keys(), simMatirx);
		TermTriple.insertTriples(esnode,triples,this.index, type);
	}

	public void importSWEEToES(ESNodeClient esnode,String inputFileName, String type) throws IOException, InterruptedException, ExecutionException
	{                                                                               //e = 9, w1 =1, w2 =3
		//prep.InitiateIndex("podaacsession");

		DeleteRedType drt = new DeleteRedType(esnode);
		drt.deleteAllByQuery(this.index, type, QueryBuilders.matchAllQuery());
		
		BufferedReader br = null;
		String line = "";
		double weight = 0;
		
		try {
			br = new BufferedReader(new FileReader(inputFileName));
			while ((line = br.readLine()) != null) {
				String[] strList = line.toLowerCase().split(",");
				//String keywords = strList[0] + "," + strList[2];
				if(strList[1].equals("subclassof"))
				{
					weight = 0.75;
				}else{
					weight = 0.9;
				}

				IndexRequest ir = new IndexRequest(this.index, type).source(jsonBuilder()
						.startObject()
						.field("concept_A", StringTool.customAnalyzing(this.index, strList[2]))
						.field("concept_B", StringTool.customAnalyzing(this.index, strList[0]))
						.field("weight", weight)	
						.endObject());
				ESNode.bulkProcessor.add(ir);

			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	  
	}
	
	public String extractRelated(String str, String input){
		String[] str_List = str.split(",");
		if(input.equals(str_List[0])){
			return str_List[1];
		}else{
			return str_List[0];
		}
	}
	
	public void aggregateRelatedTerms(String input, String model){
		SearchResponse usrhis = ESNode.client.prepareSearch(this.index)
		        .setTypes(model)
		        .setQuery(QueryBuilders.termQuery("keywords", input))
		        .addSort("weight",SortOrder.DESC) 
		        .setSize(20)
		        .execute()
		        .actionGet();
		for (SearchHit hit : usrhis.getHits().getHits()) 
		{
			Map<String,Object> result = hit.getSource();
			String keywords = (String) result.get("keywords");
			String relatedKey = extractRelated(keywords, input);
			//System.out.println(relatedKey);
			if(!relatedKey.equals(input))
			{
				LinkedTerm lTerm = new LinkedTerm(relatedKey, (double) result.get("weight"), model);
				termList.add(lTerm);
			}
	        	   
	    }
	}
	
	public void aggregateRelatedTermsSWEET(String input, String model){
		SearchResponse usrhis = ESNode.client.prepareSearch(this.index)
		        .setTypes(model)
		        .setQuery(QueryBuilders.termQuery("concept_A", input))
		        .addSort("weight",SortOrder.DESC) 
		        .setSize(30)
		        .execute()
		        .actionGet();
		for (SearchHit hit : usrhis.getHits().getHits()) 
		{
			Map<String,Object> result = hit.getSource();
			String concept_B = (String) result.get("concept_B");
			if(!concept_B.equals(input))
			{
				LinkedTerm lTerm = new LinkedTerm(concept_B, (double) result.get("weight"), model);
				termList.add(lTerm);
			}	        	   
	    }
	}
	
	public Map<String, List<LinkedTerm>> aggregateRelatedTermsFromAllmodel(String input)
	{
		aggregateRelatedTerms(input, "userHistory");
		aggregateRelatedTerms(input, "userClicking");
		aggregateRelatedTerms(input, "GCMDMetadata");
		aggregateRelatedTermsSWEET(input, "SWEET");
		
		Map<String, List<LinkedTerm>> group = termList.stream().collect(Collectors.groupingBy(w -> w.model));
		
		return termList.stream().collect(Collectors.groupingBy(w -> w.term));
	}
	
	public int getModelweight(String model)
	{
		if(model.equals("userHistory"))
		{
			return 2;
		}
		
		if(model.equals("userClicking"))
		{
			return 2;
		}
		
		if(model.equals("GCMDMetadata"))
		{
			return 2;
		}
		
		if(model.equals("SWEET"))
		{
			return 1;
		}
		
		return 0;
	}
	
	public LinkedHashMap<String, Double> sortMapByValue(Map passedMap) {
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
	
	public String TransformQuerySem(String input, int Num) throws InterruptedException, ExecutionException{
		termList = new ArrayList<LinkedTerm>();
		Map<String, Double> terms_map = new HashMap<String, Double>();
		
		Map<String, List<LinkedTerm>> map = aggregateRelatedTermsFromAllmodel(StringTool.customAnalyzing(this.index,input));
		for (Entry<String, List<LinkedTerm>> entry : map.entrySet()) {
			
			/*if(entry.getKey().equals("ocean current")){
				System.out.println("bingo");
			}*/
			
			List<LinkedTerm> list = entry.getValue();
			double sum_model_w = 0;
			double sum_w_term= 0;
			for (LinkedTerm element : list) {
				sum_model_w += getModelweight(element.model);
				sum_w_term += element.weight*getModelweight(element.model);
			}
			
			double final_w = (sum_w_term/sum_model_w) + ((list.size() - 2) * 0.05);
			if(final_w<0){
				final_w=0;
			}
			
			if(final_w>1){
				final_w=1;
			}
			terms_map.put(entry.getKey(), Double.parseDouble(df.format(final_w)));			
		}
		
		//System.out.println(sortMapByValue(terms_map));	
		Map<String, Double> sortedMap = sortMapByValue(terms_map);  //terms_map will be empty after this step
		
		String output = "(" + input.replace(" ", " AND ") + ")";
		int count = 0;
		for (Entry<String, Double> entry : sortedMap.entrySet()) {
			String item = "(" + entry.getKey().replace(" ", " AND ") + ")";
			if(count<Num)
			{
			output += " OR " + item;
			}
			
			/*if(count == Num)
			{
			output += item;
			}*/
			count++;
		}
		return output;

	}
	
	public String appyMajorRule(String input, int Num) throws InterruptedException, ExecutionException{
		termList = new ArrayList<LinkedTerm>();
		Map<String, Double> terms_map = new HashMap<String, Double>();
		
		Map<String, List<LinkedTerm>> map = aggregateRelatedTermsFromAllmodel(StringTool.customAnalyzing(this.index,input));
		for (Entry<String, List<LinkedTerm>> entry : map.entrySet()) {
			
			/*if(entry.getKey().equals("ocean current")){
				System.out.println("bingo");
			}*/
			
			List<LinkedTerm> list = entry.getValue();
			double sum_model_w = 0;
			double sum_w_term= 0;
			for (LinkedTerm element : list) {
				sum_model_w += getModelweight(element.model);
				sum_w_term += element.weight*getModelweight(element.model);
			}
			
			double final_w = (sum_w_term/sum_model_w) + ((list.size() - 2) * 0.05);
			if(final_w<0){
				final_w=0;
			}
			
			if(final_w>1){
				final_w=1;
			}
			terms_map.put(entry.getKey(), Double.parseDouble(df.format(final_w)));			
		}
		
		//System.out.println(sortMapByValue(terms_map));	
		Map<String, Double> sortedMap = sortMapByValue(terms_map);  //terms_map will be empty after this step
		
		String output = "";
		int count = 0;
		for (Entry<String, Double> entry : sortedMap.entrySet()) {
			if(count<Num)
			{
			output += entry.getKey() + " = " + entry.getValue() + ",";
			}
			count++;
		}
		return output;

	}
	
	public JsonObject appyMajorRuleToJson(String input) throws InterruptedException, ExecutionException{
		termList = new ArrayList<LinkedTerm>();
		Map<String, Double> terms_map = new HashMap<String, Double>();
		
		Map<String, List<LinkedTerm>> map = aggregateRelatedTermsFromAllmodel(StringTool.customAnalyzing(this.index,input));
		
		
		for (Entry<String, List<LinkedTerm>> entry : map.entrySet()) {	
			//System.out.println("**********");
			//System.out.println(entry);
			List<LinkedTerm> list = entry.getValue();
			
			for (LinkedTerm element : list) {
				if(element.model.equals("userHistory"))
				System.out.println(element.model + " " + element.term + " " + element.weight);
				//System.out.println(element.term);
				//System.out.println(element.weight);
			}
		}
		
		
		for (Entry<String, List<LinkedTerm>> entry : map.entrySet()) {		
			List<LinkedTerm> list = entry.getValue();
			double sum_model_w = 0;
			double sum_w_term= 0;
			for (LinkedTerm element : list) {
				sum_model_w += getModelweight(element.model);
				sum_w_term += element.weight*getModelweight(element.model);
			}
			
			double final_w = (sum_w_term/sum_model_w) + ((sum_model_w - 2) * 0.05);
			if(final_w<0){
				final_w=0;
			}
			
			if(final_w>1){
				final_w=1;
			}
			terms_map.put(entry.getKey(), Double.parseDouble(df.format(final_w)));			
		}
		
		Map<String, Double> sortedMap = sortMapByValue(terms_map);
		
		int count = 0;
		Map<String, Double> trimmed_map = new HashMap<String, Double>();
		for (Entry<String, Double> entry : sortedMap.entrySet()) {
			if(count<10)
			{
				trimmed_map.put(entry.getKey(), entry.getValue());	
			}
			count++;
		}
		
		return MapToJson(input, trimmed_map);

	}
	
	
	
	private JsonObject MapToJson(String word, Map<String, Double> wordweights) {
		Gson gson = new Gson();
		JsonObject json = new JsonObject();
		
		List<JsonObject> nodes = new ArrayList<JsonObject>();
		JsonObject firstNode = new JsonObject();
		firstNode.addProperty("name", word);
		firstNode.addProperty("group", 1);
		nodes.add(firstNode);
		Set<String> words = wordweights.keySet();
		for(String wordB : words){
			JsonObject node = new JsonObject();
			node.addProperty("name", wordB);
			node.addProperty("group", 10);
			nodes.add(node);
		}
		JsonElement nodesElement = gson.toJsonTree(nodes);
		json.add("nodes", nodesElement);
			
		List<JsonObject> links = new ArrayList<JsonObject>();
		
		Collection<Double> weights = wordweights.values();
		int num = 1;
		for(double weight : weights){
			JsonObject link = new JsonObject();
			link.addProperty("source", num);
			link.addProperty("target", 0);
			link.addProperty("value", weight);
			links.add(link);
			num += 1;
		}
		JsonElement linksElement = gson.toJsonTree(links);
		json.add("links", linksElement);

		return json;
	}
	
	
	public void eval(String output) throws IOException, InterruptedException, ExecutionException{
		File file = new File(output);
		if (!file.exists()) {
			file.createNewFile();
		}
		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		
		SearchResponse sr = ESNode.client.prepareSearch(this.index)                          
				.setTypes(SessionType+"01",SessionType+"02",SessionType+"04",SessionType+"05",SessionType+"06",SessionType+"07",
						SessionType+"08",SessionType+"09",SessionType+"10",SessionType+"11",SessionType+"12")
				.setQuery(QueryBuilders.matchAllQuery())
				.setSize(0)
				.addAggregation(AggregationBuilders.terms("IPs")
						.field("keywords").size(0))  //important to set size 0, sum_other_doc_count
				.execute().actionGet();
		Terms IPs = sr.getAggregations().get("IPs");
		int count = 0;
		for (Terms.Bucket entry : IPs.getBuckets()) 
		{			
			
			if(count<10)
			{
				bw.write(entry.getKey() +"," + appyMajorRule(entry.getKey(), 12) + "\n\nComments: \n");
			}
			count++;
			
			if(count==10)
			{
				bw.write("\n");
			}
			
			if(entry.getDocCount()>=19&&entry.getDocCount()<=22){                               
				bw.write(entry.getKey() +"," + appyMajorRule(entry.getKey(), 12) + "\n\nComments: \n");
			}
			
			
		} 
		
		bw.write("\n");
		String random[] = {"sea surface height", "noaa 18", "grace", "seasat", "pathfinder", "ocean wave", "ghrsst", "sea ice", "avhrr 2", "jason 1 geodetic"};
		for(int j=0; j<random.length; j++)
		{
			bw.write(random[j] +"," + appyMajorRule(random[j], 12) +"\n\nComments: \n");
		}
		
		bw.close();
	}
		
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		long startTime=System.currentTimeMillis(); 	
			
		DataMiner prep = new DataMiner();
		LinkageGenerator lg= new LinkageGenerator(prep);
		//lg.importToES("C:/AIST LOG FILES/PODAACKeywords/clusteringmatrix_userbags_bi_filtered.csv", "userHistory");
		//lg.importToES("C:/AIST LOG FILES/PODAACKeywords/clusteringmatrix_hi_import_downloadIn_revised_svd.csv", "userClicking");
		//lg.importToES("C:/AIST LOG FILES/PODAACKeywords/GCMD_tfidf_svd.csv", "GCMDMetadata");
		//lg.importSWEEToES("C:/AIST LOG FILES/PODAACKeywords/Ocean_triples.csv", "SWEET");
		
		String userCSV = "D:/test/clusteringmatrix_userbags_bi_filtered.csv";
		lg.importToES(null,userCSV, "userHistory");
		
		//String clickCSV = "D:/test/distResult_SVD_Query_sim.csv";
		//lg.importToES(clickCSV, "userClicking");
		//lg.appyMajorRuleToJson("sea surface temperature");
		
		//System.out.println(lg.TransformQuerySem("ocean wind", 3));
		
		//lg.eval("C:/AIST LOG FILES/PODAACKeywords/eval.csv");
		
		ESNode.bulkProcessor.awaitClose(20, TimeUnit.MINUTES);
		ESNode.node.close();
		long endTime=System.currentTimeMillis();
		System.out.println("Done!" + "Time elapsed： "+ (endTime-startTime)/1000+"s");
	}

}
