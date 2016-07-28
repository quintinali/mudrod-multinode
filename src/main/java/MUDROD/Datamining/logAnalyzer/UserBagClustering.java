package MUDROD.Datamining.logAnalyzer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import MUDROD.Datamining.DataMiner;
import Utils.ESNode;
import Utils.ESNodeClient;

public class UserBagClustering {
	//DataMiner prep = null;
	String SessionType = null;
	String index = "";
	public UserBagClustering(DataMiner test)
	{
		//prep = test;
		SessionType = test.SessionStats;
		this.index = test.index;
	}
	
	public void GenerateMatrix(String output) throws IOException{  //you need it
		
		File file = new File(output);
		if (!file.exists()) {
			file.createNewFile();
		}
		
		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		
		bw.write("Num" + ",");	
		
		//step 1
		SearchResponse sr = ESNode.client.prepareSearch(this.index)                          
				.setTypes(SessionType+"01",SessionType+"02",SessionType+"04",SessionType+"05",SessionType+"06",SessionType+"07",
						SessionType+"08",SessionType+"09",SessionType+"10",SessionType+"11",SessionType+"12")
				.setQuery(QueryBuilders.matchAllQuery())
				.setSize(0)
				.addAggregation(AggregationBuilders.terms("IPs")
						.field("IP").size(0))  //important to set size 0, sum_other_doc_count
				.execute().actionGet();
		Terms IPs = sr.getAggregations().get("IPs");
		List<String> IPList = new ArrayList<String>();
		for (Terms.Bucket entry : IPs.getBuckets()) {
			if(entry.getDocCount()>5){                               //important threshold!!!!
				IPList.add(entry.getKey());
			}
		} 
		
		bw.write(String.join(",", IPList) +"\n");
		
		bw.write("Num,");
		for(int k=0; k< IPList.size(); k++){
			if(k!=IPList.size()-1){
			bw.write("f" + k + ",");
			}else{
				bw.write("f" + k + "\n");
			}
		}
		
		//step 2
		SearchResponse sr_2 = ESNode.client.prepareSearch(this.index)                          
				.setTypes(SessionType+"01",SessionType+"02",SessionType+"04",SessionType+"05",SessionType+"06",SessionType+"07",
						SessionType+"08",SessionType+"09",SessionType+"10",SessionType+"11",SessionType+"12")
				.setQuery(QueryBuilders.matchAllQuery())
				.setSize(0)
				.addAggregation(AggregationBuilders.terms("KeywordAgg")
						.field("keywords").size(0).subAggregation(
								AggregationBuilders.terms("IPAgg")
								.field("IP").size(0)))  //important to set size 0, sum_other_doc_count
				.execute().actionGet();
		Terms keywords = sr_2.getAggregations().get("KeywordAgg");
		int NumDocs = keywords.getBuckets().size();
		for (Terms.Bucket keyword : keywords.getBuckets()) {			
			
			Map<String, Double> IP_map = new HashMap<String, Double>();
			Terms IPAgg = keyword.getAggregations().get("IPAgg");
			
			int distinct_user = IPAgg.getBuckets().size();
			if(distinct_user>5)                     //important to set size 0, sum_other_doc_count
			{
				bw.write(keyword.getKey() + ",");	
				for (Terms.Bucket IP : IPAgg.getBuckets()) {
					double tfidf = GetTFIDF((int) (long)IP.getDocCount(), NumDocs, IP.getKey());				
					IP_map.put(IP.getKey(), tfidf);
				}			
				for(int i =0; i<IPList.size();i++){
					if(IP_map.containsKey(IPList.get(i))){
						//bw.write(df.format(dataset_map.get(datasetList.get(i))) + ",");
						bw.write(IP_map.get(IPList.get(i)) + ",");
					}else{
						bw.write("0,");
					}				
				}			
				bw.write("\n");
			}
		}
		
		bw.close();
		System.out.print("\ndone");
	}
	
	public double GetTFIDF(int tf, int numdoc, String view){
		double tf_root = Math.sqrt(tf);
		FilterBuilder filter_search = FilterBuilders.boolFilter()
				.must(FilterBuilders.termFilter("IP", view));
		QueryBuilder  query_search = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), filter_search);
		SearchResponse sr = ESNode.client.prepareSearch(this.index)                          
				.setTypes(SessionType+"01",SessionType+"02",SessionType+"04",SessionType+"05",SessionType+"06",SessionType+"07",
						SessionType+"08",SessionType+"09",SessionType+"10",SessionType+"11",SessionType+"12")
				.setQuery(query_search)
				.setSize(0)
				.addAggregation(AggregationBuilders.terms("KeywordAgg")
						.field("keywords").size(0))  //important to set size 0, sum_other_doc_count
				.execute().actionGet();
		Terms keywords = sr.getAggregations().get("KeywordAgg");
		int docFreq = keywords.getBuckets().size();
		double tmp = (double) numdoc/(docFreq+1);
		double idf = 1 + Math.log10(tmp);
		double tfidf = tf_root*idf;
		
		
		return tfidf;
	}
	
	public void GenerateBinaryMatrix(ESNodeClient esnode, String output) throws IOException{  
		File file = new File(output);
		if (!file.exists()) {
			file.createNewFile();
		}
		
		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		
		bw.write("Num" + ",");	

		//step 1
		List<String> indexs = esnode.getIndexListWithPrefix("aistcloud");
		String[] indexsArr = indexs.toArray(new String[0]);
	
		SearchResponse sr = ESNode.client.prepareSearch(indexsArr)                          
				.setTypes(SessionType)
				.setQuery(QueryBuilders.matchAllQuery())
				.setSize(0)
				.addAggregation(AggregationBuilders.terms("IPs")
						.field("IP").size(0))  //important to set size 0, sum_other_doc_count
				.execute().actionGet();
		Terms IPs = sr.getAggregations().get("IPs");
		List<String> IPList = new ArrayList<String>();
		for (Terms.Bucket entry : IPs.getBuckets()) {
			if(entry.getDocCount()>5){                               //important threshold!!!!
				IPList.add(entry.getKey());
			}
		} 
		
		bw.write(String.join(",", IPList) +"\n");
		
		bw.write("Num,");
		for(int k=0; k< IPList.size(); k++){
			if(k!=IPList.size()-1){
			bw.write("f" + k + ",");
			}else{
				bw.write("f" + k + "\n");
			}
		}
		
		//step 2
		SearchResponse sr_2 = esnode.client.prepareSearch(indexsArr)                          
				.setTypes(SessionType)
				.setQuery(QueryBuilders.matchAllQuery())
				.setSize(0)
				.addAggregation(AggregationBuilders.terms("KeywordAgg")
						.field("keywords").size(0).subAggregation(
								AggregationBuilders.terms("IPAgg")
								.field("IP").size(0)))  //important to set size 0, sum_other_doc_count
				.execute().actionGet();
		Terms keywords = sr_2.getAggregations().get("KeywordAgg");
		for (Terms.Bucket keyword : keywords.getBuckets()) {							
			Map<String, Integer> IP_map = new HashMap<String, Integer>();
			Terms IPAgg = keyword.getAggregations().get("IPAgg");	
			
			int distinct_user = IPAgg.getBuckets().size();
			if(distinct_user>5)
			{
				bw.write(keyword.getKey() + ",");
				for (Terms.Bucket IP : IPAgg.getBuckets()) {
					
					IP_map.put(IP.getKey(), 1);
				}			
				for(int i =0; i<IPList.size();i++){
					if(IP_map.containsKey(IPList.get(i))){
						//bw.write(df.format(dataset_map.get(datasetList.get(i))) + ",");
						bw.write(IP_map.get(IPList.get(i)) + ",");
					}else{
						bw.write("0,");
					}				
				}			
				bw.write("\n");
			}
		}
		
		bw.close();
		System.out.print("\ndone");
	}

	public static void main(String[] args) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
        long startTime=System.currentTimeMillis(); 	
		
        DataMiner prep = new DataMiner("podacc");
		UserBagClustering test = new UserBagClustering(prep);
		
		//test.GenerateMatrix("C:/AIST LOG FILES/PODAACKeywords/clusteringmatrix_userbags.csv");
		test.GenerateBinaryMatrix(null,"C:/AIST LOG FILES/PODAACKeywords/clusteringmatrix_userbags_bi_filtered.csv");
		
		ESNode.bulkProcessor.awaitClose(20, TimeUnit.MINUTES);
		ESNode.node.close();
		long endTime=System.currentTimeMillis();
		System.out.println("Done!" + "Time elapsed： "+ (endTime-startTime)/1000+"s");
	}

}
