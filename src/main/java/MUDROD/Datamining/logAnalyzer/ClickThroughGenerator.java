package MUDROD.Datamining.logAnalyzer;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import MUDROD.Datamining.DataMiner;
import MUDROD.SessionRecon.Session;
import Utils.ESNode;
import Utils.ESNodeClient;

public class ClickThroughGenerator {
	String index;
	String cleanup_type;
	String session_type;
	//DataMiner processor;
	int timeThres; // should be used

	public ClickThroughGenerator(DataMiner processor) {
		this.index = processor.index;
		this.cleanup_type = processor.Cleanup_type;
		this.session_type = processor.SessionStats;
		//this.processor = processor;
	}

	public List<ClickThroughData> genClickThroughData(ESNodeClient esnode) throws UnsupportedEncodingException {
		List<String> indexs = esnode.getIndexListWithPrefix("aistcloud");

		List<ClickThroughData> result = new ArrayList<ClickThroughData>();
		for (int n = 0; n < indexs.size(); n++) {
			String index = indexs.get(n);
			Map<String, Map<String,String>> sessionIds = this.getSessionIDs(index);
			String sessionId = "";
			String cleanupType = "";
			String tmpindex = "";
			Session session = new Session(this.index, esnode);
			for (String key : sessionIds.keySet()) {
				sessionId = key;
				tmpindex = sessionIds.get(key).get("index");
				cleanupType = sessionIds.get(key).get("cleanupType");
				List<ClickThroughData> datas = session.genClickThroughData(tmpindex, cleanupType, sessionId);
				result.addAll(datas);
			}
		}

		return result;
	}

	public Map<String, Map<String,String>> getSessionIDs(String index) throws UnsupportedEncodingException {

		Map<String, Map<String,String>> sessionIDs = new HashMap<String, Map<String,String>>();
	
		SearchResponse scrollResp = ESNode.client.prepareSearch(index).setTypes(this.session_type)
				.setScroll(new TimeValue(60000))
				.setQuery(QueryBuilders
						.matchAllQuery())
				.execute().actionGet();

		String sessionID = "";
		String cleanupType = this.cleanup_type;
		int session_count = 0;
		String sessionTime = "";

		Session session = new Session(index, null);

		while (true) {
			for (SearchHit hit : scrollResp.getHits().getHits()) {
				session_count++;
				Map<String, Object> result = hit.getSource();
				sessionID = (String) result.get("SessionID");
				sessionTime =  (String) result.get("Time");
				
				Map<String,String> sessionInfo = new HashMap<String,String>();
				sessionInfo.put("index", index);
				sessionInfo.put("cleanupType", cleanupType);
				sessionInfo.put("time", sessionTime);
		        
				sessionIDs.put(sessionID, sessionInfo);
			}
			scrollResp = ESNode.client.prepareSearchScroll(scrollResp.getScrollId())
					.setScroll(new TimeValue(600000)).execute().actionGet();
			if (scrollResp.getHits().getHits().length == 0) {
				break;
			}
		}
		
		return sessionIDs;
	}
	
	public static void main(String[] args) throws IOException{
	/*	DataMiner prep = new DataMiner("podacc");
		ClickThroughGenerator gen = new ClickThroughGenerator(prep);
		List<ClickThroughData> datas = gen.genClickThroughData();

		String outputFile = "C:/AIST LOG FILES/PODAACKeywords/clickstream";
	    Writer out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile)));
	    for (int k = 0; k < datas.size(); k++) { 
	    	out.write(String.format("%s\n", datas.get(k).toJson()));
	    }
	    out.close();*/
	}

}
