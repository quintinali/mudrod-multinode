package MUDROD.Datamining.logAnalyzer;

import java.io.IOException;
import java.io.Serializable;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class ClickThroughData implements Serializable {
	
	public String keywords;
	public String viewDataset;
	public String downloadDataset;
	public String sessionID;
	public String type;
	
	public ClickThroughData(){
		
	}
	
	public ClickThroughData(String keywords, String viewDataset, boolean download){
		this.keywords = keywords;
		this.viewDataset = viewDataset;
		this.downloadDataset = "";
		if(download){
			this.downloadDataset = viewDataset;
		}
	}
	
	public void setSessionId(String sessionID){
		this.sessionID = sessionID;
	}
	
	public void setType(String type){
		this.type = type;
	}
	
	public String toString(){
		
		return "query:" + keywords + "|| view dataset:" + viewDataset + "|| download Dataset:" + downloadDataset;
	
	}
	
	public String toJson() {

		String jsonQuery = "{";
		jsonQuery += "\"query\":\"" + this.keywords + "\",";
		jsonQuery += "\"viewdataset\":\"" + this.viewDataset + "\",";
		jsonQuery += "\"downloaddataset\":\"" + this.downloadDataset + "\",";
		jsonQuery += "\"sessionId\":\"" + this.sessionID + "\",";
		jsonQuery += "\"type\":\"" + this.type + "\"";

		jsonQuery += "},";

		return jsonQuery;
	}
	
	public static ClickThroughData parseFromLogLine(String logline)throws IOException, NoSuchAlgorithmException, JSONException {

		JSONObject jsonData = new JSONObject(logline);
		ClickThroughData data = new ClickThroughData();
		data.setKeyWords(jsonData.getString("query"));
		data.setViewDataset(jsonData.getString("viewdataset"));
		data.setDownloadDataset(jsonData.getString("downloaddataset"));

		return data;
	}
	
	public static List<ClickThroughData> parseDatasFromLogLine(String logline)throws IOException, NoSuchAlgorithmException, JSONException {

		JSONObject jsonData = new JSONObject(logline);
		List<ClickThroughData> datas = new ArrayList<ClickThroughData>();
		
		String query = jsonData.getString("query");
		String[] querys = query.split(",");
		for(int i=0; i<querys.length; i++){
			
			ClickThroughData click = new ClickThroughData();
			click.setKeyWords(querys[i]);
			click.setViewDataset(jsonData.getString("viewdataset"));
			click.setDownloadDataset(jsonData.getString("downloaddataset"));
			datas.add(click);
		}
		
		return datas;
	}
	
	public void setKeyWords(String query){
		this.keywords = query;
	}
	
	public void setViewDataset(String dataset){
		this.viewDataset = dataset;
	}
	
	
	public void setDownloadDataset(String dataset){
		this.downloadDataset = dataset;
	}

	public String getKeyWords(){
		return this.keywords;
	}
	
	public String getViewDataset(){
		return this.viewDataset;
	}
}
