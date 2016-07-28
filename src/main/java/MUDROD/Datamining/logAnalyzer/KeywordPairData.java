package MUDROD.Datamining.logAnalyzer;

public class KeywordPairData {
	
	public String keywordA;
	public String keywordB;
	public double weight;
	public String time;
	public String sessionID;

	public String Aurl;
	public String Burl;
	public String viewurl;
	public int type; // 0-single; 1-combo

	
	public KeywordPairData(){
		
	}
	
	public KeywordPairData(String keywordA, String keywordB, double weight){
		this.keywordA = keywordA;
		this.keywordB = keywordB;
		this.weight = weight;
		//this.sessionID = sessionID;
	}
	
	public KeywordPairData(String keywordA, String keywordB, double weight,int type, String Aurl, String Burl,String viewurl){
		this.keywordA = keywordA;
		this.keywordB = keywordB;
		this.weight = weight;
		//this.sessionID = sessionID;
		this.type = type;
		
		this.Aurl = Aurl;
		this.Burl = Burl;
		this.viewurl = viewurl;
	}
	
	
	public void setSessionId(String sessionID){
		this.sessionID = sessionID;
	}
	
	public void setTime(String time){
		this.time = time;
	}

	
	public String toString(){
		
		return "keywordA:" + keywordA + "|| keywordB" + keywordB + "|| weight:" + weight;
	
	}
	
	public String toJson() {

		String jsonQuery = "{";
		jsonQuery += "\"keywordA\":\"" + this.keywordA + "\",";
		jsonQuery += "\"keywordB\":\"" + this.keywordB + "\",";
		jsonQuery += "\"weight\":\"" + this.weight + "\",";
		//jsonQuery += "\"sessionId\":\"" + this.sessionID + "\",";
		jsonQuery += "\"sessionId\":\"" + this.sessionID + "\"";

		jsonQuery += "},";

		return jsonQuery;
	}

}
