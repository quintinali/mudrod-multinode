package MUDROD.Datamining.Metadata;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.*;
import java.lang.String;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SQLContext;
import org.codehaus.jettison.json.JSONException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import MUDROD.Datamining.DataMiner;
import Utils.ESNode;
import Utils.StringTool;
import scala.Tuple2;

/**
 * This class represents an Apache access log line. See
 * http://httpd.apache.org/docs/2.2/logs.html for more details.
 */
public class PODAACMetaData implements Serializable {

	private String shortname;
	private String longname;
	private String topic;
	private String term;
	private String variable;
	private String keywordStr;
	private String abstractStr;
	private String isoTopic;
	private String sensor;
	private String source;
	private String project;
	boolean hasAbstarct;
	
	private List<String> keywordList;
	private List<String> termList;
	private List<String> topicList;
	private List<String> variableList;
	private List<String> abstractList;
	private List<String> isotopicList;
	private List<String> sensorList;
	private List<String> sourceList;
	private List<String> projectList;
	
	DataMiner prep = null;
	String type = "metadataSource";
	String index = "metadata";
	public PODAACMetaData(DataMiner miner) {
		prep = miner;
		prep.index = "metadata";
	}

	public PODAACMetaData(String shortname, String longname, String topic, String term, String variable,
			String keywordStr) throws UnsupportedEncodingException, NoSuchAlgorithmException, InterruptedException, ExecutionException {
		this.shortname = shortname;
		this.longname = longname;
		this.topic = topic;
		this.term = term;
		this.variable = variable;
		this.keywordStr = keywordStr;
		this.hasAbstarct = false;

		/*this.keywordList = new ArrayList<String>();
		this.termList = new ArrayList<String>();
		this.topicList = new ArrayList<String>();
		this.variableList = new ArrayList<String>();
		this.abstractList = new ArrayList<String>();*/
		
		this.keywordList = new ArrayList<String>();
		this.termList = new ArrayList<String>();
		this.topicList = new ArrayList<String>();
		this.variableList = new ArrayList<String>();
		this.abstractList = new ArrayList<String>();
		this.isotopicList = new ArrayList<String>();
		this.sensorList = new ArrayList<String>();
		this.sourceList = new ArrayList<String>();
		this.projectList = new ArrayList<String>();
		
		this.parseDetailInfo();
	}
	
	public PODAACMetaData(String shortname, String longname, String topic, String term, String variable,
			String keywordStr, String isotopic, String sensor, String source, String project, String abstractstr) throws UnsupportedEncodingException, NoSuchAlgorithmException, InterruptedException, ExecutionException {
		this.shortname = shortname;
		this.longname = longname;
		this.topic = topic;
		this.term = term;
		this.variable = variable;
		this.keywordStr = keywordStr;
		this.abstractStr = abstractstr;
		this.isoTopic = isotopic;
		this.sensor = sensor;
		this.source = source;
		this.project = project;
		
		this.keywordList = new ArrayList<String>();
		this.termList = new ArrayList<String>();
		this.topicList = new ArrayList<String>();
		this.variableList = new ArrayList<String>();
		this.abstractList = new ArrayList<String>();
		this.isotopicList = new ArrayList<String>();
		this.sensorList = new ArrayList<String>();
		this.sourceList = new ArrayList<String>();
		this.projectList = new ArrayList<String>();
		this.parseDetailInfo();
		//this.setAllTermList();
	}

	public PODAACMetaData() {
		this.keywordList = new ArrayList<String>();
		this.termList = new ArrayList<String>();
	}

	public PODAACMetaData parseDetailInfo() throws UnsupportedEncodingException, NoSuchAlgorithmException, InterruptedException, ExecutionException {
		this.setKeywords(this.keywordStr);
		this.setTerms(this.term);
		this.setTopicList(this.topic);
		this.setVaraliableList(this.variable);
		
		if(this.isoTopic != null && !this.isoTopic.equals("")){
			this.setISOTopicList(this.isoTopic);
		}
		
		if(this.sensor != null&& !this.sensor.equals("")){
			this.setSensorList(this.sensor);
		}
		
		if(this.source != null&&!this.source.equals("")){
			this.setSourceList(this.source);
		}
		
		if(this.project != null&&!this.project.equals("")){
			this.setProjectList(this.project);
		}
		
		if(this.abstractStr != null&&!this.abstractStr.equals("")){
			this.setAbstractList(this.abstractStr);
		}
		
		
		
		return this;
	}

	private void setAbstractList(String line) throws InterruptedException, ExecutionException {
	
	}

	private void setProjectList(String project2) {
		this.splitString(project2, this.projectList);
	}

	private void setSourceList(String source2) {
		this.splitString(source2, this.sourceList);
	}

	private void setSensorList(String sensor2) {
		this.splitString(sensor2, this.sensorList);
	}

	private void setISOTopicList(String isoTopic2) {
		this.splitString(isoTopic2, this.isotopicList);
	}

	public List<String> getKeywordList() {
		return this.keywordList;
	}

	public List<String> getTermList() {
		return this.termList;
	}

	public String getShortName() {
		return this.shortname;
	}
	
	public String getLongName() {
		return this.longname;
	}

	/*public List<String> getAllTermList(){
		
		return this.allcustomeTerms;
	}
*/
	public List<String> getAllTermList() throws InterruptedException, ExecutionException {
		List<String> allterms = new ArrayList<String>();
		if (this.termList.size() > 0) {
			allterms.addAll(this.termList);
		}

		if (this.keywordList.size() > 0) {
			allterms.addAll(this.keywordList);
		}
		
		if (this.topicList.size() > 0) {
			allterms.addAll(this.topicList);
		}

		if (this.variableList.size() > 0) {
			allterms.addAll(this.variableList);
		}

		if (this.isotopicList.size() > 0) {
			allterms.addAll(this.isotopicList);
		}
		
		if (this.sensorList.size() > 0) {
			allterms.addAll(this.sensorList);
		}
		if (this.sourceList.size() > 0) {
			allterms.addAll(this.sourceList);
		}
		if (this.projectList.size() > 0) {
			allterms.addAll(this.projectList);
		}
		if (this.abstractList.size() > 0) {
			allterms.addAll(this.abstractList);
		}
		
		//System.out.println(allterms);
		/*int nsize = allterms.size();
		List<String> allcustomeTerms = new ArrayList<String>();
		for(int i=0; i<nsize; i++){
			if(allterms !=null && allterms.get(i) != null && !allterms.get(i).equals("")){
				String customterm = StringTool.customAnalyzing(this.index,allterms.get(i));
				//String customterm = allterms.get(i);
				allcustomeTerms.add(customterm);
			}
		}
		return allcustomeTerms;*/
		
		return allterms;
	}
	
	public String getKeyword(){
		return String.join(",", this.keywordList);
	}
	
	public String getTerm(){
		return String.join(",", this.termList);
	}
	
	public String getTopic(){
		return String.join(",", this.topicList);
	}
	
	public String getVariable(){
		return String.join(",", this.variableList);
	}
	
	public String getAbstract(){
		return this.abstractStr;
	}

	public void setTerms(String termstr) {
		this.splitString(termstr, this.termList);
	}

	public void setKeywords(String keywords) {
		this.splitString(keywords, this.keywordList);
	}

	public void setTopicList(String topicStr) {
		this.splitString(topicStr, this.topicList);
	}

	public void setVaraliableList(String varilableStr) {
		this.splitString(varilableStr, this.variableList);
	}

	public void splitString(String oristr, List<String> list) {
		if (oristr == null) {
			return;
		}

		int length = oristr.length();
		if (oristr.startsWith("\"")) {
			oristr = oristr.substring(1);
		}
		if (oristr.endsWith("\"")) {
			oristr = oristr.substring(0, oristr.length() - 1);
		}

		String strs[] = oristr.trim().split(",");
		if (strs != null) {
			for (int i = 0; i < strs.length; i++) {
				String str = strs[i].trim();
				if (str.startsWith(",") || str.startsWith("\"")) {
					str = str.substring(1);
				}
				if (str.endsWith(",") || str.endsWith("\"")) {
					str = str.substring(0, str.length() - 1);
				}
				if (str == "") {
					continue;
				}
				list.add(str);
			}
		}
	}

	public void splitAbstract(String abstractStr){
		Set<String> set = new HashSet<>(this.termList);
		set.addAll(this.topicList);
		set.addAll(this.variableList);
		set.addAll(this.keywordList);
		List<String> mergeList = new ArrayList<String>(set);
		
		System.out.println(mergeList);
		
	}
	
	public static PODAACMetaData parseFromLogLine(String logline) throws IOException, NoSuchAlgorithmException, InterruptedException, ExecutionException {
		// String parts[] = logline.split(", ");
		String shortname = null;
		String longname = null;
		String topic = null;
		String term = null;
		String variable = null;
		String keywordStr = null;
		int first, second, third, foutrh, fifth = 0;

		first = logline.indexOf(", ");
		second = logline.indexOf(", ", first + 1);
		third = logline.indexOf(", ", second + 1);
		foutrh = logline.indexOf(", ", third + 1);
		fifth = logline.indexOf(", ", foutrh + 1);

		shortname = logline.substring(0, first).trim();
		
		longname = logline.substring(first + 1, second).trim();
		topic = logline.substring(second + 1, third).trim();
		term = logline.substring(third + 1, foutrh).trim();
		variable = logline.substring(foutrh + 1, fifth).trim();
		keywordStr = logline.substring(fifth + 1).trim();

		return new PODAACMetaData(shortname, longname, topic, term, variable, keywordStr);
	}

	public void getAbstractByWS() throws JAXBException, IOException {
		String uri = "http://podaac.jpl.nasa.gov/ws/metadata/dataset?format=gcmd&shortName=" + this.shortname;
		System.out.println(uri);
		URL url = new URL(uri);
		HttpURLConnection connection = (HttpURLConnection) url.openConnection();
		connection.setRequestMethod("GET");
		connection.setRequestProperty("Accept", "application/xml");
		InputStream xml = connection.getInputStream();

		JAXBContext jc = JAXBContext.newInstance(DIFMetadata.class);
		DIFMetadata metadata = (DIFMetadata) jc.createUnmarshaller().unmarshal(xml);
		if(metadata.summary != null){
			this.hasAbstarct = true;
			this.abstractStr = metadata.summary.Abstract.trim();
			
			this.isoTopic = String.join(",", metadata.isoTopic);
			this.sensor = metadata.sensor.sensor_short_name.trim() + "," + metadata.sensor.sensor_long_name.trim();
			this.source = metadata.source.source_short_name.trim() + "," +  metadata.source.source_long_name.trim();
			this.project = metadata.project.project_short_name.trim() + "," + metadata.project.project_long_name.trim();
		}
		
		connection.disconnect();
	}

	public JavaRDD<PODAACMetaData> loadMetadata(String metadataFile, JavaSparkContext sc) {
		JavaRDD<PODAACMetaData> metadataRDD = sc.textFile(metadataFile).map(s -> PODAACMetaData.parseFromLogLine(s));
		JavaPairRDD<String, List<String>> dataTermsRDD = metadataRDD
				.mapToPair(new PairFunction<PODAACMetaData, String, List<String>>() {
					public Tuple2<String, List<String>> call(PODAACMetaData metadata) throws Exception {
						return new Tuple2(metadata.getShortName(), metadata.getAllTermList());
					}
				});

		return metadataRDD;
	}

	public void importMetaDataToES() throws IOException, JAXBException {

		SparkConf conf = new SparkConf().setAppName("Log Analyzer SQL");
		conf.setMaster("local[4]");
		conf.set("spark.driver.maxResultSize", "3g");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		// load meta data
		//String metadataFile = "D:/PO.DAAC_Shortname_Longname_GCMDTopic_GCMDTerm_GCMDVariable_Keywords.txt";
		String metadataFile = "D:/metadata_test.txt";
		JavaRDD<PODAACMetaData> metadataRDD = sc.textFile(metadataFile).map(s -> PODAACMetaData.parseFromLogLine(s));
		List<PODAACMetaData>  metadataList = metadataRDD.collect();
		 
		Timer timer = new Timer();
		TimerTask task = new TimerTask() {
			int count = 0;
			@Override
			public void run() {
				if (count >= metadataList.size()) {
					timer.cancel();
					try {
						importToESTask(metadataList);
					} catch (IOException e) {
						e.printStackTrace();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					return;
				}
				try {
					metadataList.get(count).getAbstractByWS();
				} catch (JAXBException | IOException e) {
					e.printStackTrace();
				}
				count += 1;
				System.out.println("every 1 seconds...");
			}
		};
		timer.schedule(task, 1000, 1000);
		sc.stop();
	}
	
	public void importToESTask(List<PODAACMetaData>  metadataList) throws IOException, InterruptedException{
		DataMiner prep = new DataMiner("podaac");
		for(int i=0; i< metadataList.size(); i++){
			PODAACMetaData metadata = metadataList.get(i);
			if(metadata.hasAbstarct){
				IndexRequest ir = new IndexRequest(prep.index, "metadataSource").source(jsonBuilder()
						.startObject()
						.field("shortName", metadata.getShortName())
						.field("longName", metadata.getLongName())
						.field("keywordStr", metadata.getKeyword())
						.field("termStr", metadata.getTerm())
						.field("topicStr", metadata.getTopic())
						.field("variableStr", metadata.getVariable())
						.field("abstractStr", metadata.getAbstract())
						.field("isotopic", metadata.getISOTopic())
						.field("sensor", metadata.getSensor())
						.field("source", metadata.getSource())
						.field("project", metadata.getProject())
						.endObject());
				ESNode.bulkProcessor.add(ir);
			}else{
				IndexRequest ir = new IndexRequest(prep.index, "without").source(jsonBuilder()
						.startObject()
						.field("shortName", metadata.getShortName())
						.field("longName", metadata.getLongName())
						.field("keywordStr", metadata.getKeyword())
						.field("termStr", metadata.getTerm())
						.field("topicStr", metadata.getTopic())
						.field("variableStr", metadata.getVariable())
						.endObject());
				ESNode.bulkProcessor.add(ir);
			}
		}
		
		ESNode.bulkProcessor.awaitClose(20, TimeUnit.MINUTES);
		ESNode.node.close();
	}
	
	 private String getProject() {
		// TODO Auto-generated method stub
		return this.project;
	}

	private String getSource() {
		// TODO Auto-generated method stub
		return this.source;
	}

	private String getSensor() {
		// TODO Auto-generated method stub
		return this.sensor;
	}

	private String getISOTopic() {
		// TODO Auto-generated method stub
		return this.isoTopic;
	}

	public void putMapping(DataMiner prep) throws IOException{
		 
		ESNode.client.admin().indices().prepareCreate("podacctest").setSettings(ImmutableSettings.settingsBuilder().loadFromSource(jsonBuilder()
			.startObject()
				.startObject("analysis")
					.startObject("filter")
						.startObject("english_stop")
							.field("type", "stop")
							.field("stopwords", "_english_s")
						.endObject()
						.startObject("english_stemmer")
							.field("type", "stemmer")
							.field("language", "english")
						.endObject()
						.startObject("english_possessive_stemmer")
							.field("type", "stemmer")
							.field("language", "possessive_english")
						.endObject()
						/*.startObject("english_keywords")
							.field("type", "keyword_marker")
							.field("keywords", "[]")
						.endObject()*/
					.endObject()

				    .startObject("analyzer")
				        .startObject("english")
							.field("tokenizer", "standard")
							//.array("filter", "[\"english_possessive_stemmer\",\"lowercase\",\"english_stop\",\"english_stemmer\"]" )
						.endObject()
				    .endObject()
			    .endObject()
			.endObject().string())).execute().actionGet();
		 
		 
		 
	    	XContentBuilder Mapping =  jsonBuilder()
					.startObject()
						.startObject("metadataProcessed")
							.startObject("properties")
								.startObject("shortName")
									.field("type", "string")
									.field("index", "not_analyzed")
								.endObject()
								.startObject("longName")
									.field("type", "string")
									.field("index", "not_analyzed")
								.endObject()
								/*.startObject("keyword")
									.field("type", "string")
									.field("index", "english")
								.endObject()*/
								.startObject("term")
									.field("type", "string")
									.field("index", "english")
								.endObject()
								.startObject("topic")
									.field("type", "string")
									.field("index", "english")
								.endObject()
								.startObject("variable")
									.field("type", "string")
									.field("index", "english")
								.endObject()
								.startObject("abstract")
									.field("type", "string")
									.field("index", "english")
								.endObject()
							.endObject()
						.endObject()
					.endObject();

	        ESNode.client.admin().indices()
			  .preparePutMapping(prep.index)
	          .setType("metadataProcessed")
	          .setSource(Mapping)
	          .execute().actionGet();
	}
	
public void exportFromES() throws IOException, InterruptedException{
		
		DataMiner prep = new DataMiner("podaac");
		//this.putMapping(prep);
	    SearchResponse scrollResp = ESNode.client.prepareSearch(prep.index)
	    		.setTypes("metadataSource")
	    		.setQuery(QueryBuilders.matchAllQuery())
	    		.setScroll(new TimeValue(60000))
	            .setSize(1).execute().actionGet(); 

		String outputFile = "D:/test/metadata.txt";
	    Writer out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile)));
	    while (true) {
	    	for (SearchHit hit : scrollResp.getHits().getHits()) {
	    		Map<String,Object> result = hit.getSource();     		
				String shortName = (String) result.get("shortName");
				String longName= (String) result.get("longName");
				String keywordStr = (String) result.get("keywordStr");
				String termStr = (String) result.get("termStr");
				String topicStr= (String) result.get("topicStr");
				String variableStr = (String) result.get("variableStr");
				String abstractStr = (String) result.get("abstractStr");	
				String isotopic= (String) result.get("isotopic");
				String sensor = (String) result.get("sensor");
				String source = (String) result.get("source");	
				String project = (String) result.get("project");	
				
				
				String jsonQuery = "{";
				jsonQuery += "\"shortName\":\"" + shortName + "\",";
				jsonQuery += "\"longName\":\"" + longName.replace("\"", "") + "\",";
				jsonQuery += "\"keywordStr\":\"" + keywordStr + "\",";
				jsonQuery += "\"termStr\":\"" + termStr + "\",";
				jsonQuery += "\"topicStr\":\"" + topicStr + "\",";
				jsonQuery += "\"variableStr\":\"" + variableStr + "\",";
				
			/*	jsonQuery += "\"isotopic\":\"" + isotopic.trim().replace("\n", "").replace("\t", "") + "\",";
				jsonQuery += "\"sensor\":\"" + sensor.trim() + "\",";
				jsonQuery += "\"source\":\"" + source.trim() + "\",";
				jsonQuery += "\"project\":\"" + project.trim() + "\",";*/
				
				jsonQuery += "\"abstractStr\":\"" + URLEncoder.encode(abstractStr, "UTF-8") + "\"";
				jsonQuery += "},";
				
				out.write(String.format("%s\n", jsonQuery));
		}
	        		         		        
	        scrollResp = ESNode.client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
	        if (scrollResp.getHits().getHits().length == 0) {
	            break;
	        }
	    }
	    
	    out.close();
	    System.out.println("done");
	    ESNode.bulkProcessor.awaitClose(20, TimeUnit.MINUTES);
	    ESNode.node.close();
	}
	 
	public static void main(String[] args) throws IOException, JSONException, NoSuchAlgorithmException, JAXBException, InterruptedException, ExecutionException {
		long startTime=System.currentTimeMillis(); 
		PODAACMetaData data = new PODAACMetaData();
		data.exportFromES();
	}

}
