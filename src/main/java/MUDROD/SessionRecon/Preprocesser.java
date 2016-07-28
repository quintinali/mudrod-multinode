package MUDROD.SessionRecon;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.Serializable;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.codehaus.jettison.json.JSONException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.index.query.QueryBuilders;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import Utils.DeleteRedType;
import Utils.ESNodeClient;
import Utils.FileUtil;
import Utils.MappingConfig;
import Utils.SparkNode;
import Utils.StringTool;
import Utils.jobUtil;
import scala.Tuple2;

//MUDROD.SessionRecon.Preprocesser;
public class Preprocesser implements Serializable  {
	
	public String index;
	public String HTTP_type = "rawhttp";
	public String FTP_type = "rawftp";
	public String Cleanup_type = "cleanupLog";
	public String SessionStats = "sessionStatic";
	public String PODAACkeywordsType = "podaacKeywords";
	public ESNodeClient esnode;
	public Map<String,String> config;
	//String workflowUrl = "http://localhost:8080/MudrodUtil/";
	
	public Preprocesser() throws IOException {

	}
	
	public Preprocesser(String Index) throws IOException {
		this.index = Index;
	}
	
	public Preprocesser(ESNodeClient node,String index) throws IOException {
		this.index = index;
		this.esnode = node;
	}
	
	public Preprocesser(ESNodeClient node, Map<String,String> configMap) throws IOException {
		this.index = configMap.get("indexName");
		this.esnode = node;
		this.config = configMap;
	}
	
	public Preprocesser(Map<String,String> configMap) throws IOException {
		this.index = configMap.get("indexName");
		this.config = configMap;	
	}

	public void Read(String HTTPfileName, String FTPfileName) throws ParseException, IOException, InterruptedException
	{	
		/*String time_suffix = HTTPfileName.substring(Math.max(HTTPfileName.length() - 6, 0));
		HTTP_type = "rawhttp" + time_suffix;
		FTP_type = "rawftp" + time_suffix;
		Cleanup_type = "cleanupLog" + time_suffix;
		SessionStats = "sessionstats" + time_suffix;*/
		
		ImportLogFile imp = new ImportLogFile(this.esnode);
		imp.ReadLogFile(HTTPfileName, "http", index, HTTP_type);
		System.out.println(HTTPfileName);
		
		imp.ReadLogFile(FTPfileName, "FTP", index, FTP_type);
		System.out.println(FTPfileName);
		
		esnode.RefreshIndex();
	}
	
	public void LoadLog(String HTTPfileName, String FTPfileName, JavaSparkContext jsc) throws ParseException, IOException, InterruptedException
	{	
		/*String time_suffix = HTTPfileName.substring(Math.max(HTTPfileName.length() - 6, 0));
		HTTP_type = "rawhttp" + time_suffix;
		FTP_type = "rawftp" + time_suffix;
		Cleanup_type = "cleanupLog" + time_suffix;
		SessionStats = "sessionstats" + time_suffix;*/
		
		JavaRDD<ApacheAccessLog> accessLogRDD = jsc.textFile(HTTPfileName).map(s -> ApacheAccessLog.parseFromLogLine(this, s))
				.filter(WebLog::checknull)
				.repartition(2); // Optionally, change this.;
		
		
		accessLogRDD.foreachPartition(new VoidFunction<Iterator<ApacheAccessLog>>() {
			@Override
			public void call(Iterator<ApacheAccessLog> results) throws Exception {
				// TODO Auto-generated method stub
				
				while (results.hasNext()) {
					ApacheAccessLog log = results.next();
					IndexRequest ir = new IndexRequest(index, HTTP_type).source(jsonBuilder()
							.startObject()
							.field("IP", log.IP)
							.field("Time", log.Time)
							.field("Request", log.Request)
							.field("Response", log.Response)
							.field("Bytes", log.Bytes)
							.field("Referer", log.Referer)
							.field("Browser", log.Browser)
							.endObject()).routing(log.IP);
					esnode.bulkProcessor.add(ir);
				}
			}
		});
	
		esnode.RefreshIndex();
	}

	public void splitLog(String HTTPfileName,JavaSparkContext jsc){
		
		String time_suffix = config.get("year");
		String logtype = config.get("logtype");
		String logname = logtype.equals("access") ? "access_log" : "vsftpd.log";	
		JavaRDD<String> accessLogRDD = jsc.textFile(HTTPfileName);

		for(int i =1 ;i<13; i++)  //attention start from 2
		{ 
			String month = null;
		
			if(i<10){
				month = "0" + Integer.toString(i);
			}else{
				month = Integer.toString(i);
			}
			
			String outputFile = "/tmp/splitlog/" + logname + time_suffix + month + ".txt";
			int intmonth = i;
			JavaRDD<String> monthLogRDD = accessLogRDD.filter(new Function<String, Boolean>() {
				public Boolean call(String s) throws Exception {
					String time = s;
					int logmonth = 0;
					if (time.contains("/Jan/") || time.contains(" Jan ")){
						logmonth = 1;
					}else if (time.contains("/Feb/") || time.contains(" Feb ")){
						logmonth = 2;
					}else if (time.contains("/Mar/") || time.contains(" Mar ")){
						logmonth = 3;
					}else if (time.contains("/Apr/") || time.contains(" Apr ")){
						logmonth = 4;
					}else if (time.contains("/May/") || time.contains(" May ")){
						logmonth = 5;
					}else if (time.contains("/Jun/") || time.contains(" Jun ")){
						logmonth = 6;
					}else if (time.contains("/Jul/") || time.contains(" Jul ")){
						logmonth = 7;
					}else if (time.contains("/Aug/") || time.contains(" Aug ")){
						logmonth = 8;
					}else if (time.contains("/Sep/") || time.contains(" Sep ")){
						logmonth = 9;
					}else if (time.contains("/Oct/") || time.contains(" Oct ")){
						logmonth = 10;
					}else if (time.contains("/Nov/") || time.contains(" Nov ")){
						logmonth = 11;
					}else if (time.contains("/Dec/") || time.contains(" Dec ")){
						logmonth = 12;
					}
					
					if(logmonth == intmonth){
						return true;
					}
					return false;
				}
			});
			monthLogRDD.coalesce(1,true).saveAsTextFile(outputFile);
		}
	}

	public void RemoveCrawlers(int rate) throws InterruptedException, IOException
	{
		CrawlerDetection crawlerDec = new CrawlerDetection(this);
		crawlerDec.CheckByRate(rate);
		esnode.RefreshIndex();
	}
	
	public void GenerateSession(int timeThres) throws ElasticsearchException, IOException
	{
		SessionGenerator generator = new SessionGenerator(this, timeThres);
		generator.genSessionByReferer();
		esnode.RefreshIndex();

		generator.combineShortSessions();
		esnode.RefreshIndex();
	}
	
	public void CalSessionStats() throws IOException, InterruptedException, ExecutionException
	{		
		SessionStatistic sta = new SessionStatistic(this);
		sta.processSession();
		esnode.RefreshIndex();
	}
	
	public void DeleteRedTyeps(){
		DeleteRedType drt = new DeleteRedType(this.esnode);
		drt.deleteTypeByMapping(index, HTTP_type);
		drt.deleteTypeByMapping(index, FTP_type);
		esnode.RefreshIndex();
	}
	
	public void processMonthlyData(String month) throws ParseException, IOException, InterruptedException, ExecutionException{
		long monthStartTime=System.currentTimeMillis();
		long monthEndTime=0;
		
		StringTool.initESNode(esnode);//important!
		esnode.InitiateIndex(config);
		esnode.customIndex(index, config);
		
		String HTTPfileName = config.get("accesslog") + "." + month;
		String FTPfileName = config.get("ftplog") + "." +  month;
	
		long setpStartTime=System.currentTimeMillis();
		long setpEndTime=0;
		
		String time_suffix = HTTPfileName.substring(Math.max(HTTPfileName.length() - 6, 0));
		setpStartTime=System.currentTimeMillis();
		this.Read(HTTPfileName,FTPfileName);
		setpEndTime=System.currentTimeMillis();
		System.out.println(time_suffix +" reading log is done!" +  "Time elapsed： "+ (setpEndTime-setpStartTime)/1000+"s");
		
		setpStartTime = setpEndTime;
		this.RemoveCrawlers(30); //attention
		setpEndTime=System.currentTimeMillis();
		System.out.println(time_suffix + " RemoveCrawlers is done!"  + "Time elapsed： "+ (setpEndTime-setpStartTime)/1000+"s");
	   	
		setpStartTime = setpEndTime;
		this.GenerateSession(600);
		setpEndTime=System.currentTimeMillis();
		System.out.println(time_suffix + " GenerateSession is done!"  + "Time elapsed： "+ (setpEndTime-setpStartTime)/1000+"s");
		
		setpStartTime = setpEndTime;
		this.CalSessionStats(); 
		setpEndTime=System.currentTimeMillis();
		System.out.println(time_suffix +" CalSessionStats is done!"  + "Time elapsed： "+ (setpEndTime-setpStartTime)/1000+"s");
		
		setpStartTime = setpEndTime;
		this.DeleteRedTyeps();
		setpEndTime=System.currentTimeMillis();
		System.out.println(time_suffix +" DeleteRedTyeps is done!"  + "Time elapsed： "+ (setpEndTime-setpStartTime)/1000+"s");
	
		//close index
		esnode.closeIndex(index);
		
		monthEndTime=System.currentTimeMillis();
		System.out.println(time_suffix +" process done!"  + "Time elapsed： "+ (monthEndTime-monthStartTime)/1000+"s");
	}

	public HashMap<String,String> readConfig() throws JSONException, IOException {
		HashMap<String,String> config = new HashMap<String,String>();
		BufferedReader br = new BufferedReader(new FileReader("../podacclog/config.json"), 819200);
		int count =0;
		try {
			String line = br.readLine();
		    while (line != null) {	
		    	if(line.equals("{") || line.equals("}"))
		    	{
		    		line = br.readLine();
		    		continue;
		    	}	
		    	else{
		    		String[] parts = line.trim().split(":");
		    		String filed = parts[0];
		    		String value =  parts[1];
		    		value = value.endsWith(",") ? value.substring(0, value.length()-1) : value;
		    		config.put(filed, value);
		    	}
		    	
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
		}
		
		return config;
	}

	public static void main(String[] args) throws ParseException, IOException, InterruptedException, ExecutionException {

		// TODO Auto-generated method stub
		long startTime=System.currentTimeMillis(); 
		String configFile = "";
		if(args.length ==0){
			configFile = "../podacclog/config.json";
		}else{
			configFile = args[0];
		}

		Map<String,String> config = StringTool.readConfig(configFile);
		
		//for workflow
		 if(config.get("workflow") !=null && config.get("workflow").equals("true")){
			 String jobname = config.get("jobname");
			 String nodetag = config.get("nodetag");
			 String workflowUrl = config.get("workflowUrl");
			 jobUtil.addJob(workflowUrl, jobname, nodetag, "processlog",startTime);
		}
		 
		//for split logs
		if(config.get("splitlog")!=null && config.get("splitlog").equals("true")){
			FileUtil fileutil= new FileUtil();
			String months = fileutil.mvSplitfile(config);
			config.put("month", months);
			//System.out.println(config.get("month"));
		}
			
		String oriIndex = config.get("indexName");
		ESNodeClient esnode = new ESNodeClient(config);
		
		System.out.println(esnode.bRouting);
		if(config.get("file").equals("1")){
			String strmonth = config.get("month");
			String[] months = strmonth.split(",");
			for(int i=0; i<months.length; i++){
				config.put("indexName", oriIndex + months[i]);
				Preprocesser test = new Preprocesser(esnode, config);
				test.processMonthlyData(months[i]);
				config.put("indexName", oriIndex);
			}
		}
		else if(config.get("file").equals("2")){
			String stryear= config.get("year");
			String[] years = stryear.split(",");
			for(int j=0; j<years.length; j++){
				String year = years[j];
				for(int i =1 ;i<13; i++)  //attention start from 2
				{ 
					String month = null;
				
					if(i<10){
						month = "0" + Integer.toString(i);
					}else{
						month = Integer.toString(i);
					}
					config.put("indexName", oriIndex + year + month);
					Preprocesser test = new Preprocesser(esnode, config);
					test.processMonthlyData(year + month);
					config.put("indexName", oriIndex);
				}
			}
		}

		esnode.bulkProcessor.awaitClose(20, TimeUnit.MINUTES);
		esnode.node.close();  
        
		long endTime=System.currentTimeMillis();
		long lasttime=(endTime-startTime)/1000;
		System.out.println("Preprocessing is done!" + "Time elapsed： "+ lasttime+"s");
		
		//for workflow
		 if(config.get("workflow") !=null && config.get("workflow").equals("true")){
			 String jobname = config.get("jobname");
			 String nodetag = config.get("nodetag");
			 String workflowUrl = config.get("workflowUrl");
			 jobUtil.stopJob(workflowUrl, jobname, nodetag, "processlog",lasttime);
		}
	}


	public static void main1(String[] args) throws ParseException, IOException, InterruptedException, ExecutionException {
		// TODO Auto-generated method stub
		long startTime=System.currentTimeMillis(); 
		String configFile = "";
		if(args.length ==0){
			configFile = "../podacclog/config.json";
		}else{
			configFile = args[0];
		}
		
		Map<String,String> config = StringTool.readConfig(configFile);
		String filename = config.get("filename");
		
		SparkNode spark = new SparkNode();
		Preprocesser test = new Preprocesser(config);
		test.splitLog(filename, spark.sc);
        
		long endTime=System.currentTimeMillis();
		System.out.println("Preprocessing is done!" + "Time elapsed： "+ (endTime-startTime)/1000+"s");
	}
}
