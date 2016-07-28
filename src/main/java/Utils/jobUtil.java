package Utils;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


public class jobUtil {
	String index;
	String type;
	ESNodeClient esnode;
	Map<String,String> config;
	
	public jobUtil() throws IOException {
		// TODO Auto-generated constructor stub
		this.index = "jobmonitor";
		this.type = "job";
	}
	
	public static void addJob(String workflowUrl, String jobname, String nodetag, String step, long startTime) {

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MMM-dd-HH:mm:ss");
		Date resultdate = new Date(startTime);
		String sDate = sdf.format(resultdate);
		String url = workflowUrl + "startJob?jobname=" + jobname + "&step=" + step + "&nodeip=" + nodetag;
		System.out.println(url);
		JsonObject retObj = jobUtil.invokeURL(url);

	}

	public static void stopJob(String workflowUrl, String jobname, String nodetag,  String step, long lastTime) {

		String url = workflowUrl + "updateJobStep?jobname=" + jobname + "&step=" + step + "&lapsetime=" + lastTime
				+ "&nodeip=" + nodetag;
		System.out.println(url);
		jobUtil.invokeURL(url);

	}
	
	public static JsonObject invokeURL(String url){
		
		URL gwtServlet = null;
		String result = "";
	    try {
			URL obj = new URL(url);
			HttpURLConnection con = (HttpURLConnection) obj.openConnection();
			con.setRequestMethod("GET");
			int responseCode = con.getResponseCode();
			//System.out.println("\nSending 'GET' request to URL : " + url);
			//System.out.println("Response Code : " + responseCode);
			BufferedReader in = new BufferedReader(
			        new InputStreamReader(con.getInputStream()));
			String inputLine;
			StringBuffer response = new StringBuffer();

			while ((inputLine = in.readLine()) != null) {
				response.append(inputLine);
			}
			in.close();
			System.out.println(response.toString());
			result = response.toString();

	    } catch (MalformedURLException e) {
	        // TODO Auto-generated catch block
	        e.printStackTrace();
	    } catch (IOException e) {
	        // TODO Auto-generated catch block
	        e.printStackTrace();
	    }
	    
	    JsonParser parser = new JsonParser();
	    JsonObject json = (JsonObject) parser.parse(result);
	    
	    return json;
	}

}
