package MUDROD.SessionRecon;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.elasticsearch.action.index.IndexRequest;
import Utils.ESNodeClient;

public class ImportLogFile {
	
	private ESNodeClient esnode;
	public ImportLogFile(ESNodeClient esnode) {
		this.esnode = esnode;
	}
	
	String logEntryPattern = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+|-) \"((?:[^\"]|\")+)\" \"([^\"]+)\"";
	public static final int NUM_FIELDS = 9;
	Pattern p = Pattern.compile(logEntryPattern);
	Matcher matcher;

	public String SwithtoNum(String time){
		if (time.contains("Jan")){
			time = time.replace("Jan", "1");   
		}else if (time.contains("Feb")){
			time = time.replace("Feb", "2");   
		}else if (time.contains("Mar")){
			time = time.replace("Mar", "3");   
		}else if (time.contains("Apr")){
			time = time.replace("Apr", "4");   
		}else if (time.contains("May")){
			time = time.replace("May", "5");   
		}else if (time.contains("Jun")){
			time = time.replace("Jun", "6");   
		}else if (time.contains("Jul")){
			time = time.replace("Jul", "7");   
		}else if (time.contains("Aug")){
			time = time.replace("Aug", "8");   
		}else if (time.contains("Sep")){
			time = time.replace("Sep", "9");   
		}else if (time.contains("Oct")){
			time = time.replace("Oct", "10");   
		}else if (time.contains("Nov")){
			time = time.replace("Nov", "11");
		}else if (time.contains("Dec")){
			time = time.replace("Dec", "12");
		}

		return time;
	}
	
	public void ReadLogFile(String fileName, String Type, String index, String type) throws ParseException, IOException, InterruptedException{
		//BufferedReader br = new BufferedReader(new FileReader(fileName));
		BufferedReader br = new BufferedReader(new FileReader(fileName), 819200);
		int count =0;
		try {
			String line = br.readLine();
		    while (line != null) {	
		    	if(Type.equals("FTP"))
		    	{
		    		ParseSingleLineFTP(line, index, type);
		    	}else{
		    		ParseSingleLineHTTP(line, index, type);
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
		    System.out.println("done reading " + Type + "file!" + count);
		}
	}

	public void ParseSingleLineFTP(String log, String index, String type) throws IOException, ParseException{
		String ip = log.split(" +")[6];

		String time = log.split(" +")[1] + ":"+log.split(" +")[2] +":"+log.split(" +")[3]+":"+log.split(" +")[4];

		time = SwithtoNum(time);
		SimpleDateFormat formatter = new SimpleDateFormat("MM:dd:HH:mm:ss:yyyy");
		Date date = formatter.parse(time);
		String bytes = log.split(" +")[7];
		
		String request = log.split(" +")[8].toLowerCase();
		
		if(!request.contains("/misc/") && !request.contains("readme"))
		{
		IndexRequest ir = new IndexRequest(index, type).source(jsonBuilder()
				.startObject()
				.field("LogType", "ftp")
				.field("IP", ip)
				.field("Time", date)
				.field("Request", request)
				.field("Bytes", Long.parseLong(bytes))
				.endObject())/*.routing(ip)*/;

		if(esnode.bRouting){
			ir.routing(ip);
		}
		esnode.bulkProcessor.add(ir);
		}

	}

	public void ParseSingleLineHTTP(String log, String index, String type) throws IOException, ParseException{
		matcher = p.matcher(log);
		if (!matcher.matches() || 
				NUM_FIELDS != matcher.groupCount()) {
			//bw.write(log+"\n");
			return;
		}
		String time = matcher.group(4);
		time = SwithtoNum(time);
		SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
		Date date = formatter.parse(time);

		String bytes = matcher.group(7);
		if(bytes.equals("-")){
			bytes="0";
		}

		String request = matcher.group(5).toLowerCase();
		String agent = matcher.group(9);
		CrawlerDetection crawlerDe = new CrawlerDetection();
		if(crawlerDe.CheckKnownCrawler(agent))
		{

		}
		else
		{
			if(request.contains(".js")||request.contains(".css")||request.contains(".jpg")||request.contains(".png")||request.contains(".ico")||
					request.contains("image_captcha")||request.contains("autocomplete")||request.contains(".gif")||
					request.contains("/alldata/")||request.contains("/api/")||request.equals("get / http/1.1")||
					request.contains(".jpeg")||request.contains("/ws/"))   //request.contains("/ws/")  need to be discussed
			{

			}else{
				IndexRequest ir = new IndexRequest(index, type).source(jsonBuilder()
						.startObject()
						.field("LogType", "PO.DAAC")
						.field("IP", matcher.group(1))
						.field("Time", date)
						.field("Request", matcher.group(5))
						.field("Response", matcher.group(6))
						.field("Bytes", Integer.parseInt(bytes))
						.field("Referer", matcher.group(8))
						.field("Browser", matcher.group(9))
						.endObject())/*.routing(matcher.group(1))*/;
				
				if(esnode.bRouting){
					ir.routing(matcher.group(1));
				}

				esnode.bulkProcessor.add(ir);

			}
		}		
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
