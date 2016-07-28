package MUDROD.SessionRecon;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.Seconds;
import org.elasticsearch.common.joda.time.format.DateTimeFormatter;
import org.elasticsearch.common.joda.time.format.ISODateTimeFormat;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import Utils.ESNodeClient;



public class CrawlerDetection {
	public static final String GoogleBot = "gsa-crawler (Enterprise; T4-JPDGU3TRCQAXZ; earthdata-sa@lists.nasa.gov,srinivasa.s.tummala@nasa.gov)";
	public static final String GoogleBot21 = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)";
	public static final String BingBot ="Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)";
	public static final String YahooBot ="Mozilla/5.0 (compatible; Yahoo! Slurp; http://help.yahoo.com/help/us/ysearch/slurp)";
	public static final String RogerBot ="rogerbot/1.0 (http://www.moz.com/dp/rogerbot, rogerbot-crawler@moz.com)";
	public static final String YacyBot ="yacybot (/global; amd64 Windows Server 2008 R2 6.1; java 1.8.0_31; Europe/de) http://yacy.net/bot.html";	
	public static final String YandexBot ="Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots)";
	public static final String GoogleImage ="Googlebot-Image/1.0";
	public static final String RandomBot1 ="Mozilla/5.0 (iPhone; CPU iPhone OS 6_0 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Version/6.0 Mobile/10A5376e Safari/8536.25 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)";
	public static final String NoAgentBot = "-";

	public static final String PerlBot = "libwww-perl/";	
	public static final String ApacheHTTP ="Apache-HttpClient/";
	public static final String JavaHTTP ="Java/";
	public static final String cURL ="curl/";

	private String index = null;
	private String Cleanup_type = null;
	private String HTTP_type = null;
	private String FTP_type = null;
	private ESNodeClient esnode;

	public CrawlerDetection(){
			
	}
	
	public CrawlerDetection(Preprocesser processor) {
		this.index = processor.index;
		this.Cleanup_type = processor.Cleanup_type;
		this.HTTP_type = processor.HTTP_type;
		this.FTP_type = processor.FTP_type;
		this.esnode = processor.esnode;
	}
	
	public CrawlerDetection(String index, String HTTP_type, String FTP_type, String Cleanup_type){
		this.index = index;
		this.HTTP_type = HTTP_type;
		this.FTP_type = FTP_type;
		this.Cleanup_type = Cleanup_type;		
	}

	public boolean CheckKnownCrawler(String agent){
		if(agent.equals(GoogleBot)||agent.equals(GoogleBot21)||agent.equals(BingBot)||agent.equals(YahooBot)||agent.equals(RogerBot)||
				agent.equals(YacyBot)||agent.equals(YandexBot)||agent.equals(GoogleImage)||agent.equals(RandomBot1)||agent.equals(NoAgentBot)||
				agent.contains(PerlBot)||agent.contains(ApacheHTTP)||agent.contains(JavaHTTP)||agent.contains(cURL))
		{
			return true;
		}else{
			return false;
		}
	}

	public void CheckByRate(int rate) throws InterruptedException, IOException{	
		SearchResponse sr = esnode.client.prepareSearch(index)                          
				.setTypes(HTTP_type)
				.setQuery(QueryBuilders.matchAllQuery())
				.setSize(0)
				.addAggregation(AggregationBuilders.terms("Users").field("IP").size(0))
				.execute().actionGet();
		Terms Users = sr.getAggregations().get("Users");

		int user_count = 0;

		Pattern pattern = Pattern.compile("get (.*?) http/*");
		Matcher matcher;
		for (Terms.Bucket entry : Users.getBuckets()) {		    
			FilterBuilder filter_search = FilterBuilders.boolFilter()
					.must(FilterBuilders.termFilter("IP", entry.getKey()));
			QueryBuilder  query_search = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), filter_search);

			/*SearchResponse checkRobot = esnode.client.prepareSearch(index)
					.setTypes(HTTP_type,FTP_type)
					.setQuery(query_search)
					.setSize(0)
					.setRouting(entry.getKey())
					.addAggregation(AggregationBuilders.dateHistogram("by_minute").field("Time").interval((DateHistogram.Interval.MINUTE)).order(DateHistogram.Order.COUNT_DESC))
					.execute().actionGet();*/
			
			SearchRequestBuilder checkRobotBuilder = esnode.client.prepareSearch(index)
					.setTypes(HTTP_type,FTP_type)
					.setQuery(query_search)
					.setSize(0)
					.addAggregation(AggregationBuilders.dateHistogram("by_minute").field("Time").interval((DateHistogram.Interval.MINUTE)).order(DateHistogram.Order.COUNT_DESC))
					;
			
			if(esnode.bRouting){
				checkRobotBuilder.setRouting(entry.getKey());
			}
			
			SearchResponse checkRobot =  checkRobotBuilder.execute().actionGet();

			DateHistogram agg = checkRobot.getAggregations().get("by_minute");

			List<? extends Bucket> botList = agg.getBuckets();
			long max_count = botList.get(0).getDocCount();
			if(max_count>=rate){                                                 //variable one
				//bw.write(entry.getKey()+"\n");
				//System.out.print(Long.toString(max_count)+"\n");
			}
			else
			{
				user_count++;	
				DateTime dt1 = null;
				int toLast = 0;
				SearchResponse scrollResp = esnode.client.prepareSearch(index)
						.setTypes(HTTP_type,FTP_type)
						.setScroll(new TimeValue(60000))
						.setQuery(query_search)
						.setSize(100).execute().actionGet(); //100 hits per shard will be returned for each scroll


				while (true) {
					for (SearchHit hit : scrollResp.getHits().getHits()) {
						Map<String,Object> result = hit.getSource(); 
						String logtype = (String) result.get("LogType");
						if(logtype.equals("PO.DAAC")){
							String request = (String) result.get("Request");
							matcher = pattern.matcher(request.trim().toLowerCase());
							boolean find = false;
							while (matcher.find()) {
								request = matcher.group(1);
								result.put("RequestUrl", "http://podaac.jpl.nasa.gov" + request);
								find = true;
							}
							if(!find){
								result.put("RequestUrl", request);
							}
						}else{
							result.put("RequestUrl",(String) result.get("Request"));
						}

						DateTimeFormatter fmt = ISODateTimeFormat.dateTime();
						DateTime dt2 = fmt.parseDateTime((String) result.get("Time"));

						if(dt1==null){
							toLast = 0;
						}else{
							toLast = Math.abs(Seconds.secondsBetween(dt1, dt2).getSeconds());
						}
						result.put("ToLast", toLast);			          
						IndexRequest ir = new IndexRequest(index, Cleanup_type).source(result);
						if(esnode.bRouting){
							ir.routing(entry.getKey());
						}

						esnode.bulkProcessor.add(ir); 			              
						dt1 = dt2;
					}

					scrollResp = esnode.client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
					if (scrollResp.getHits().getHits().length == 0) {
						break;
					}
				}

			}
		}

		System.out.println("Clean Up is done." + "The number of users:"+ Integer.toString(user_count));
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
