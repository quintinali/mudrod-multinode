package MUDROD.SessionRecon;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutionException;
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
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import Utils.ESNodeClient;
import Utils.StringTool;


public class SessionStatistic {

	String index;
	String cleanup_type;
	String session_type;
	Preprocesser processor;
	private ESNodeClient esnode;

	public SessionStatistic(Preprocesser processor) {
		this.index = processor.index;
		this.cleanup_type = processor.Cleanup_type;
		this.session_type = processor.SessionStats;
		this.processor = processor;
		this.esnode = processor.esnode;
	}

	public void processSession() throws IOException, InterruptedException, ExecutionException {
		String inputType = this.cleanup_type;
		String outputType = this.session_type;

		MetricsAggregationBuilder StatsAgg = AggregationBuilders.stats("Stats").field("Time");
		SearchResponse sr = esnode.client.prepareSearch(this.index).setTypes(inputType)
				.setQuery(QueryBuilders.matchAllQuery())
				.addAggregation(AggregationBuilders.terms("Sessions").field("SessionID").size(0).subAggregation(
						StatsAgg))
				.execute().actionGet();

		Terms Sessions = sr.getAggregations().get("Sessions");
		DateTimeFormatter fmt = ISODateTimeFormat.dateTime();
		String min = null;
		String max = null;
		DateTime start = null;
		DateTime end = null;
		int duration = 0;
		float request_rate = 0;

		//int TEST = Sessions.getBuckets().size();
		//System.out.println(TEST);
		int session_count = 0;
		Pattern pattern = Pattern.compile("get (.*?) http/*");
		for (Terms.Bucket entry : Sessions.getBuckets()) {
			//System.out.println(entry.getKey());
			if (entry.getDocCount() >= 3 && !entry.getKey().equals("invalid")) {

				Stats agg = entry.getAggregations().get("Stats");
				min = agg.getMinAsString();
				max = agg.getMaxAsString();
				start = fmt.parseDateTime(min);
				end = fmt.parseDateTime(max);
	
				duration = Seconds.secondsBetween(start, end).getSeconds();

				int searchDataListRequest_count = 0;
				int searchDataRequest_count = 0;
				int searchDataListRequest_byKeywords_count = 0;
				int ftpRequest_count = 0;
				int keywords_num = 0;
				String routing = entry.getKey().split("@")[0];
				String IP = null;
				String keywords = "";
				String views = "";
				String downloads = "";
				FilterBuilder filter_search = FilterBuilders.boolFilter()
						.must(FilterBuilders.termFilter("SessionID", entry.getKey()));
				QueryBuilder query_search = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), filter_search);

				/*SearchResponse scrollResp = esnode.client.prepareSearch(this.index)
						.setTypes(inputType).setScroll(new TimeValue(60000)).setQuery(query_search).setSize(100).setRouting(routing)
						.execute().actionGet();*/
				
				
				SearchRequestBuilder scrollBuilder = esnode.client.prepareSearch(this.index)
						.setTypes(inputType).setScroll(new TimeValue(60000)).setQuery(query_search).setSize(100);
						
				if(esnode.bRouting){
					scrollBuilder.setRouting(routing);
				}
				SearchResponse scrollResp = scrollBuilder.execute().actionGet();

				while (true) {
					
					for (SearchHit hit : scrollResp.getHits().getHits()) {
						Map<String, Object> result = hit.getSource();

						String request = (String) result.get("Request");
						String logType = (String) result.get("LogType");
						IP = (String) result.get("IP");
						Matcher matcher = pattern.matcher(request.trim().toLowerCase());
						while (matcher.find()) {
							request = matcher.group(1);
						}
						//Map<String, String> mapRequest = RequestUrl.URLRequest(request);

						String datasetlist = "/datasetlist?";
						String dataset = "/dataset/";
						if (request.contains(datasetlist)) {
							searchDataListRequest_count++;

							String info = RequestUrl.GetSearchInfo(request) + ",";

							if(!info.equals(",")){
								if(keywords.equals("")){	    							
									keywords = keywords + info;
								}else{
									String[] items = info.split(",");
									String[] keyword_list = keywords.split(",");
									for(int m = 0; m<items.length; m++){
										//if(!keywords.contains(items[m])){
										if(!Arrays.asList(keyword_list).contains(items[m])){
											keywords = keywords +items[m]+",";		    								
										}	
									}
								}
							}

						}
						if (request.startsWith(dataset)) {
							searchDataRequest_count++;
							if (findDataset(request) != null) {
								String view = findDataset(request);

								if (views.equals("")) {
									views = view;
								} else {
									if (views.contains(view)) {

									} else {
										views = views + "," + view;
									}
								}
							}
						}
						if (logType.equals("ftp")) {
							ftpRequest_count++;
							String download = "";
							String request_lowercase = request.toLowerCase();
							if (request_lowercase.endsWith(".jpg") == false
									&& request_lowercase.endsWith(".pdf") == false
									&& request_lowercase.endsWith(".txt") == false
									&& request_lowercase.endsWith(".gif") == false) {
								download = request;
							}

							if (downloads.equals("")) {
								downloads = download;
							} else {
								if (downloads.contains(download)) {

								} else {
									downloads = downloads + "," + download;
								}
							}
						}

					}

					scrollResp = esnode.client.prepareSearchScroll(scrollResp.getScrollId())
							.setScroll(new TimeValue(600000)).execute().actionGet();
					
					//System.out.println( "another loop");
					// Break condition: No hits are returned
					if (scrollResp.getHits().getHits().length == 0) {
						break;
					}
				}

				if(!keywords.equals("")){
					keywords_num = keywords.split(",").length;
				}
				
				//System.out.println(keywords);
				//System.out.println(keywords_num);
				//System.out.println(start);
				//System.out.println(end);

				//if (searchDataListRequest_count != 0 && searchDataRequest_count != 0 && ftpRequest_count != 0 && keywords_num<50) {
				if (searchDataListRequest_count != 0 && searchDataListRequest_count <=100 && searchDataRequest_count != 0 && searchDataRequest_count <=200 && ftpRequest_count <=100) {
					//GeoIp converter = new GeoIp();
					//Coordinates loc = converter.toLocation(IP);
					String sessionURL = "443/logmining/?sessionid=" + entry.getKey() + "&sessionType=" + outputType
							+ "&requestType=" + inputType;
					session_count++;

					IndexRequest ir = new IndexRequest(this.index, outputType).source(jsonBuilder().startObject()
							.field("SessionID", entry.getKey()).field("SessionURL", sessionURL)
							.field("Request_count", entry.getDocCount()).field("Duration", duration)
							.field("Number of Keywords", keywords_num)
							.field("Time", min).field("End_time", max)
							.field("searchDataListRequest_count", searchDataListRequest_count)
							.field("searchDataListRequest_byKeywords_count", searchDataListRequest_byKeywords_count)
							.field("searchDataRequest_count", searchDataRequest_count).field("keywords", StringTool.customAnalyzing(this.index,keywords))
							.field("views", views).field("downloads", downloads).field("request_rate", request_rate)
							.field("Comments", "").field("Validation", 0).field("Produceby", 0).field("Correlation", 0)
							.field("IP", IP)/*.field("Coordinates", loc.latlon)*/.endObject());

					esnode.bulkProcessor.add(ir);
				}
			}
		}

		System.out.println("Session Processing is done." + "The number of sessions:" + Integer.toString(session_count));
	}
/*	
	public String customAnalyzing(String str) throws InterruptedException, ExecutionException{
		String[] str_list = str.split(",");
		for(int i = 0; i<str_list.length;i++)
		{
			String tmp = "";
			AnalyzeResponse r = ESNode.client.admin().indices()
		            .prepareAnalyze(str_list[i]).setIndex(index).setAnalyzer("cody")
		            .execute().get();
			for (AnalyzeToken token : r.getTokens()) {
				tmp +=token.getTerm() + " ";
		    }
			str_list[i] = tmp.trim();
		}
		
		String analyzed_str = String.join(",", str_list);
		return analyzed_str;
	}*/

	public String findDataset(String request) {
		String pattern1 = "/dataset/";
		String pattern2;
		if (request.contains("?")) {
			pattern2 = "?";
		} else {
			pattern2 = " ";
		}

		Pattern p = Pattern.compile(Pattern.quote(pattern1) + "(.*?)" + Pattern.quote(pattern2));
		Matcher m = p.matcher(request);
		if (m.find()) {
			return m.group(1);
		}
		return null;
	}
}