package MUDROD.SessionRecon;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
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
import org.elasticsearch.search.sort.SortOrder;
import Utils.ESNodeClient;

public class SessionGenerator {
	String index;
	String cleanup_type;
	String session_type;
	int timeThres; // should be used
	private ESNodeClient esnode;
	
	public SessionGenerator(Preprocesser processor, int timeThres) {
		this.index = processor.index;
		this.cleanup_type = processor.Cleanup_type;
		this.session_type = processor.SessionStats;
		this.esnode = processor.esnode;
		this.timeThres = timeThres;
	}

	public void genSessionByReferer() throws ElasticsearchException, IOException {
		SearchResponse sr = esnode.client.prepareSearch(this.index).setTypes(this.cleanup_type)
				.setQuery(QueryBuilders.matchAllQuery()).setSize(0)
				.addAggregation(AggregationBuilders.terms("Users").field("IP").size(0)).execute().actionGet();
		Terms Users = sr.getAggregations().get("Users");

		int session_count = 0;
		for (Terms.Bucket entry : Users.getBuckets()) {
			
			// test
			/*if(!entry.getKey().equals("157.249.83.68")){
				//91.98.217.94 
				continue; 
			}*/
			
			
			String start_time = null;
			int session_count_in = 0;

			FilterBuilder filter_search = FilterBuilders.boolFilter()
					.must(FilterBuilders.termFilter("IP", entry.getKey()));
			
			QueryBuilder query_search = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), filter_search);
			

			SearchRequestBuilder scrollBuilder = esnode.client.prepareSearch(this.index).setTypes(this.cleanup_type)
					.setScroll(new TimeValue(60000)).setQuery(query_search).addSort("Time", SortOrder.ASC) // important																					// !!
					.setSize(100);

			if(esnode.bRouting){
				scrollBuilder.setRouting(entry.getKey());
			}

			SearchResponse scrollResp = scrollBuilder.execute().actionGet();		
			Map<String, Map<String, DateTime>> sessionReqs = new HashMap<String, Map<String, DateTime>>();
			String request = "";
			String referer = "";
			String logType = "";
			String id = "";
			String ip = entry.getKey();
			String indexUrl = "http://podaac.jpl.nasa.gov/";
			DateTime time = null;
			DateTimeFormatter fmt = ISODateTimeFormat.dateTime();
			// DateTime dt2 = fmt.parseDateTime((String) result.get("Time"));
			while (scrollResp.getHits().getHits().length != 0) {
				for (SearchHit hit : scrollResp.getHits().getHits()) {
					Map<String, Object> result = hit.getSource();
					request = (String) result.get("RequestUrl");
					referer = (String) result.get("Referer");
					logType = (String) result.get("LogType");
					time = fmt.parseDateTime((String) result.get("Time"));
					id = hit.getId();

					if (logType.equals("PO.DAAC")) {
						if (referer.equals("-") || referer.equals(indexUrl) || !referer.contains(indexUrl)) {
							session_count++;
							session_count_in++;
							sessionReqs.put(ip + "@" + session_count_in, new HashMap<String, DateTime>());
							sessionReqs.get(ip + "@" + session_count_in).put(request, time);

							update(this.index, this.cleanup_type, id, "SessionID", ip + "@" + session_count_in, ip);

						} else {
							int count = session_count_in;
							int rollbackNum = 0;
							while (true) {
								Map<String, DateTime> requests = (Map<String, DateTime>) sessionReqs
										.get(ip + "@" + count);
								if (requests == null) {
									sessionReqs.put(ip + "@" + count, new HashMap<String, DateTime>());
									sessionReqs.get(ip + "@" + count).put(request, time);
									update(this.index, this.cleanup_type, id, "SessionID", ip + "@" + count, ip);

									break;
								}
								ArrayList<String> keys = new ArrayList<String>(requests.keySet());
								boolean bFindRefer = false;

								for (int i = keys.size() - 1; i >= 0; i--) {
									rollbackNum++;
									if (keys.get(i).equals(referer.toLowerCase())) {
										bFindRefer = true;
										// threshold,if time interval > 10*
										// click num, start a new session
										if (Math.abs(Seconds.secondsBetween(requests.get(keys.get(i)), time)
												.getSeconds()) < 600 * rollbackNum) {
											sessionReqs.get(ip + "@" + count).put(request, time);
											update(this.index, this.cleanup_type, id, "SessionID", ip + "@" + count, ip);
										} else {
											session_count++;
											session_count_in++;
											sessionReqs.put(ip + "@" + session_count_in,
													new HashMap<String, DateTime>());
											sessionReqs.get(ip + "@" + session_count_in).put(request, time);
											update(this.index, this.cleanup_type, id, "SessionID",
													ip + "@" + session_count_in, ip);
										}

										break;
									}
								}

								if (bFindRefer) {
									break;
								}

								count--;
								if (count < 0) {

									session_count++;
									session_count_in++;

									sessionReqs.put(ip + "@" + session_count_in, new HashMap<String, DateTime>());
									sessionReqs.get(ip + "@" + session_count_in).put(request, time);
									update(this.index, this.cleanup_type, id, "SessionID", ip + "@" + session_count_in, ip);

									break;
								}
							}
						}
					} else if (logType.equals("ftp")) {

						// may affect computation efficiency
						Map<String, DateTime> requests = (Map<String, DateTime>) sessionReqs
								.get(ip + "@" + session_count_in);
						if (requests == null) {
							sessionReqs.put(ip + "@" + session_count_in, new HashMap<String, DateTime>());
						} else {
							ArrayList<String> keys = new ArrayList<String>(requests.keySet());
							int size = keys.size();
							// System.out.println(Math.abs(Seconds.secondsBetween(requests.get(keys.get(size-1)),
							// time).getSeconds()));
							if (Math.abs(Seconds.secondsBetween(requests.get(keys.get(size - 1)), time)
									.getSeconds()) > 600) {
								// System.out.println("new session");
								session_count += 1;
								session_count_in += 1;
								sessionReqs.put(ip + "@" + session_count_in, new HashMap<String, DateTime>());
							}
						}
						sessionReqs.get(ip + "@" + session_count_in).put(request, time);
						update(this.index, this.cleanup_type, id, "SessionID", ip + "@" + session_count_in, ip);
					}
				}

				scrollResp = esnode.client.prepareSearchScroll(scrollResp.getScrollId())
						.setScroll(new TimeValue(600000)).execute().actionGet();
			}
		}

		//System.out.print("Update is done\n");
		System.out.println("The number of original sessions:" + Integer.toString(session_count));
	}

	public void combineShortSessions() throws ElasticsearchException, IOException {
		SearchResponse sr = esnode.client.prepareSearch(this.index).setTypes(this.cleanup_type)
				.setQuery(QueryBuilders.matchAllQuery())
				.addAggregation(AggregationBuilders.terms("Users").field("IP").size(0)).execute().actionGet();
		Terms Users = sr.getAggregations().get("Users");

		for (Terms.Bucket entry : Users.getBuckets()) {
			String IP =  entry.getKey();
			FilterBuilder filter_all = FilterBuilders.boolFilter()
					.must(FilterBuilders.termFilter("IP", entry.getKey()));
			QueryBuilder query_all = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), filter_all);
			
			SearchRequestBuilder check_allBuilder = esnode.client.prepareSearch(this.index).setTypes(this.cleanup_type)
					.setScroll(new TimeValue(60000)).setQuery(query_all).setSize(0);
			if(esnode.bRouting){
				check_allBuilder.setRouting(entry.getKey());
			}
			SearchResponse check_all =  check_allBuilder.execute().actionGet();
			long all = check_all.getHits().getTotalHits();

			FilterBuilder filter_check = FilterBuilders.boolFilter()
					.must(FilterBuilders.termFilter("IP", entry.getKey()))
					.must(FilterBuilders.termFilter("Referer", "-"));
			QueryBuilder query_check = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), filter_check);
			
			//System.out.println(query_check.toString());
			
			SearchResponse check_referer = esnode.client.prepareSearch(this.index).setTypes(this.cleanup_type)
					.setScroll(new TimeValue(60000)).setQuery(query_check).setSize(0).execute().actionGet();

			long num_invalid = check_referer.getHits().getTotalHits();

			double invalid_rate = (float) (num_invalid / all);

			if (invalid_rate >= 0.8 || all < 3) {
				deleteInvalid(entry.getKey());
				//System.out.print(entry.getKey() + "\n");
				continue;
			}

			FilterBuilder filter_search = FilterBuilders.boolFilter()
					.must(FilterBuilders.termFilter("IP", entry.getKey()));
			QueryBuilder query_search = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), filter_search);

			MetricsAggregationBuilder StatsAgg = AggregationBuilders.stats("Stats").field("Time");
			
			SearchRequestBuilder sr_session_builder = esnode.client.prepareSearch(this.index).setTypes(this.cleanup_type)
					.setScroll(new TimeValue(60000)).setQuery(query_search)
					.addAggregation(
							AggregationBuilders.terms("Sessions").field("SessionID").size(0).subAggregation(StatsAgg))
					;
			if(esnode.bRouting){
				sr_session_builder.setRouting(entry.getKey());
			}
			SearchResponse sr_session = sr_session_builder.execute().actionGet();

			Terms Sessions = sr_session.getAggregations().get("Sessions");

			List<Session> sessionList = new ArrayList<Session>();
			for (Terms.Bucket session : Sessions.getBuckets()) {
				Stats agg = session.getAggregations().get("Stats");
				Session sess = new Session(agg.getMinAsString(), agg.getMaxAsString(), session.getKey());
				sessionList.add(sess);
			}

			Collections.sort(sessionList);

			DateTimeFormatter fmt = ISODateTimeFormat.dateTime();
			String last = null;
			String lastnewID = null;
			String lastoldID = null;
			String current = null;
			for (Session s : sessionList) {
				current = s.getEndTime();
				if (last != null) {
					if (Seconds.secondsBetween(fmt.parseDateTime(last), fmt.parseDateTime(current))
							.getSeconds() < 600) {
						if (lastnewID == null) {
							s.setNewID(lastoldID);
						} else {
							s.setNewID(lastnewID);
						}

						FilterBuilder fs = FilterBuilders.boolFilter()
								.must(FilterBuilders.termFilter("SessionID", s.getID()));
						QueryBuilder qs = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), fs);
						
						
						SearchRequestBuilder scrollBuilder = esnode.client.prepareSearch(this.index)
								.setTypes(this.cleanup_type).setScroll(new TimeValue(60000)).setQuery(qs).setSize(100)
								;
						if(esnode.bRouting){
							scrollBuilder.setRouting(IP);
						}
						SearchResponse scrollResp =  scrollBuilder.execute().actionGet();
						
						while (true) {
							for (SearchHit hit : scrollResp.getHits().getHits()) {
								if (lastnewID == null) {
									update(this.index, this.cleanup_type, hit.getId(), "SessionID", lastoldID, IP);
								} else {
									update(this.index, this.cleanup_type, hit.getId(), "SessionID", lastnewID, IP);
								}
							}

							scrollResp = esnode.client.prepareSearchScroll(scrollResp.getScrollId())
									.setScroll(new TimeValue(600000)).execute().actionGet();
							// Break condition: No hits are returned
							if (scrollResp.getHits().getHits().length == 0) {
								break;
							}
						}
					}
				}
				lastoldID = s.getID();
				lastnewID = s.getNewID();
				last = current;
			}

			//System.out.print(entry.getKey() + "\n");
		}
		System.out.print("Combining is done.\n");
	}

	public void deleteInvalid(String ip) throws ElasticsearchException, IOException {
		FilterBuilder filter_all = FilterBuilders.boolFilter().must(FilterBuilders.termFilter("IP", ip));
		QueryBuilder query_all = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), filter_all);

		SearchResponse scrollResp = esnode.client.prepareSearch(this.index).setTypes(this.cleanup_type)
				.setScroll(new TimeValue(60000)).setQuery(query_all).setSize(100).execute().actionGet();
		
		while (true) {
			for (SearchHit hit : scrollResp.getHits().getHits()) {
				/*
				 * DeleteResponse response =
				 * Ek_test.client.prepareDelete(this.index, this.cleanup_type,
				 * hit.getId()) .setOperationThreaded(false) .get();
				 */
				update(this.index, this.cleanup_type, hit.getId(), "SessionID", "invalid",ip);
			}

			scrollResp = esnode.client.prepareSearchScroll(scrollResp.getScrollId())
					.setScroll(new TimeValue(600000)).execute().actionGet();
			// Break condition: No hits are returned
			if (scrollResp.getHits().getHits().length == 0) {
				break;
			}
		}
	}


	private void update(String index, String type, String id, String field1, Object value1, String routing)
			throws ElasticsearchException, IOException {
		UpdateRequest ur = new UpdateRequest(index, type, id)
				.doc(jsonBuilder().startObject().field(field1, value1).endObject());
		if(esnode.bRouting){
			 ur.routing(routing);
		}
		esnode.bulkProcessor.add(ur);
	}
	
}