package Utils;

import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import MUDROD.SessionRecon.Preprocesser;

public class DeleteRedType {
	private ESNodeClient esnode;
	
	public DeleteRedType(ESNodeClient esnode) {
		this.esnode = esnode;
	}
	
	public DeleteRedType() {
	
	}
	
	public void deleteAllByQuery(String index, String type, QueryBuilder query) {

		SearchResponse scrollResp = esnode.client.prepareSearch(index)
				.setSearchType(SearchType.SCAN)
				.setTypes(type)
				.setScroll(new TimeValue(60000))
				.setQuery(query)
				.setSize(10000)
				.execute().actionGet();  //10000 hits per shard will be returned for each scroll

		//SearchHit[] searchHits = scrollResp.getHits().getHits();

		while (true) {
			for (SearchHit hit : scrollResp.getHits().getHits()) {
				DeleteRequest deleteRequest = new DeleteRequest(index, type, hit.getId());
				esnode.bulkProcessor.add(deleteRequest);
			}
			
			//System.out.println("Need to delete " + scrollResp.getHits().getHits().length + " records");
			
			scrollResp = esnode.client.prepareSearchScroll(scrollResp.getScrollId())
					.setScroll(new TimeValue(600000)).execute().actionGet();
			if (scrollResp.getHits().getHits().length == 0) {
				break;
			}

		}
	}
	
	public void deleteTypeByMapping(String index, String type){
		
		SearchResponse scrollResp = esnode.client.prepareSearch(index)
				.setTypes(type)
				.setQuery(QueryBuilders.matchAllQuery())
				.setSize(1)
				.execute().actionGet();

		if(scrollResp.getHits().getHits().length >0){
			DeleteMappingResponse r = esnode.client.admin().indices().prepareDeleteMapping(index).setType(type).execute().actionGet();
			//System.out.println("delete" + type);
		}
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
