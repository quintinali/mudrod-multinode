package MUDROD.Datamining.tools;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.index.query.QueryBuilders;

import Utils.DeleteRedType;
import Utils.ESNode;
import Utils.ESNodeClient;

public class TermTriple implements Serializable {
	public long keyAId;
	public long keyBId;
	public double weight;
	public String keyA;
	public String keyB;
	
	public String toString(){
		return keyA + "," + keyB + ":" + weight;
	}
	
	public static void insertTriples(ESNodeClient esnode, List<TermTriple> triples,String insertIndex, String insertType) throws IOException{
		
		DeleteRedType drt = new DeleteRedType(esnode);
		drt.deleteAllByQuery(insertIndex, insertType, QueryBuilders.matchAllQuery());
		
		int size = triples.size();
		for(int i=0; i<size;i++){
			IndexRequest ir = new IndexRequest(insertIndex, insertType).source(jsonBuilder()
					.startObject()
					.field("keywords", triples.get(i).keyA + "," + triples.get(i).keyB)
					.field("weight", triples.get(i).weight)	
					.endObject());
			ESNode.bulkProcessor.add(ir);
		}
	}

}
