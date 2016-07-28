package MUDROD.Datamining.ontologyExtractor;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import MUDROD.Datamining.DataMiner;
import Utils.ESNode;
import Utils.StringTool;
import Utils.DeleteRedType;

public class Sweet {
	DataMiner prep = null;
	String type = "SWEET";
	public Sweet(DataMiner miner) {
		prep = miner;

	}

	public void putSWEETMapping() throws IOException {
		XContentBuilder Mapping = jsonBuilder().startObject().startObject("SWEET").startObject("properties")
				.startObject("concept_A").field("type", "string").field("index", "not_analyzed").endObject()
				.startObject("concept_B").field("type", "string").field("index", "not_analyzed").endObject()

		.endObject().endObject().endObject();

		ESNode.client.admin().indices().preparePutMapping(prep.index).setType("SWEET").setSource(Mapping).execute()
				.actionGet();
	}

	public void importSWEEToES(String inputFileName)
			throws IOException, InterruptedException, ExecutionException {

		DeleteRedType drt = new DeleteRedType();
		drt.deleteAllByQuery(prep.index, type, QueryBuilders.matchAllQuery());

		BufferedReader br = null;
		String line = "";
		double weight = 0;

		try {
			br = new BufferedReader(new FileReader(inputFileName));
			while ((line = br.readLine()) != null) {
				String[] strList = line.toLowerCase().split(",");
				// String keywords = strList[0] + "," + strList[2];
				if (strList[1].equals("subclassof")) {
					weight = 0.75;
				} else {
					weight = 0.9;
				}

				IndexRequest ir = new IndexRequest(prep.index, type)
						.source(jsonBuilder().startObject().field("concept_A", StringTool.customAnalyzing(prep.index,strList[2]))
								.field("concept_B", StringTool.customAnalyzing(prep.index,strList[0])).field("weight", weight).endObject());
				ESNode.bulkProcessor.add(ir);

			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		long startTime = System.currentTimeMillis();

		DataMiner prep = new DataMiner("podacc");
		// LinkageGenerator lg= new LinkageGenerator(prep);
		// lg.importToES("C:/AIST LOG
		// FILES/PODAACKeywords/clusteringmatrix_userbags_bi_filtered.csv",
		// "userHistory");
		// lg.importToES("C:/AIST LOG
		// FILES/PODAACKeywords/clusteringmatrix_hi_import_downloadIn_revised_svd.csv",
		// "userClicking");
		// lg.importToES("C:/AIST LOG FILES/PODAACKeywords/GCMD_tfidf_svd.csv",
		// "GCMDMetadata");
		// lg.importSWEEToES("C:/AIST LOG
		// FILES/PODAACKeywords/Ocean_triples.csv", "SWEET");

		// lg.findRelatedTerms("ocean winds");

		// System.out.println(lg.TransformQuerySem("ocean wind", 3));

		// lg.eval("C:/AIST LOG FILES/PODAACKeywords/eval.csv");

		ESNode.bulkProcessor.awaitClose(20, TimeUnit.MINUTES);
		ESNode.node.close();
		long endTime = System.currentTimeMillis();
		System.out.println("Done!" + "Time elapsed： " + (endTime - startTime) / 1000 + "s");
	}

}
