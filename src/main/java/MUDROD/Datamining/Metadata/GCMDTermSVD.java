package MUDROD.Datamining.Metadata;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import MUDROD.Datamining.DataMiner;
import MUDROD.Datamining.tools.MatrixTool;
import MUDROD.Datamining.tools.TFIDF;
import MUDROD.Datamining.tools.TermTriple;
import Utils.ESNode;
import Utils.ESNodeClient;
import Utils.SparkNode;
import scala.Tuple2;

import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.mllib.linalg.Matrices;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.SingularValueDecomposition;
import org.apache.spark.mllib.linalg.Vector;

public class GCMDTermSVD implements Serializable {
	//DataMiner prep = null;
	String index = "metadata";
	String type = "metadataSource";
	
	public GCMDTermSVD() throws IOException
	{
		
	}
	
	public JavaPairRDD<String, List<String>> loadMetadata() throws UnsupportedEncodingException, NoSuchAlgorithmException, InterruptedException, ExecutionException{
	    SearchResponse scrollResp = ESNode.client.prepareSearch(index)
	    		.setTypes(type)
	    		.setQuery(QueryBuilders.matchAllQuery())
	    		.setScroll(new TimeValue(60000))
	            .setSize(100).execute().actionGet(); 

	    List<PODAACMetaData> metadatas = new ArrayList<PODAACMetaData>();
	    while (true) {
	    	for (SearchHit hit : scrollResp.getHits().getHits()) {
	    		Map<String,Object> result = hit.getSource();    
	    		String shortname = (String) result.get("shortName");
				String topic = (String) result.get("topic");
				String term = (String) result.get("term");
				String keyword = (String) result.get("keyword");
				String variable = (String) result.get("variable");
				String longname = (String) result.get("longName");
				/*String isotopic = (String) result.get("isotopic");
				String sensor = (String) result.get("sensor");
				String source = (String) result.get("source");
				String project = (String) result.get("project");
				
				String abstractstr = (String) result.get("abstract");*/
				
				PODAACMetaData metadata = new PODAACMetaData(shortname, longname, topic,  term,  variable,
						keyword);
				
				metadatas.add(metadata);
	        }
	        		         		        
	        scrollResp = ESNode.client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
	        if (scrollResp.getHits().getHits().length == 0) {
	            break;
	        }
	    }
	    
	    System.out.println(metadatas.size());
		JavaRDD<PODAACMetaData> metadataRDD = SparkNode.sc.parallelize(metadatas);
		System.out.println(metadataRDD.count());
		JavaPairRDD<String, List<String>> metadataTermsRDD = metadataRDD.mapToPair(new PairFunction<PODAACMetaData, String, List<String>>() {
			public Tuple2<String, List<String>> call(PODAACMetaData metadata) throws Exception {
					return new Tuple2<String, List<String>>(metadata.getShortName(), metadata.getAllTermList());
			}
		}).reduceByKey(new Function2<List<String>, List<String>, List<String>>() {
			@Override
			public List<String> call(List<String> v1, List<String> v2) throws Exception {
				// TODO Auto-generated method stub
				List<String> list = new ArrayList<String>();
				list.addAll(v1);
				list.addAll(v2);
				return list;
			}
		});
		
		return metadataTermsRDD;
	}

	/*public void calSematincSimilarity(String inputfile, String outputfile) throws NoSuchAlgorithmException, IOException, InterruptedException, ExecutionException, JSONException{
		
		//JavaRDD<PODAACMetaData> metadataRDD = this.loadMetadata(inputfile);
		//this.calSematincSimilarity(metadataRDD, outputfile);
	}*/
	
	private JavaPairRDD<String, List<String>> loadMetadata(String inputfile) throws JSONException, NoSuchAlgorithmException, InterruptedException, ExecutionException, IOException {
		// TODO Auto-generated method stub
		//String fileName = "D:/metadata.txt";
		BufferedReader br = new BufferedReader(new FileReader(inputfile));
		int count =0;
		
		List<PODAACMetaData> metadatas = new ArrayList<PODAACMetaData>();
		try {
			String line = br.readLine();
		    while (line != null) {
		    	JSONObject jsonData = new JSONObject(line);
		    	
		    	String shortname =  jsonData.getString("shortName");
				String topic =  jsonData.getString("topicStr");
				String term =  jsonData.getString("termStr");
				String keyword =  jsonData.getString("keywordStr");
				String variable =  jsonData.getString("variableStr");
				//String isotopic =  jsonData.getString("isotopic");
				//String sensor =  jsonData.getString("sensor");
				//String source =  jsonData.getString("source");
				//String project =  jsonData.getString("project");
				String longname =  jsonData.getString("longName");
				String abstractstr =  jsonData.getString("abstractStr");
				
				PODAACMetaData metadata = new PODAACMetaData(shortname, longname, topic,  term,  variable,
						keyword,  "",  "",  "",  "",  "");

				metadatas.add(metadata);
				
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
		    //bulkProcessor.close();
		    System.out.print(count);
		    System.out.print("\ndone");
		}
		
		JavaRDD<PODAACMetaData> metadataRDD = SparkNode.sc.parallelize(metadatas);
		
		JavaPairRDD<String, List<String>> metadataTermsRDD = metadataRDD.mapToPair(new PairFunction<PODAACMetaData, String, List<String>>() {
			public Tuple2<String, List<String>> call(PODAACMetaData metadata) throws Exception {
					return new Tuple2<String, List<String>>(metadata.getShortName(), metadata.getAllTermList());
			}
		}).reduceByKey(new Function2<List<String>, List<String>, List<String>>() {
			@Override
			public List<String> call(List<String> v1, List<String> v2) throws Exception {
				// TODO Auto-generated method stub
				List<String> list = new ArrayList<String>();
				list.addAll(v1);
				list.addAll(v2);
				return list;
			}
		});
		
		return metadataTermsRDD;
	}

	public void calSematincSimilarity(String outputfile) throws NoSuchAlgorithmException, InterruptedException, ExecutionException, IOException {
		
		JavaPairRDD<String, List<String>> metadataTermsRDD = this.loadMetadata();
		RowMatrix svdMatrix = buildSVDMatrix(metadataTermsRDD);
		
		JavaRDD<String> termRDD = metadataTermsRDD.values().flatMap(new FlatMapFunction<List<String>, String>() {
			public Iterable<String> call(List<String> list) {
				return list;
			}
		}).distinct();
		List<String> allTerms = termRDD.collect();
		
		MatrixTool.exportMatrix(svdMatrix, metadataTermsRDD, allTerms, outputfile);
	}
	
	public void calSematincSimilarity(ESNodeClient esnode, String txt, String insertIndex, String insertType) throws NoSuchAlgorithmException, InterruptedException, ExecutionException, IOException, JSONException {
		
		JavaPairRDD<String, List<String>> metadataTermsRDD = this.loadMetadata(txt);
		
		RowMatrix svdMatrix = buildSVDMatrix(metadataTermsRDD);
		CoordinateMatrix simMatirx = MatrixTool.calVecsimilarity(svdMatrix.rows().toJavaRDD());
		JavaRDD<String> queryRDD = metadataTermsRDD.values().flatMap(new FlatMapFunction<List<String>, String>() {
			public Iterable<String> call(List<String> list) {
				return list;
			}
		}).distinct();
		
		List<TermTriple> triples = MatrixTool.toTriples(queryRDD, simMatirx);
		TermTriple.insertTriples(esnode,triples,insertIndex,insertType);
	}

	public void calSematincSimilarity(ESNodeClient esnode,String insertIndex, String insertType) throws NoSuchAlgorithmException, InterruptedException, ExecutionException, IOException, JSONException {
		
		JavaPairRDD<String, List<String>> metadataTermsRDD = this.loadMetadata();
		
		RowMatrix svdMatrix = buildSVDMatrix(metadataTermsRDD);
		CoordinateMatrix simMatirx = MatrixTool.calVecsimilarity(svdMatrix.rows().toJavaRDD());
		JavaRDD<String> queryRDD = metadataTermsRDD.values().flatMap(new FlatMapFunction<List<String>, String>() {
			public Iterable<String> call(List<String> list) {
				return list;
			}
		}).distinct();
		
		List<TermTriple> triples = MatrixTool.toTriples(queryRDD, simMatirx);
		TermTriple.insertTriples(esnode,triples,insertIndex,insertType);
	}
	
	public RowMatrix buildSVDMatrix(JavaPairRDD<String, List<String>> metaDataQueryRDD) throws IOException {
		// generate tfidf matrix
		RowMatrix mat = TFIDF.creatTFIDFMatrix(metaDataQueryRDD, SparkNode.sc);
		// svd
		SingularValueDecomposition<RowMatrix, Matrix> svd = mat.computeSVD(300, true, 1.0E-9d);
		RowMatrix U = svd.U();
		Vector s = svd.s();
		Matrix V = svd.V();

		RowMatrix svdMatrix = U.multiply(Matrices.diag(s));
		return svdMatrix;
	}

	public static void main(String[] args) throws IOException, JSONException, InterruptedException, NoSuchAlgorithmException, ExecutionException {
		long startTime=System.currentTimeMillis(); 	   
		
		DataMiner prep = new DataMiner("podacc");
		GCMDTermSVD svdtest = new GCMDTermSVD();
		svdtest.calSematincSimilarity( "D:/distResult_SVD_GCMD_sim.csv");
		//svdtest.getRelatedWords("gravity");
		
		ESNode.bulkProcessor.awaitClose(20, TimeUnit.MINUTES);
		ESNode.node.close();
		long endTime=System.currentTimeMillis();
		System.out.println("Preprocessing is done!" + "Time elapsed： "+ (endTime-startTime)/1000+"s");
	}
	
}
