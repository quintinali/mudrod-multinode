package MUDROD.Datamining.Metadata;
import scala.Tuple2;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.clustering.LDA;
import org.apache.spark.mllib.linalg.DenseMatrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import MUDROD.Datamining.DataMiner;
import Utils.ESNode;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.spark.SparkConf;

public class LDAModel implements Serializable {
	
	public JavaPairRDD<String, List<String>> loadData(JavaSparkContext sc) throws Exception {
		DataMiner prep = new DataMiner("podaac");
	    SearchResponse scrollResp = ESNode.client.prepareSearch(prep.index)
	    		.setTypes("metadataSource")
	    		.setQuery(QueryBuilders.matchAllQuery())
	    		.setScroll(new TimeValue(60000))
	            .setSize(100).execute().actionGet(); 

	    List<Tuple2<String, List<String>>> datasetsTokens= new ArrayList<Tuple2<String, List<String>>>();
	    while (true) {
	    	for (SearchHit hit : scrollResp.getHits().getHits()) {
	    		Map<String,Object> result = hit.getSource();     		
				String shortName = (String) result.get("shortName");
				
				String abstractTokens = (String) result.get("abstract_tokens");	
				List<String> abstractTokenList = this.getTokens(prep,abstractTokens);
				
				String keywordTokens = (String) result.get("keyword_tokens");	
				List<String> keywordTokenList = this.getTokens(prep,keywordTokens);
				
				String termTokens = (String) result.get("term_tokens");	
				List<String> termTokenList = this.getTokens(prep,termTokens);
				
				String variableTokens = (String) result.get("variable_tokens");	
				List<String> variableTokenList = this.getTokens(prep,variableTokens);
				
				List<String> tokens = new ArrayList<String>();
				tokens.addAll(abstractTokenList);
				tokens.addAll(keywordTokenList);
				tokens.addAll(termTokenList);
				tokens.addAll(variableTokenList);
				
				datasetsTokens.add(new Tuple2(shortName, tokens));
	        }
	        		         		        
	        scrollResp = ESNode.client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
	        if (scrollResp.getHits().getHits().length == 0) {
	            break;
	        }
	    }
	    
	    ESNode.bulkProcessor.awaitClose(20, TimeUnit.MINUTES);
	    ESNode.node.close();
		
		JavaRDD<Tuple2<String, List<String>>> datasetsTokensRDD = sc.parallelize(datasetsTokens);
		//System.out.println(datasetsTokensRDD.collect());
		
		JavaPairRDD<String, List<String>> datasetsTokenPairRDD = datasetsTokensRDD.mapToPair(new PairFunction<Tuple2<String, List<String>>, String, List<String>>() {
			public Tuple2<String, List<String>> call(Tuple2<String, List<String>> term) throws Exception {
				return term;
			}
		});
		
		return datasetsTokenPairRDD;
	}
	
	public List<String> getTokens(DataMiner prep, String str) throws Exception{
		String[] splits = str.split(" ");
		List list = java.util.Arrays.asList(splits);
		return list;
	}
	
	//tool
	private static String readUrl(String urlString) throws Exception {
        BufferedReader reader = null;
        try {
            URL url = new URL(urlString);
            reader = new BufferedReader(new InputStreamReader(url.openStream()));
            StringBuffer buffer = new StringBuffer();
            int read;
            char[] chars = new char[1024];
            while ((read = reader.read(chars)) != -1)
                buffer.append(chars, 0, read); 

            return buffer.toString();
        } finally {
            if (reader != null)
                reader.close();
        }
    }

	
	public RowMatrix creatCountMatrix(JavaPairRDD<String, List<String>> uniqueDocRDD, JavaSparkContext sc) throws IOException {

		JavaRDD<String> termRDD = uniqueDocRDD.values().flatMap(new FlatMapFunction<List<String>, String>() {
			public Iterable<String> call(List<String> list) {
				return list;
			}
		}).distinct();
		List<String> allTerms = termRDD.collect();
		
		JavaRDD<Vector> countRDD = uniqueDocRDD.map(queryTerms -> {
			List<String> qterms = queryTerms._2;
			if (qterms != null) {
				Map<String, Integer> unitermNums = new HashMap<String, Integer>();
				int qtermssize =  qterms.size();
				for (int i = 0; i <qtermssize; i++) {
					String term = qterms.get(i);
					if (!unitermNums.containsKey(term)) {
						unitermNums.put(term, 1);
					} else {
						int nnum = unitermNums.get(term);
						unitermNums.put(term, nnum + 1);
					}
				}

				Set<String> uniterms = unitermNums.keySet();
				int unitermnum = uniterms.size();
				// allTerms
				double[] termcount = new double[allTerms.size()];
				int j = 0;
				for (int i = 0; i < allTerms.size(); i++) {

					String term = allTerms.get(i);
					if (uniterms.contains(term)) {
						double termnum = unitermNums.get(term);
						termcount[j] = termnum;
					} else {
						termcount[j] = 0;
					}

					j++;
				}

				Vector vec = Vectors.dense(termcount);
				return vec;
			} else {
				double[] termcount = new double[allTerms.size()];
				Vector vec = Vectors.dense(termcount);
				return vec;
			}
		});

		
		RowMatrix query_term_count_mat = new RowMatrix(countRDD.rdd());
		
		RowMatrix term_query_count_mat = this.transpose_rowmatrix(query_term_count_mat, sc); 
		this.exportMatrix(term_query_count_mat,uniqueDocRDD,"D:/idaexcel.csv");
		
		return query_term_count_mat;
	}
	
	private RowMatrix transpose_rowmatrix(RowMatrix rowmatrix, JavaSparkContext sc){
		DenseMatrix dense_mtx_transpose = new DenseMatrix((int) rowmatrix.numCols(), (int) rowmatrix.numRows(), rowmatrix.toBreeze().toArray$mcD$sp(),true);
		RowMatrix rowmatrix_out = this.denseToRowMatrix(dense_mtx_transpose, sc);
		return rowmatrix_out;
	}
	
	private RowMatrix denseToRowMatrix(DenseMatrix dense, JavaSparkContext sc) {

		double[] data = dense.toArray();
		int rownum = dense.numRows();
		int colnum = dense.numCols();
		LinkedList<Vector> rowsList = new LinkedList<Vector>();
		for (int i = 0; i < rownum; i++) {
			double[] row = new double[colnum];
			for (int j = 0; j < colnum; j++) {
				row[j] = data[i + j * rownum];
			}
			Vector currentRow = Vectors.dense(row);
			rowsList.add(currentRow);

		}
		JavaRDD<Vector> rows2 = sc.parallelize(rowsList);
		RowMatrix rowMatrix = new RowMatrix(rows2.rdd());

		return rowMatrix;
	}
	

	public static void main(String[] args) throws Exception {
		SparkConf conf = new SparkConf().setAppName("LDA Example");
		conf.setMaster("local[4]");
		conf.set("spark.driver.maxResultSize", "3g");
		JavaSparkContext sc = new JavaSparkContext(conf);

		LDAModel model = new LDAModel();
		JavaPairRDD<String, List<String>> datasetTokensRDD = model.loadData(sc);
		RowMatrix countMatrix = model.creatCountMatrix(datasetTokensRDD, sc);
		JavaRDD<Vector> parsedData = countMatrix.rows().toJavaRDD();
		  
		// Index documents with unique IDs
		JavaPairRDD<Long, Vector> corpus = JavaPairRDD
				.fromJavaRDD(parsedData.zipWithIndex().map(new Function<Tuple2<Vector, Long>, Tuple2<Long, Vector>>() {
					public Tuple2<Long, Vector> call(Tuple2<Vector, Long> doc_id) {
						return doc_id.swap();
					}
				}));
		corpus.cache();

		// Cluster the documents into three topics using LDA
		org.apache.spark.mllib.clustering.LDAModel ldaModel = new LDA().setK(8).  
	      setDocConcentration(2).  
	      setTopicConcentration(2).  
	      setMaxIterations(50).  
	      setSeed(0L).  
	      setCheckpointInterval(10).  
	      setOptimizer("em").  
	      run(corpus); 

		// Output topics. Each is a distribution over words (matching word countvectors)
		JavaRDD<String> tokensRDD = datasetTokensRDD.values().flatMap(new FlatMapFunction<List<String>, String>() {
			public Iterable<String> call(List<String> list) {
				return list;
			}
		}).distinct();
		List<String> allTokens = tokensRDD.collect();
		Map<Integer, String> tokenIds = new HashMap<Integer, String>();
		int length = allTokens.size();
		for(int i=0; i<length; i++){
			tokenIds.put(i, allTokens.get(i));
		}
		
		System.out.println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize() + " words):");
		/*Matrix topics = ldaModel.topicsMatrix();
		for (int topic = 0; topic < 19; topic++) {
			System.out.print("Topic " + topic + ":");
			HashMap wordweights = new HashMap<String, Double>();
			for (int word = 0; word < ldaModel.vocabSize(); word++) {
				//System.out.print(" " + topics.apply(word, topic));
				wordweights.put(allTokens.get(word), topics.apply(word, topic));
			}
			
			Map<String, Double> sortedMap = model.sortMapByValue(wordweights);
			int n = 0;
			for(String s: sortedMap.keySet()){
				System.out.print(s + ":" + sortedMap.get(s) + "; ");
				n+=1;
				if(n>10){
					break;
				}
			}
			System.out.println();
		}*/

		Tuple2<int[],double[]>[] topics = ldaModel.describeTopics(10);
		for (int topic = 0; topic < topics.length; topic++) {
			System.out.print("Topic " + topic + ":");
			int[] word =topics[topic]._1;
			double[] weight =topics[topic]._2;
			for(int i=0; i< word.length; i++){
				System.out.print(" " +  tokenIds.get(word[i]) + ":" +  weight[i]);
			}
			
			 System.out.println();
		}
	}
	
	public LinkedHashMap sortMapByValue(HashMap passedMap) {
		   List mapKeys = new ArrayList(passedMap.keySet());
		   List mapValues = new ArrayList(passedMap.values());
		   Collections.sort(mapValues, Collections.reverseOrder());
		   Collections.sort(mapKeys,Collections.reverseOrder());

		   LinkedHashMap sortedMap = new LinkedHashMap();

		   Iterator valueIt = mapValues.iterator();
		   while (valueIt.hasNext()) {
		       Object val = valueIt.next();
		       Iterator keyIt = mapKeys.iterator();

		       while (keyIt.hasNext()) {
		           Object key = keyIt.next();
		           String comp1 = passedMap.get(key).toString();
		           String comp2 = val.toString();

		           if (comp1.equals(comp2)){
		               passedMap.remove(key);
		               mapKeys.remove(key);
		               sortedMap.put((String)key, (Double)val);
		               break;
		           }

		       }

		   }
		   return sortedMap;
		}
	
	private void exportMatrix(RowMatrix simMatrix, JavaPairRDD<String, List<String>> uniqueDocRDD, String fileName)
			throws IOException {

		//RowMatrix term_query_count_mat = this.transpose_rowmatrix(query_term_count_mat, sc); 
		JavaRDD<String> termRDD = uniqueDocRDD.values().flatMap(new FlatMapFunction<List<String>, String>() {
			public Iterable<String> call(List<String> list) {
				return list;
			}
		}).distinct();
		List<String> allwords = termRDD.collect();

		List<String> docs = uniqueDocRDD.keys().collect();

		// test code
		int simRows = (int) simMatrix.numRows();
		int simCols = (int) simMatrix.numCols();
		List<Vector> rows = (List<Vector>) simMatrix.rows().toJavaRDD().collect();

		// File file = new File("D:/distResult_downloadin_rowmatrix.csv");
		File file = new File(fileName);
		if (!file.exists()) {
			file.createNewFile();
		}

		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		String coltitle = " no" + ",";
		for (int j = 0; j < docs.size(); j++) {
			coltitle += docs.get(j) + ",";
		}
		coltitle = coltitle.substring(0, coltitle.length() - 1);
		bw.write(coltitle + "\n");

		// bw.write("Keywords" + "," + "CosinSim" + "\n");
		for (int i = 0; i < simRows; i++) {
			Vector row = rows.get(i);
			double[] rowvlaue = rows.get(i).toArray();
			String srow = allwords.get(i) + ",";
			for (int j = 0; j < simCols; j++) {
				if (rowvlaue[j] <= 0) {
					srow += "" + ",";
				} else {
					srow += rowvlaue[j] + ",";
				}
				
				//srow += rowvlaue[j] + ",";
			}
			srow = srow.substring(0, srow.length() - 1);
			bw.write(srow + "\n");
		}

		bw.close();
	}
}
