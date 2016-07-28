package MUDROD.Datamining.logAnalyzer;

import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.codehaus.jettison.json.JSONException;
import MUDROD.Datamining.DataMiner;
import MUDROD.Datamining.tools.MatrixTool;
import MUDROD.Datamining.tools.TFIDF;
import MUDROD.Datamining.tools.TermTriple;
import Utils.ESNode;
import Utils.ESNodeClient;
import Utils.SparkNode;
import scala.Tuple2;
import org.apache.spark.mllib.linalg.Matrices;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.SingularValueDecomposition;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;

public class ClickStreamSVD implements Serializable {

	DataMiner prep = null;
	String KeywordTripleType = "KeywordTriple";
	String index = "";

	public ClickStreamSVD(DataMiner test) throws IOException {
		prep = test;
		this.index = test.index;
	}

	//load data from es
	 public JavaPairRDD<String, List<String>> loadClickThroughData(ESNodeClient esnode) throws UnsupportedEncodingException{ ClickThroughGenerator gen = new
		 ClickThroughGenerator(prep); 
		 List<ClickThroughData> QueryList = gen.genClickThroughData(esnode); 
		 JavaRDD<ClickThroughData> clickthroughRDD = SparkNode.sc.parallelize(QueryList); 
		 
		 JavaPairRDD<String, List<String>> metaDataQueryRDD = clickthroughRDD
					.mapToPair(new PairFunction<ClickThroughData, String, List<String>>() {
						public Tuple2<String, List<String>> call(ClickThroughData click) throws Exception {
							List<String> query = new ArrayList<String>();
							query.add(click.getKeyWords());
							return new Tuple2<String, List<String>>(click.getViewDataset(), query);
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
		 
		 return metaDataQueryRDD; 
	 }
	 
	 //load data from txt file
	public JavaRDD<ClickThroughData> loadClickThroughData(String clickthroughFile, JavaSparkContext sc) {
		JavaRDD<ClickThroughData> clickthroughRDD = sc.textFile(clickthroughFile)
				.flatMap(new FlatMapFunction<String, ClickThroughData>() {
					public Iterable<ClickThroughData> call(String line)
							throws NoSuchAlgorithmException, IOException, JSONException {
						List<ClickThroughData> clickthroughs = (List<ClickThroughData>) ClickThroughData
								.parseDatasFromLogLine(line);
						return clickthroughs;
					}
				});
		return clickthroughRDD;
	}

	//output sim triples into csv files
	public void calSematincSimilarity(ESNodeClient esnode,String outfilename) throws IOException {
		JavaPairRDD<String, List<String>> metaDataQueryRDD = this.loadClickThroughData(esnode);
		RowMatrix svdMatrix = buildSVDMatrix(metaDataQueryRDD);
		
		JavaRDD<String> queryRDD = metaDataQueryRDD.values().flatMap(new FlatMapFunction<List<String>, String>() {
			public Iterable<String> call(List<String> list) {
				return list;
			}
		}).distinct();
		List<String> allQueries = queryRDD.collect();
		MatrixTool.exportMatrix(svdMatrix, metaDataQueryRDD, allQueries, outfilename);
	}

	//directly insert sim triples into es
	public void calSematincSimilarity(ESNodeClient esnode,String insertIndex, String insertType) throws IOException {
		JavaPairRDD<String, List<String>> metaDataQueryRDD = this.loadClickThroughData(esnode);
		
		RowMatrix svdMatrix = buildSVDMatrix(metaDataQueryRDD);
		CoordinateMatrix simMatirx = MatrixTool.calVecsimilarity(svdMatrix.rows().toJavaRDD());
		JavaRDD<String> queryRDD = metaDataQueryRDD.values().flatMap(new FlatMapFunction<List<String>, String>() {
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
		SingularValueDecomposition<RowMatrix, Matrix> svd = mat.computeSVD(100, true, 1.0E-9d);
		RowMatrix U = svd.U();
		Vector s = svd.s();
		Matrix V = svd.V();

		RowMatrix svdMatrix = U.multiply(Matrices.diag(s));
		return svdMatrix;
	}

	public static void main(String[] args) throws IOException, JSONException, InterruptedException {
		long startTime = System.currentTimeMillis();
		SparkNode sparkNode = new SparkNode();
		ESNode esnode = new ESNode();
		DataMiner prep = new DataMiner("podaacsession");
		ClickStreamSVD svdtest = new ClickStreamSVD(prep);
		//svdtest.calSematincSimilarity("D:/distResult_SVD_Query_sim.csv");
		//svdtest.calSematincSimilarity("D:/access_keywords/clickstream", "D:/distResult_SVD_Query_sim.csv");
		svdtest.calSematincSimilarity(null,"podaacsession", "userClicking");

		ESNode.bulkProcessor.awaitClose(20, TimeUnit.MINUTES);
		ESNode.node.close();
		SparkNode.sc.stop();
		long endTime = System.currentTimeMillis();
		System.out.println("Preprocessing is done!" + "Time elapsed： " + (endTime - startTime) / 1000 + "s");
	}
}
