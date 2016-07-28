package MUDROD.Datamining.tools;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.DenseMatrix;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.elasticsearch.action.index.IndexRequest;

import com.google.common.base.Optional;
import Utils.ESNode;
import Utils.SparkNode;
import scala.Tuple2;

public class MatrixTool implements Serializable {

	public static RowMatrix transpose_rowmatrix(RowMatrix rowmatrix, JavaSparkContext sc) {
		DenseMatrix dense_mtx_transpose = new DenseMatrix((int) rowmatrix.numCols(), (int) rowmatrix.numRows(),
				rowmatrix.toBreeze().toArray$mcD$sp(), true);
		RowMatrix rowmatrix_out = denseToRowMatrix(dense_mtx_transpose, sc);
		return rowmatrix_out;
	}

	public static RowMatrix denseToRowMatrix(DenseMatrix dense, JavaSparkContext sc) {

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

	private static double[] upperMatrixToFull(Matrix simMatrix) {

		double[] similaritys = simMatrix.toArray();
		int simRows = simMatrix.numRows();
		int simCols = simMatrix.numCols();
		int simlength = similaritys.length;

		for (int i = 0; i < simRows; i++) {
			for (int j = 0; j < simCols; j++) {

				if (i == j) {
					similaritys[i + j * simCols] = 1.0;
				}
				if (similaritys[i + j * simCols] != similaritys[j + i * simCols]) {
					similaritys[i + j * simCols] = similaritys[j
							+ i * simCols] = similaritys[i + j * simCols] > similaritys[j + i * simCols]
									? similaritys[i + j * simCols] : similaritys[j + i * simCols];
				}
			}
		}

		return similaritys;
	}

	public static void exportMatrix(RowMatrix simMatrix, JavaPairRDD<String, List<String>> uniqueDocRDD,
			List<String> allwords, String fileName) throws IOException {

		JavaRDD<String> termRDD = uniqueDocRDD.values().flatMap(new FlatMapFunction<List<String>, String>() {
			public Iterable<String> call(List<String> list) {
				return list;
			}
		}).distinct();

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
		String coltitle = " Num" + ",";
		for (int j = 0; j < simCols; j++) {
			coltitle += docs.get(j) + ",";
		}
		coltitle = coltitle.substring(0, coltitle.length() - 1);
		bw.write(coltitle + "\n");

		String secondtitle = " Num" + ",";
		for (int j = 0; j < simCols; j++) {
			secondtitle += "f" + j + ",";
		}
		secondtitle = secondtitle.substring(0, secondtitle.length() - 1);
		bw.write(secondtitle + "\n");

		// bw.write("Keywords" + "," + "CosinSim" + "\n");
		for (int i = 0; i < simRows; i++) {
			Vector row = rows.get(i);
			double[] rowvlaue = rows.get(i).toArray();
			String srow = allwords.get(i) + ",";
			for (int j = 0; j < simCols; j++) {
				/*
				 * if(rowvlaue[j]==0){ srow += "" + "," ; }else{ srow +=
				 * rowvlaue[j] + "," ; }
				 */

				srow += rowvlaue[j] + ",";
			}
			srow = srow.substring(0, srow.length() - 1);
			bw.write(srow + "\n");
		}

		bw.close();
	}
	
	public static void calWordSimilarty(RowMatrix U, JavaPairRDD<String, List<String>> uniqueQueryRDD , String index, String type, JavaSparkContext sc) throws IOException{

		RowMatrix semantic_U_transpose = MatrixTool.transpose_rowmatrix(U, sc);
		CoordinateMatrix matrixtest = semantic_U_transpose.columnSimilarities();
		Matrix simMatrix = matrixtest.toBlockMatrix().toLocalMatrix();
		
		JavaRDD<String> termRDD = uniqueQueryRDD.values().flatMap(new FlatMapFunction<List<String>, String>() {
			public Iterable<String> call(List<String> list) {
				return list;
			}
		}).distinct();
		List<String> allTerms = termRDD.collect();
		
		MatrixTool.insertTriple(simMatrix, allTerms,index,type);
	}
	
	public static void insertTriple(Matrix simMatrix, List<String> words, String index, String type) throws IOException {

		/*Preprocesser prep = new Preprocesser();
		prep.InitiateIndex("podaac");
		String KeywordTripleType = "KeywordTriple";*/
		
		int simRows = simMatrix.numRows();
		int simCols = simMatrix.numCols();
		double[] similaritys = simMatrix.toArray();
		//double[] similaritys = this.upperMatrixToFull(simMatrix);
		
		Map<String, Integer> wordIds = new HashMap<String, Integer>();
		int num = 1;
		for (int i = 0; i < words.size(); i++) {
			wordIds.put(words.get(i), num);
			num++;
		}
		
		String wordA = "";
		String wordB = "";
		double weight = 0.0;
		for(int i=0; i< simRows; i++){
			for(int j=i; j< simCols; j++){
				weight = similaritys[i + j *simRows];
				if(weight>0.0){
					wordA = words.get(i);
					wordB = words.get(j);
					DecimalFormat df = new DecimalFormat("#.00");
					double dweight = Double.parseDouble(df.format(weight));
					IndexRequest ir = new IndexRequest(index, type).source(jsonBuilder()
							 .startObject()	
							 .field("keywords", wordA+","+wordB)
							 .field("weight", dweight)
							 .endObject());
					
					ESNode.bulkProcessor.add(ir);
				}
			}
		}
	}
	
	public static JavaPairRDD<String, Vector> loadVectorFromCSV(String csvFileName){
		// skip the first two lines(header), important!
		JavaRDD<String> importRDD = SparkNode.sc.textFile(csvFileName);
		JavaPairRDD<String, Long> importIdRDD = importRDD.zipWithIndex().filter(new Function<Tuple2<String,Long>, Boolean>() {
			public Boolean call(Tuple2<String, Long> v1) throws Exception {
				if(v1._2>1){
					return true;
				}
				return false;
			}
			
		});
		
		JavaPairRDD<String, Vector> vecRDD = importIdRDD.mapToPair(new PairFunction<Tuple2<String, Long>, String, Vector>() {
			@Override
			public Tuple2<String, Vector> call(Tuple2<String, Long> t) throws Exception {
				 String[] fields = t._1.split(",");
				 String word = fields[0];
				 int fieldsize = fields.length;
				 String[] numfields = Arrays.copyOfRange(fields, 1, fieldsize-1);
				 double[] nums = Stream.of(numfields).mapToDouble(Double::parseDouble).toArray();
				 Vector vec = Vectors.dense(nums);
				 return new Tuple2<String,Vector>(word, vec);
			}
		});
		
		return vecRDD;
	}
	
	public static CoordinateMatrix calVecsimilarity(JavaRDD<Vector> vecs){
		IndexedRowMatrix indexedMatrix = MatrixTool.buildIndexRowMatrix(vecs);
		RowMatrix transposeMatrix = MatrixTool.transposeMatrix(indexedMatrix);
		CoordinateMatrix simMatirx = transposeMatrix.columnSimilarities();
		
		return simMatirx;
	}
	
	public static List<TermTriple> toTriples(JavaRDD<String> keys, CoordinateMatrix simMatirx){
		
		if(simMatirx.numCols() != keys.count()){
			return null;
		}
		
		//index words
		JavaPairRDD<Long, String> keyIdRDD = JavaPairRDD.fromJavaRDD(keys.zipWithIndex().map(new Function<Tuple2<String, Long>, Tuple2<Long, String>>() {
			public Tuple2<Long, String> call(Tuple2<String, Long> doc_id) {
				return doc_id.swap();
			}
		}));
		
		JavaPairRDD<Long, TermTriple> entries_rowRDD =  simMatirx.entries().toJavaRDD().mapToPair(new PairFunction<MatrixEntry, Long, TermTriple>() {
			public Tuple2<Long, TermTriple> call(MatrixEntry t) throws Exception {
				// TODO Auto-generated method stub
				TermTriple triple = new TermTriple();
				triple.keyAId = t.i();
				triple.keyBId = t.j();
				triple.weight = t.value();
				return new Tuple2<Long, TermTriple>(triple.keyAId, triple);
			}
		});
		
		JavaPairRDD<Long, TermTriple> entries_colRDD = entries_rowRDD.leftOuterJoin(keyIdRDD).values().mapToPair(new PairFunction<Tuple2<TermTriple, Optional<String>>, Long, TermTriple>() {
			public Tuple2<Long, TermTriple> call(Tuple2<TermTriple, Optional<String>> t) throws Exception {
				// TODO Auto-generated method stub
				TermTriple triple = t._1;
				Optional<String> stra= t._2;
				if(stra.isPresent()){
					triple.keyA = stra.get();
				}
				return new Tuple2<Long, TermTriple>(triple.keyBId, triple);
			}
		});
		
		
		JavaRDD<TermTriple> tripleRDD = entries_colRDD.leftOuterJoin(keyIdRDD).values().map(new Function<Tuple2<TermTriple, Optional<String>>, TermTriple>() {
			public TermTriple call(Tuple2<TermTriple, Optional<String>> t) throws Exception {
				TermTriple triple = t._1;
				Optional<String> strb= t._2;
				if(strb.isPresent()){
					triple.keyB = strb.get();
				}
				return triple;
			}
		});
		
		List<TermTriple> triples =tripleRDD.collect();
		return triples;
	}

	public static IndexedRowMatrix buildIndexRowMatrix(JavaRDD<Vector> vecs){
		JavaRDD<IndexedRow> indexrows = vecs.zipWithIndex().map(new Function<Tuple2<Vector, Long>, IndexedRow>() {
			public IndexedRow call(Tuple2<Vector, Long> doc_id) {
				return new IndexedRow(doc_id._2, doc_id._1);
			}
		});
		IndexedRowMatrix indexedMatrix = new IndexedRowMatrix(indexrows.rdd());
		
		return indexedMatrix;
	}
	
	public static RowMatrix transposeMatrix(IndexedRowMatrix indexedMatrix){
		RowMatrix transposeMatrix =indexedMatrix.toCoordinateMatrix().transpose().toRowMatrix();
		return transposeMatrix;
	}
}



