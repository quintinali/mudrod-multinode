package MUDROD.Datamining.tools;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.feature.IDF;
import org.apache.spark.mllib.feature.IDFModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import scala.Tuple2;
import scala.Tuple3;

public class TFIDF implements Serializable {

	public static RowMatrix creatTFIDFMatrix_old(JavaPairRDD<String, List<String>> uniqueDocRDD, JavaSparkContext sc) throws IOException {

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
		RowMatrix term_query_count_mat = MatrixTool.transpose_rowmatrix(query_term_count_mat, sc); // modify
		//String file = "D:/distResult_downloadin_countmatrix.csv";
		//this.exportMatrix(term_query_count_mat, uniqueDocRDD, allTerms, file);
		JavaRDD<Vector> newcountRDD = term_query_count_mat.rows().toJavaRDD();
		System.out.println(uniqueDocRDD.collect());
		
		IDFModel idfModel = new IDF().fit(newcountRDD);
		JavaRDD<Vector> idf = idfModel.transform(newcountRDD);
		RowMatrix tfidfMat = new RowMatrix(idf.rdd());
		String file2 = "D:/GCMD_tfidfmatrix.csv";
		
		//MatrixTool.exportMatrix(tfidfMat, uniqueDocRDD, allTerms, file2);
	
		return tfidfMat;
	}
	
	public static RowMatrix creatTFIDFMatrix(JavaPairRDD<String, List<String>> uniqueDocRDD, JavaSparkContext sc) throws IOException {

		// Index documents with unique IDs
		JavaPairRDD<List<String>, Long> corpus = uniqueDocRDD.values().zipWithIndex();
		//cal word-doc numbers
		JavaPairRDD<Tuple2<String,Long>, Double> worddoc_num_RDD = corpus.flatMapToPair(new PairFlatMapFunction<Tuple2<List<String>,Long>, Tuple2<String,Long>, Double>() {
			@Override
			public Iterable<Tuple2<Tuple2<String,Long>, Double>> call(Tuple2<List<String>,Long> docwords) throws Exception {
				List<Tuple2<Tuple2<String,Long>, Double>> pairs = new ArrayList<Tuple2<Tuple2<String,Long>, Double>>();
				List<String> words = docwords._1;
				int n = words.size();
				for(int i=0; i<n; i++){
					Tuple2<String,Long> worddoc = new Tuple2<String,Long>(words.get(i), docwords._2);
					pairs.add(new Tuple2<Tuple2<String,Long>, Double>(worddoc, 1.0));
				}
				return pairs;
			}
		}).reduceByKey(new Function2<Double, Double, Double>() {
			public Double call(Double first, Double second) throws Exception {
				return first + second;
			}
		});
		//cal word doc-numbers
		JavaPairRDD<String, Tuple2<List<Long>,List<Double>>> word_docnum_RDD = worddoc_num_RDD.mapToPair(new PairFunction<Tuple2<Tuple2<String,Long>, Double>, String, Tuple2<List<Long>,List<Double>>>() {
			@Override
			public Tuple2<String, Tuple2<List<Long>, List<Double>>> call(Tuple2<Tuple2<String, Long>, Double> worddoc_num)
					throws Exception {
				List<Long> docs= new ArrayList<Long>();
				docs.add(worddoc_num._1._2);
				List<Double> nums= new ArrayList<Double>();
				nums.add(worddoc_num._2);
				Tuple2<List<Long>, List<Double>> docmums = new Tuple2<List<Long>, List<Double>>(docs, nums);
				return new Tuple2<String, Tuple2<List<Long>, List<Double>>>(worddoc_num._1._1, docmums);
			}
		});
		//trans to vector
		//
		int corporsize = (int) uniqueDocRDD.keys().count();
		JavaPairRDD<String, Vector> word_vectorRDD =
		word_docnum_RDD.reduceByKey(new Function2<Tuple2<List<Long>,List<Double>>, Tuple2<List<Long>,List<Double>>,Tuple2<List<Long>,List<Double>>>() {
			@Override
			public Tuple2<List<Long>, List<Double>> call(Tuple2<List<Long>, List<Double>> arg0,
					Tuple2<List<Long>, List<Double>> arg1) throws Exception {
				arg0._1.addAll(arg1._1);
				arg0._2.addAll(arg1._2);
				return new Tuple2<List<Long>, List<Double>>(arg0._1, arg0._2);
			}
		}).mapToPair(new PairFunction<Tuple2<String, Tuple2<List<Long>, List<Double>>>, String, Vector>() {
			@Override
			public Tuple2<String, Vector> call(Tuple2<String, Tuple2<List<Long>, List<Double>>> arg0)
					throws Exception {
				// TODO Auto-generated method stub
				int docsize = arg0._2._1.size();
				int[] intArray = new int[docsize];
				double[] doubleArray = new double[docsize];
				for(int i=0; i<docsize;i++){
					intArray[i] = arg0._2._1.get(i).intValue();
					doubleArray[i] = arg0._2._2.get(i).intValue();
				}
				Vector sv = Vectors.sparse(corporsize, intArray,doubleArray);
				return new Tuple2<String, Vector>(arg0._1, sv);
			}
		});
		
		RowMatrix word_doc_count_mat = new RowMatrix(word_vectorRDD.values().rdd());
		JavaRDD<Vector> newcountRDD = word_doc_count_mat.rows().toJavaRDD();
		IDFModel idfModel = new IDF().fit(newcountRDD);
		JavaRDD<Vector> idf = idfModel.transform(newcountRDD);
		RowMatrix tfidfMat = new RowMatrix(idf.rdd());
		
		return tfidfMat;
	}
	
	
	public static RowMatrix creatTFIDFMatrix2/*toWordVector*/(JavaRDD<List<String>> docWordsRDD, JavaSparkContext sc) throws IOException {

		// Index doc with unique IDs
		JavaPairRDD<List<String>, Long> corpus = docWordsRDD.zipWithIndex();
		
		JavaRDD<Tuple3<String,Long,Double>> word_docRDD = corpus.flatMap(new FlatMapFunction<Tuple2<List<String>,Long>, Tuple3<String,Long,Double>>() {
			@Override
			public Iterable<Tuple3<String,Long, Double>> call(Tuple2<List<String>,Long> docwords) throws Exception {
				List<Tuple3<String,Long, Double>> pairs = new ArrayList<Tuple3<String,Long, Double>>();
				List<String> words = docwords._1;
				int n = words.size();
				for(int i=0; i<n; i++){
					pairs.add(new Tuple3<String,Long, Double>(words.get(i), docwords._2,1.0));
				}
				return pairs;
			}
		});
		
		//word_numRDD
		//word_docRDD.ag
		
	/*	//cal word-doc numbers
		JavaPairRDD<Tuple2<String,Long>, Double> worddoc_num_RDD = corpus.flatMapToPair(new PairFlatMapFunction<Tuple2<List<String>,Long>, Tuple2<String,Long>, Double>() {
			@Override
			public Iterable<Tuple2<Tuple2<String,Long>, Double>> call(Tuple2<List<String>,Long> docwords) throws Exception {
				List<Tuple2<Tuple2<String,Long>, Double>> pairs = new ArrayList<Tuple2<Tuple2<String,Long>, Double>>();
				List<String> words = docwords._1;
				int n = words.size();
				for(int i=0; i<n; i++){
					Tuple2<String,Long> worddoc = new Tuple2<String,Long>(words.get(i), docwords._2);
					pairs.add(new Tuple2<Tuple2<String,Long>, Double>(worddoc, 1.0));
				}
				return pairs;
			}
		}).reduceByKey(new Function2<Double, Double, Double>() {
			public Double call(Double first, Double second) throws Exception {
				return first + second;
			}
		});
		//cal word doc-numbers
		JavaPairRDD<String, Tuple2<List<Long>,List<Double>>> word_docnum_RDD = worddoc_num_RDD.mapToPair(new PairFunction<Tuple2<Tuple2<String,Long>, Double>, String, Tuple2<List<Long>,List<Double>>>() {
			@Override
			public Tuple2<String, Tuple2<List<Long>, List<Double>>> call(Tuple2<Tuple2<String, Long>, Double> worddoc_num)
					throws Exception {
				List<Long> docs= new ArrayList<Long>();
				docs.add(worddoc_num._1._2);
				List<Double> nums= new ArrayList<Double>();
				nums.add(worddoc_num._2);
				Tuple2<List<Long>, List<Double>> docmums = new Tuple2<List<Long>, List<Double>>(docs, nums);
				return new Tuple2<String, Tuple2<List<Long>, List<Double>>>(worddoc_num._1._1, docmums);
			}
		});
		//trans to vector
		//
		int corporsize = (int) uniqueDocRDD.keys().count();
		JavaPairRDD<String, Vector> word_vectorRDD =
		word_docnum_RDD.reduceByKey(new Function2<Tuple2<List<Long>,List<Double>>, Tuple2<List<Long>,List<Double>>,Tuple2<List<Long>,List<Double>>>() {
			@Override
			public Tuple2<List<Long>, List<Double>> call(Tuple2<List<Long>, List<Double>> arg0,
					Tuple2<List<Long>, List<Double>> arg1) throws Exception {
				arg0._1.addAll(arg1._1);
				arg0._2.addAll(arg1._2);
				return new Tuple2<List<Long>, List<Double>>(arg0._1, arg0._2);
			}
		}).mapToPair(new PairFunction<Tuple2<String, Tuple2<List<Long>, List<Double>>>, String, Vector>() {
			@Override
			public Tuple2<String, Vector> call(Tuple2<String, Tuple2<List<Long>, List<Double>>> arg0)
					throws Exception {
				// TODO Auto-generated method stub
				int docsize = arg0._2._1.size();
				int[] intArray = new int[docsize];
				double[] doubleArray = new double[docsize];
				for(int i=0; i<docsize;i++){
					intArray[i] = arg0._2._1.get(i).intValue();
					doubleArray[i] = arg0._2._2.get(i).intValue();
				}
				Vector sv = Vectors.sparse(corporsize, intArray,doubleArray);
				return new Tuple2<String, Vector>(arg0._1, sv);
			}
		});
		
		RowMatrix word_doc_count_mat = new RowMatrix(word_vectorRDD.values().rdd());
		JavaRDD<Vector> newcountRDD = word_doc_count_mat.rows().toJavaRDD();
		IDFModel idfModel = new IDF().fit(newcountRDD);
		JavaRDD<Vector> idf = idfModel.transform(newcountRDD);
		RowMatrix tfidfMat = new RowMatrix(idf.rdd());
		
		return tfidfMat;*/
		
		return null;
	}
}
