package MUDROD.Datamining.Metadata;

import scala.Tuple2;
import scala.Tuple3;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.google.common.base.Optional;

import MUDROD.Datamining.DataMiner;
import Utils.ESNode;

import java.io.BufferedWriter;

import java.io.FileOutputStream;

import java.io.IOException;

import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.spark.SparkConf;

public class LLDAModel implements Serializable {
	
	public JavaRDD<Tuple3<List<String>, String, String>> loadData(JavaSparkContext sc) throws Exception {
		DataMiner prep = new DataMiner("podacc");
	    SearchResponse scrollResp = ESNode.client.prepareSearch(prep.index)
	    		.setTypes("metadataSource")
	    		.setQuery(QueryBuilders.matchAllQuery())
	    		.setScroll(new TimeValue(60000))
	            .setSize(100).execute().actionGet(); 

	    List<Tuple3<List<String>, String, String>> label_sname_abstracts = new ArrayList<Tuple3<List<String>, String, String>>();
	    while (true) {
	    	for (SearchHit hit : scrollResp.getHits().getHits()) {
	    		Map<String,Object> result = hit.getSource();    
	    		String shortName = (String) result.get("shortName");
				String topic = (String) result.get("topicStr");
				String term = (String) result.get("termStr");
				String[] termArr = term.split(",");
				String[] topicArr = topic.split(",");
				List termList = Arrays.asList(termArr);
				List topicList = Arrays.asList(topicArr);
				
				List<String> labelList = new ArrayList<String>();
				labelList.addAll(termList);
				labelList.addAll(topicList);
 				String abstractTokens = (String) result.get("abstract_tokens");	
 				
 				String sensor = (String) result.get("sensor_tokens");
				String source = (String) result.get("source_tokens");
				String project = (String) result.get("project_tokens");
				String variable = (String) result.get("variable_tokens");
				//String termToken = (String) result.get("term_tokens");
				
				
				String[] sensorArr = sensor.split(",");
				String[] sourceArr = source.split(",");
				String[] projectArr = project.split(",");
				String[] variableArr = variable.split(",");
				
				List<String> abslList = new ArrayList<String>();
				List sensorList = Arrays.asList(sensorArr);
				List sourceList = Arrays.asList(sourceArr);
				List projectList = Arrays.asList(projectArr);
				List variableList = Arrays.asList(variableArr);
				abslList.addAll(sensorList);
				abslList.addAll(sourceList);
				abslList.addAll(projectList);
				abslList.addAll(variableList);
				
				for(int i=0; i<abslList.size(); i++){
					abstractTokens += " " + abslList.get(i).replace(" ", "_");
				}
				
 				label_sname_abstracts.add(new Tuple3(labelList, shortName, abstractTokens));
	        }
	        		         		        
	        scrollResp = ESNode.client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
	        if (scrollResp.getHits().getHits().length == 0) {
	            break;
	        }
	    }
	    
	    ESNode.bulkProcessor.awaitClose(20, TimeUnit.MINUTES);
	    ESNode.node.close();
		
		JavaRDD<Tuple3<List<String>, String, String>> label_sname_abstractRDD = sc.parallelize(label_sname_abstracts);
		return label_sname_abstractRDD;
	}
	
	public JavaPairRDD<String, Long> genLabelIds(JavaPairRDD<List<String>, String> labels_snameRDD){
		
		JavaRDD<List<String>> labelsRDD = labels_snameRDD.keys();
		JavaRDD<String> allLabelsRDD = labelsRDD.flatMap(new FlatMapFunction<List<String>, String>() {
			public Iterable<String> call(List<String> label){
				return label;
			}
		}).distinct();
		
		JavaPairRDD<String, Long> labels_idRDD = allLabelsRDD.zipWithIndex();
		
		return labels_idRDD;
	}
	
	public JavaPairRDD<String, List<Long>> assignLabelIdForShortName(JavaPairRDD<List<String>, String> labels_snameRDD, JavaPairRDD<String, Long> labels_idRDD){
		
		JavaPairRDD<String, String> singlelabel_snameRDD = labels_snameRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<List<String>, String>, String, String>() {
			public  Iterable<Tuple2<String, String>> call(Tuple2<List<String>,String> tuples) throws Exception {
				List<Tuple2<String, String>> pairs = new ArrayList<Tuple2<String, String>>();
				List<String> labels = tuples._1;
				String name = tuples._2;
				int n = labels.size();
				for(int i=0; i<n; i++){
					Tuple2 pair = new Tuple2(labels.get(i), name);
					pairs.add(pair);
				}
				return pairs;
			}
		}).distinct();
		
		JavaPairRDD<String, Tuple2<String, Optional<Long>>> label_sname_labelIdRDD = singlelabel_snameRDD.leftOuterJoin(labels_idRDD);
		
		JavaPairRDD<String, List<Long>> sname_singlelabelIdRDD = label_sname_labelIdRDD.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Optional<Long>>>, String, List<Long>>() {
			public Tuple2<String,List<Long>> call(Tuple2<String, Tuple2<String, Optional<Long>>> label_sname_labelId) throws Exception {
				String sname = label_sname_labelId._2._1;
				List<String> newterms = new ArrayList<String>();
				Optional<Long> id = label_sname_labelId._2._2;
				List<Long> labelId = new ArrayList<Long>();
				if(id.isPresent()){
					labelId.add(id.get());
				}
				return new Tuple2<String, List<Long>>(sname, labelId);
			}
		});
		
		JavaPairRDD<String, List<Long>> sname_labelIdsRDD = sname_singlelabelIdRDD.reduceByKey(new Function2<List<Long>, List<Long>,List<Long>>() {
			public List<Long> call(List<Long> first, List<Long> second) throws Exception {
				List<Long> labelIds= new ArrayList<Long>();
				labelIds.addAll(first);
				labelIds.addAll(second);
				return labelIds;
			}
		});
		
		//System.out.println(sname_labelIdsRDD.collect());
		
		return sname_labelIdsRDD;
	}
	
	public JavaPairRDD<String, List<Long>> assignLabelIdForAbstract(JavaPairRDD<String, String> sname_abstractRDD, JavaPairRDD<String, List<Long>> sname_labelIdsRDD){

		JavaPairRDD<String, Tuple2<String, Optional<List<Long>>>> sname_abstract_labelIdRDD = sname_abstractRDD.leftOuterJoin(sname_labelIdsRDD);
		JavaPairRDD<String, List<Long>> abstract_labelIdRDD = sname_abstract_labelIdRDD.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Optional<List<Long>>>>, String, List<Long>>() {
			public Tuple2<String,List<Long>> call(Tuple2<String, Tuple2<String, Optional<List<Long>>>> sname_abstract_labelId) throws Exception {
				String abstractstr = sname_abstract_labelId._2._1;
				Optional<List<Long>> id = sname_abstract_labelId._2._2;
				List<Long> labelId = new ArrayList<Long>();
				if(id.isPresent()){
					labelId.addAll(id.get());
				}
				return new Tuple2<String, List<Long>>(abstractstr, labelId);
			}
		});
		
		return abstract_labelIdRDD;
	}
	
	public List<String> getTokens(DataMiner prep, String str) throws Exception{
		String[] splits = str.split(" ");
		List list = java.util.Arrays.asList(splits);
		return list;
	}

	public void exportFile(List<Tuple2<String,List<Long>>> datasetTokens) throws IOException{
		System.out.println("export filr");
		String outputFile = "D:/models/testlabeleddata.txt";
	    Writer out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile)));
	    for (int k = 0; k < datasetTokens.size(); k++) { 
	    	
	    	List<Long> labelIds = datasetTokens.get(k)._2();
	    	String lableStr = "[";
	    	for(int i=0; i<labelIds.size(); i++){
	    		lableStr += labelIds.get(i) + " " ;
	    	}
	    	lableStr = lableStr.substring(0,lableStr.length()-1);
	    	lableStr += "]";
	    	out.write(String.format("%s\n", lableStr + "	" + datasetTokens.get(k)._1() ));
	    }
	    out.close();
	}
	

	public static void main(String[] args) throws Exception {
		SparkConf conf = new SparkConf().setAppName("LDA Example");
		conf.setMaster("local[4]");
		conf.set("spark.driver.maxResultSize", "3g");
		JavaSparkContext sc = new JavaSparkContext(conf);

		LLDAModel model = new LLDAModel();
		JavaRDD<Tuple3<List<String>, String, String>>  label_sname_abstractRDD = model.loadData(sc);
		
		//System.out.println(label_sname_abstractRDD.collect());
		JavaPairRDD<List<String>, String> label_snameRDD = label_sname_abstractRDD.mapToPair(new PairFunction<Tuple3<List<String>, String, String>, List<String>, String>() {
			public Tuple2<List<String>, String> call(Tuple3<List<String>, String, String> term) throws Exception {
				return new Tuple2(term._1(), term._2());
			}
		});
		
		JavaPairRDD<String, Long> labelIdRDD = model.genLabelIds(label_snameRDD);
		System.out.println(labelIdRDD.collect());
		JavaPairRDD<String, List<Long>> sname_labelIdsRDD = model.assignLabelIdForShortName(label_snameRDD,labelIdRDD);
		
		JavaPairRDD<String, String> sname_abstractRDD = label_sname_abstractRDD.mapToPair(new PairFunction<Tuple3<List<String>, String, String>, String, String>() {
			public Tuple2<String, String> call(Tuple3<List<String>, String, String> term) throws Exception {
				return new Tuple2(term._2(), term._3());
			}
		});
		
		//model.analyze_data(sname_abstractRDD);
		
		JavaPairRDD<String, List<Long>> abstract_labelIdRDD = model.assignLabelIdForAbstract(sname_abstractRDD,sname_labelIdsRDD);
		List<Tuple2<String,List<Long>>> abstract_labelIds= abstract_labelIdRDD.collect();
		model.exportFile(abstract_labelIds);
	}
	
	public void analyze_data(JavaPairRDD<String, String> sname_abstractRDD){
		
		JavaRDD<String> absRDD = sname_abstractRDD.values();
		JavaRDD<String> allTermRDD = absRDD.flatMap(new FlatMapFunction<String, String>() {
			public Iterable<String> call(String abs){
				return Arrays.asList(abs.split(" "));
			}
		});
		
		JavaPairRDD<String, Integer> termCountRDD = allTermRDD.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String term) throws Exception {
				return new Tuple2(term, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer first, Integer second) throws Exception {
				return first + second;
			}
		}).mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {
			public Tuple2<Integer,String> call(Tuple2<String,Integer> term) throws Exception {
				return term.swap();
			}
		}).sortByKey(false).mapToPair(new PairFunction<Tuple2<Integer,String>, String,Integer>() {
			public Tuple2<String,Integer> call(Tuple2<Integer,String> term) throws Exception {
				return term.swap();
			}
		}).filter(new Function<Tuple2<String, Integer>, Boolean>() {
			public Boolean call(Tuple2<String, Integer> term) throws Exception {
				Boolean bleft = true;
				if(term._1.contains("_")){
					bleft = false;
				}
				return bleft;
			}
		});
		
		System.out.println(termCountRDD.collect());
		//return labels_idRDD;
	}
}
