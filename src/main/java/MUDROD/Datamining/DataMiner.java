package MUDROD.Datamining;

import java.io.IOException;
import java.io.Serializable;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.codehaus.jettison.json.JSONException;
import org.elasticsearch.index.query.QueryBuilders;
import MUDROD.Datamining.Metadata.GCMDTermSVD;
import MUDROD.Datamining.intergration.LinkageGenerator;
import MUDROD.Datamining.logAnalyzer.ClickStreamSVD;
import MUDROD.Datamining.logAnalyzer.QueryRecomFilters;
import MUDROD.Datamining.logAnalyzer.UserBagClustering;
import Utils.DeleteRedType;
import Utils.ESNode;
import Utils.ESNodeClient;
import Utils.SparkNode;
import Utils.StringTool;
import Utils.jobUtil;

//MUDROD.Datamining.DataMiner
public class DataMiner implements Serializable {

	//private static final long serialVersionUID = -4236401121758176524L;
	public String index;
	public String Cleanup_type = "cleanupLog";
	public String SessionStats = "sessionStatic";
	public String PODAACkeywordsType = "podaacKeywords";
	
	//public ESNodeClient esnode;
	public Map<String,String> config;

	public DataMiner(ESNodeClient node, Map<String,String> configMap) throws IOException {
		this.index = configMap.get("indexName");
	//	this.esnode = node;
		this.config = configMap;
	}
	
	public DataMiner() throws IOException {
		this.index = "podaacsession";
	}
	
	public DataMiner(String Index) throws IOException {
		this.index = Index;
	}

	public void clickStreamAnalysis(ESNodeClient esnode,String clickCSV) throws IOException {
		ClickStreamSVD gen = new ClickStreamSVD(this);
		gen.calSematincSimilarity(esnode, clickCSV);
	}
	
	public void clickStreamAnalysis(ESNodeClient esnode,String index, String type) throws IOException {
		ClickStreamSVD gen = new ClickStreamSVD(this);
		gen.calSematincSimilarity(esnode,index,type);
	}
	
	public void userQueryAnalysis(ESNodeClient esnode,String userCSV) throws IOException {
		UserBagClustering test = new UserBagClustering(this);
		test.GenerateBinaryMatrix(esnode,userCSV);
	}
	
	public void metadataAnalysis(String gcmdCSV) throws IOException, NoSuchAlgorithmException, InterruptedException, ExecutionException {
		GCMDTermSVD svdtest = new GCMDTermSVD();
		svdtest.calSematincSimilarity(gcmdCSV);
	}
	
	public void metadataAnalysis(ESNodeClient esnode,String index, String type) throws IOException, NoSuchAlgorithmException, InterruptedException, ExecutionException, JSONException {
		GCMDTermSVD svdtest = new GCMDTermSVD();
		svdtest.calSematincSimilarity(esnode,index, type);
	}
	
	public void metadataAnalysis(ESNodeClient esnode, String metadatatxt, String index, String type) throws IOException, NoSuchAlgorithmException, InterruptedException, ExecutionException, JSONException {
		GCMDTermSVD svdtest = new GCMDTermSVD();
		svdtest.calSematincSimilarity(esnode,metadatatxt, index, type);
	}
	
	public void importCSV(ESNodeClient esnode, String userCSV, String clickCSV, String GCMDCSV, String sweetCSV) throws Exception 
	{
		LinkageGenerator lg= new LinkageGenerator(this);
		lg.importToES(esnode, userCSV, "userHistory");
		//lg.importToES(clickCSV, "userClicking");
		//lg.importToES(GCMDCSV, "GCMDMetadata");
		lg.importSWEEToES(esnode, sweetCSV, "SWEET");
	}
	
	public void DeleteRedTyeps(){
		DeleteRedType drt = new DeleteRedType();
		drt.deleteAllByQuery(index, "userHistory", QueryBuilders.matchAllQuery());
		drt.deleteAllByQuery(index, "GCMDMetadata", QueryBuilders.matchAllQuery());
		drt.deleteAllByQuery(index, "userClicking", QueryBuilders.matchAllQuery());
		drt.deleteAllByQuery(index, "SWEET", QueryBuilders.matchAllQuery());
		ESNode.RefreshIndex();
	}
	

	
	public static void main1(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();
		
		SparkNode sparkNode = new SparkNode();
		
		DataMiner test = new DataMiner("podaacsession");
		ESNode esnode = new ESNode(test.index);
		//delete type
		//test.DeleteRedTyeps();
		//windows
		//input file
		/*String metadataTxt = "D:/test/metadata.txt";
		String sweetCSV = "D:/test/Ocean_triples.csv";
		//output file
		String GCMDCSV = "D:/test/distResult_SVD_GCMD_sim.csv";
		String userCSV = "D:/test/clusteringmatrix_userbags_bi_filtered.csv";
		String clickCSV = "D:/test/distResult_SVD_Query_sim.csv";*/
		
		//linux
		//input file
		String metadataTxt = "../tmp/metadata.txt";
		String sweetCSV = "../tmp/Ocean_triples.csv";
		//output file
		String GCMDCSV = "../tmp/distResult_SVD_GCMD_sim.csv";
		String userCSV = "../tmp/clusteringmatrix_userbags_bi_filtered.csv";
		String clickCSV = "../tmp/distResult_SVD_Query_sim.csv";

	/*	test.clickStreamAnalysis(test.index, "userClicking");
		System.out.println("done click stream analysis");
		test.userQueryAnalysis(userCSV);
		System.out.println("done user query analysis");
		test.metadataAnalysis(test.index, "GCMDMetadata");
		System.out.println("done metadata analysis");
		*/
		
		
		//intergration
		//test.importCSV(userCSV, clickCSV, GCMDCSV, sweetCSV);

		SparkNode.sc.stop();
		ESNode.bulkProcessor.awaitClose(20, TimeUnit.MINUTES);
		ESNode.node.close();  
        
		long endTime=System.currentTimeMillis();
		System.out.println("Preprocessing is done!" + "Time elapsed： "+ (endTime-startTime)/1000+"s");
	}
	
	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();
		
		String configFile = "";
		if(args.length ==0){
			configFile = "../podacclog/config.json";
		}else{
			configFile = args[0];
		}

		Map<String,String> config = StringTool.readConfig(configFile);
		
		//for workflow
		 if(config.get("workflow") !=null && config.get("workflow").equals("true")){
			 String jobname = config.get("jobname");
			 String nodetag = config.get("nodetag");
			 String workflowUrl = config.get("workflowUrl");
			 jobUtil.addJob(workflowUrl, jobname, nodetag, "minelog", startTime);
		}
		 
		SparkNode sparkNode = new SparkNode();
		ESNodeClient esnode = new ESNodeClient(config);
		esnode.InitiateIndex(config);
		DataMiner test = new DataMiner(esnode,config);
		StringTool.initESNode(esnode);//important!
		
		esnode.RefreshIndex();
		
		//delete type
		//test.DeleteRedTyeps();
		//windows
		//input file
		String metadataTxt = "D:/test/metadata.txt";
		String sweetCSV = "D:/test/Ocean_triples.csv";
		//output file
		String GCMDCSV = "D:/test/distResult_SVD_GCMD_sim.csv";
		String userCSV = "D:/test/clusteringmatrix_userbags_bi_filtered.csv";
		String clickCSV = "D:/test/distResult_SVD_Query_sim.csv";
		
		//linux
		//input file
		/*String metadataTxt = "../tmp/metadata.txt";
		String sweetCSV = "../tmp/Ocean_triples.csv";
		//output file
		String GCMDCSV = "../tmp/distResult_SVD_GCMD_sim.csv";
		String userCSV = "../tmp/clusteringmatrix_userbags_bi_filtered.csv";
		String clickCSV = "../tmp/distResult_SVD_Query_sim.csv";*/

		test.clickStreamAnalysis(esnode,test.index, "userClicking");
		System.out.println("done click stream analysis");
		test.userQueryAnalysis(esnode,userCSV);
		System.out.println("done user query analysis");
		test.metadataAnalysis(esnode, metadataTxt, test.index, "GCMDMetadata");
		System.out.println("done metadata analysis");
		
		//intergration
		test.importCSV(esnode,userCSV, clickCSV, GCMDCSV, sweetCSV);

		SparkNode.sc.stop();
		ESNode.bulkProcessor.awaitClose(20, TimeUnit.MINUTES);
		ESNode.node.close();  
        
		long endTime=System.currentTimeMillis();
		long lasttime=(endTime-startTime)/1000;
		System.out.println("Preprocessing is done!" + "Time elapsed： "+ lasttime+"s");
		
		//for workflow
		 if(config.get("workflow") !=null && config.get("workflow").equals("true")){
			 String jobname = config.get("jobname");
			 String nodetag = config.get("nodetag");
			 String workflowUrl = config.get("workflowUrl");
			 jobUtil.stopJob(workflowUrl, jobname, nodetag, "minelog", lasttime);
		}
	}
}
