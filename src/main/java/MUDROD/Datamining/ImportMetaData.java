package MUDROD.Datamining;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import org.codehaus.jettison.json.JSONException;
import MUDROD.Datamining.Metadata.ESdriver;
import Utils.ESNode;

//MUDROD.Datamining.ImportMetaData
public class ImportMetaData implements Serializable {

	public void importMetaData(String metadataTxt) throws IOException, InterruptedException, JSONException {
		ESdriver esd = new ESdriver();
		esd.putMapping();
		esd.importToES(metadataTxt);
		ESNode.RefreshIndex();
		esd.addCompletion();
	}
	

	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();

		ImportMetaData test = new ImportMetaData();
		ESNode esnode = new ESNode();

		//String metadataTxt = "D:/test/metadata.txt";
		//linux
		String metadataTxt = "../tmp/metadata.txt";
		
		test.importMetaData(metadataTxt);
	
		ESNode.bulkProcessor.awaitClose(20, TimeUnit.MINUTES);
		ESNode.node.close();  
        
		long endTime=System.currentTimeMillis();
		System.out.println("importing metadata is done!" + "Time elapsed： "+ (endTime-startTime)/1000+"s");
	}
}
