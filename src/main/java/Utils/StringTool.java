package Utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse.AnalyzeToken;

public class StringTool {

	private static ESNodeClient esnode;
	
	public static void initESNode(ESNodeClient node){
		StringTool.esnode = node;
	}
	
	public static String customAnalyzing(String index, String str) throws InterruptedException, ExecutionException{
		if(str == null){
			return "";
		}
		
		if(index.equals("")){
			index = esnode.index;
		}
		
		String[] str_list = str.split(",");
		for(int i = 0; i<str_list.length;i++)
		{
			String tmp = "";
			AnalyzeResponse r = esnode.client.admin().indices()
					.prepareAnalyze(str_list[i]).setIndex(index).setAnalyzer("cody")
		            .execute().get();
			for (AnalyzeToken token : r.getTokens()) {
				tmp +=token.getTerm() + " ";
		    }
			str_list[i] = tmp.trim();
		}
		
		String analyzed_str = String.join(",", str_list);
		return analyzed_str;
	}
	
	public static Map<String,String>  readConfig(String file) throws IOException{
		//read config file
				Map<String,String> config = new HashMap<String,String>();
				BufferedReader br = new BufferedReader(new FileReader( file), 819200);
				int count =0;
				try {
					String line = br.readLine();
				    while (line != null) {	
				    	if(line.equals("{") || line.equals("}"))
				    	{
				    		line = br.readLine();
				    		continue;
				    	}	
				    	else{
				    		String[] parts = line.trim().split(":");
				    		String filed = parts[0];
				    		String value =  parts[1];
				    		filed = filed.endsWith("\"") ? filed.substring(1, filed.length()-1) : filed;
				    		value = value.endsWith(",") ? value.substring(0, value.length()-1).trim() : value.trim();
				    		value = value.endsWith("\"") ? value.substring(1, value.length()-1) : value;
				    		config.put(filed, value);
				    	}
				    	
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
				  
				}
				return config;
	}

}
