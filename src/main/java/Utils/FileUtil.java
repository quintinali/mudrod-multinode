package Utils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;

public class FileUtil {

	String httpDir;
	String ftpDir;
	List<String> logSuffixs;
	
	public FileUtil() {
		// TODO Auto-generated constructor stub
		logSuffixs = new ArrayList<String>();
	}

	public String mvSplitfile(Map<String,String> config){
		
		String nodenum = config.get("nodeno");
		String strmonth = config.get("month");
		String[] months = strmonth.split(",");
		List<String> logSuffixs =  new ArrayList<String>();
		for(int i=0; i<months.length; i++){
			//config.put("indexName", oriIndex + months[i]);
			String month = months[i];
			String httpTmpFile = config.get("accesslog") + "." + month + "_part";
			String ftpTmpFile = config.get("ftplog") + "." +  month + "_part";
			
			String suffix1 = this.mvtmpfile(httpTmpFile,nodenum);
			String suffix2 = this.mvtmpfile(ftpTmpFile,nodenum);
			
			logSuffixs.add(suffix1.substring(suffix1.lastIndexOf(".") + 1));
			logSuffixs.add(suffix2.substring(suffix2.lastIndexOf(".") + 1));
		}
		
		List<String> deduped = logSuffixs.stream().distinct().collect(Collectors.toList());
		
		return StringUtils.join(deduped, ",");
		
	}
	
	public String mvtmpfile(String tmpfilename, String nodenum) {

		File matchj = new File(tmpfilename);
		String filename = matchj.getName();
		String suffix = filename.replace("_part", nodenum);
		String parentpath = matchj.getParentFile().getAbsolutePath();
		String newpath = parentpath + "/" + suffix;
		
		System.out.println(tmpfilename);
		System.out.println(newpath);
		matchj.renameTo(new File(newpath));
		
		return suffix;
	}
	
	
	public String mvSplitfile_advance(Map<String, String> config) {

		String strmonth = config.get("month");
		String[] months = strmonth.split(",");
		for (int i = 0; i < months.length; i++) {
			// config.put("indexName", oriIndex + months[i]);
			String month = months[i];
			String HTTPFilePath = config.get("accesslog") + "." + month;
			String FTPFilePath = config.get("ftplog") + "." + month;
			
			this.httpDir = HTTPFilePath;
			this.ftpDir = FTPFilePath;

			System.out.println(HTTPFilePath);

			this.iterDir(HTTPFilePath, 1);
			this.iterDir(FTPFilePath, 2);
		}
		
		List<String> deduped = this.logSuffixs.stream().distinct().collect(Collectors.toList());
		return StringUtils.join(deduped, ",");
	}
	
	public void iterDir(String filepath, int type){
		
		File f = new File(filepath);
		if(!f.isDirectory()){
			return;
		}else{
			File[] childFiles = f.listFiles();
			int childNum = childFiles.length;
			for(int i=0; i<childNum; i++){
				File file = childFiles[i];
				if(file.isDirectory()){
					this.iterDir(file.getPath(), type);
				}else{
					String filename = file.getName();
					if(filename.startsWith("part-0000")){
						this.mvfile(file, type);
					}
				}	
			}
		}
	}
	
	public void mvfile(File file, int type){
		//File matchj = file;
		String filename = file.getName();
		String suffix = filename.replace("part-0000", "");
		String newpath = "";
		if(type == 1){
			newpath = this.httpDir.replace("_d", suffix);	
		}else if(type ==2){
			newpath = this.ftpDir.replace("_d", suffix);	
		}
		
		File newfile = new File(newpath);
	
		file.renameTo(newfile);
		
		String name = newfile.getName();
		String month = name.substring(name.lastIndexOf(".") + 1);
		logSuffixs.add(month);
	}

}
