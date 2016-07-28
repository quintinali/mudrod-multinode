package MUDROD.SessionRecon;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;  
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import Utils.ESNodeClient;
import Utils.StringTool;  
  
public class TestEngine implements Runnable   
{  
  public boolean running = false;  
  String HTTPfileName;
  String FTPfileName;
  String configFile;
  
  public TestEngine (int i ,String config)  
  {  
    Thread thread = new Thread(this);  
    thread.start();  
    
    //this.HTTPfileName = "D:/STCenter Projects/Mudrod/aist/accesslog201502/access_log.podaac.20150" + i;
    ///this.FTPfileName = "D:/STCenter Projects/Mudrod/aist/ftplog201502/podaac-ftp.log.20150" + i;
    if(i>2){
    	i=i+1;		
    }
    this.HTTPfileName = "../podacclog/access_log.20140" + i;
    this.FTPfileName = "../podacclog/vsftpd.log.20140" + i;
    
    this.configFile = config;
  }  
  
  @Override  
  public void run()   
  {  
    this.running = true;  
    System.out.println("This is currently running on a separate thread, " +  
        "the id is: " + Thread.currentThread().getId());  
      
    try   
    {  
      // this will pause this spawned thread for 5 seconds  
      //  (5000 is the number of milliseconds to pause)  
      // Also, the Thread.sleep() method throws an InterruptedException  
      //  so we must "handle" this possible exception, that's why I've  
      //  wrapped the sleep() method with a try/catch block  
    	Thread.sleep(100);  
    	
    	Map<String,String> config = StringTool.readConfig(configFile);
    	ESNodeClient esnode = new ESNodeClient(config);
   		Preprocesser test = new Preprocesser(esnode, config.get("indexName"));
    	test.processMonthlyData("1");
		this.running = false;  
    }   
    catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (ParseException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (ExecutionException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}  
    
  }  
    
  public static void main (String[] args) throws InterruptedException, IOException  
  {  
   
		String configFile = "";
		if(args.length ==0){
			configFile = "../podacclog/config.json";
		}else{
			configFile = args[0];
		}
		
	 Map<String,String> config = StringTool.readConfig(configFile);
	 ESNodeClient esnode = new ESNodeClient(config);
	Preprocesser test = new Preprocesser(esnode, config);
    //ESNodeClient esnode = new ESNodeClient();
	//Preprocesser test = new Preprocesser("aistcloud", esnode);
	StringTool.initESNode(esnode);//important!
	
    System.out.println("This is currently running on the main thread, " +  
        "the id is: " + Thread.currentThread().getId());  
    
    List<TestEngine> workers = new ArrayList<TestEngine>();  
    int clientNum = Integer.parseInt(config.get("client_num"));
    for (int i=0; i<clientNum; i++)  
    {  
      workers.add(new TestEngine(i+1, configFile));   
    }  
    
  
    Date start = new Date();  
    // We must force the main thread to wait for all the workers  
    //  to finish their work before we check to see how long it  
    //  took to complete  
    for (TestEngine worker : workers)  
    {  
      while (worker.running)  
      {  
        Thread.sleep(100);  
      }  
    }  
      
    Date end = new Date();  
      
    long difference = end.getTime() - start.getTime();  
      
    System.out.println ("This whole process took: " + difference/1000 + " seconds.");  
  }  
    
 
}  