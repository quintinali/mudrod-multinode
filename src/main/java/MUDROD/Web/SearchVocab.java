package MUDROD.Web;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.ExecutionException;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import com.google.gson.JsonObject;

import MUDROD.Datamining.DataMiner;
import MUDROD.Datamining.intergration.LinkageGenerator;
import MUDROD.Datamining.logAnalyzer.QueryRecomFilters;


/**
 * Servlet implementation class SearchVocab
 */
@WebServlet("/SearchVocab")
public class SearchVocab extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public SearchVocab() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		String concept = request.getParameter("concept");
		PrintWriter out = response.getWriter();
	
		if(concept!=null)
		{
			response.setContentType("application/json");  
			response.setCharacterEncoding("UTF-8");

			DataMiner prep = new DataMiner();
			LinkageGenerator lg= new LinkageGenerator(prep);
			//QueryRecomFilters ck = new QueryRecomFilters(prep);
				
			JsonObject json_kb = new JsonObject();
			JsonObject json_graph = null;
			JsonObject json_filter = null;

			try {
				json_graph = lg.appyMajorRuleToJson(concept);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//json_filter = ck.getRecomFilter(concept);
			json_kb.add("graph", json_graph);
			json_kb.add("filters", new JsonObject());
			out.print(json_kb.toString());
			out.flush();
			
			System.out.println("Well Done!" + "\n");

			//Ek_test.node.close();
		}else{
			out.print("Please input query");
			out.flush();
		}
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
	}

}
