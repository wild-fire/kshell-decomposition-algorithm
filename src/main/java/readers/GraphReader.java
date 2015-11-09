package readers;


import java.io.BufferedReader;
import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.spark.api.java.JavaSparkContext;

import vos.Graph;

public class GraphReader {

	public static Graph read(JavaSparkContext sparkContext) throws IOException, URISyntaxException{

		Graph graph = new Graph();
		 
		for(String line : sparkContext.textFile(sparkContext.getConf().get("com.wildfire.graph_file_path")).toArray()) {
			graph.addEdge(line);
		}
		
		return graph;		
		
	}
	
	public static Graph read(BufferedReader graphFile) throws IOException, URISyntaxException{

		Graph graph = new Graph();
		 
		while(graphFile.ready()) {
			graph.addEdge(graphFile.readLine());
		}
		
		return graph;		
		
	}
	
	
	

}
