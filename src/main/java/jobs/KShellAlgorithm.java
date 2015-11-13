package jobs;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import readers.GraphReader;
import scala.Tuple2;
import vos.Graph;

public class KShellAlgorithm {

	public static void main(String[] args) throws IOException, URISyntaxException {
		
		SparkConf sparkConf = new SparkConf().setAppName("KShell").setMaster("local[2]").set("spark.executor.memory","4g");
		sparkConf.set("com.wildfire.graph_file_path", args[0]);

		JavaSparkContext ctx = new JavaSparkContext(sparkConf);

		System.out.println("Leyendo grafo");

		// First, we read the graph and get the original MMS
		Graph graph = GraphReader.read(ctx);

		// Now, initialize required variables by the algorithm
		
		// First, the shells hash. It's a hash where the key is the number of the shell and the value the set of nodes for that shell
		List<Tuple2<Integer, HashSet<String>>> shells = new ArrayList<Tuple2<Integer, HashSet<String>>>();
		// Now the current shell we are extracting
		int currentShell = 1;
		
		
		// The algorithm is going to destroy the graph (we are removing all its nodes), so we stop when it's empty
		while(!graph.isEmpty()) {

			System.out.println("Shell " + currentShell);
			System.out.println("Graph " + graph.getNumberOfNodes());
			
			shells.add(new Tuple2<Integer, HashSet<String>>(currentShell, new HashSet<String>()));
			HashSet<String> newNodes; 
			while((newNodes = graph.getNodesWithDegreeAtMost(currentShell)).size() != 0) {
				shells.get(currentShell-1)._2.addAll(newNodes);
				System.out.println(" -->" + newNodes);
				for(String node : newNodes) {
					graph.removeNode(node);
				}
			}
			currentShell++;
		}
		
		System.out.println(shells);
		
		JavaPairRDD<String, Integer> nodesInShells =  ctx.parallelizePairs(shells).flatMapToPair(new PairFlatMapFunction<Tuple2<Integer,HashSet<String>>, String, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 5738450717020608954L;

			public Iterable<Tuple2<String, Integer>> call(
					Tuple2<Integer, HashSet<String>> shell) throws Exception {

				System.out.println("Extracting Shell " + shell._1);
				
				List<Tuple2<String, Integer>> tuples = new ArrayList<Tuple2<String,Integer>>();
				
				for(String nodeShell: shell._2) {
					tuples.add(new Tuple2<String, Integer>(nodeShell, shell._1));
				}
				// TODO Auto-generated method stub
				return tuples;
			}
		});
		
		nodesInShells.saveAsTextFile(args[1]);

	}

}
