package vos;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;

public class Graph implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4242271276031423750L;
	private Hashtable<String, HashSet<String>> neighbours = new Hashtable<String, HashSet<String>>();
	private Hashtable<Integer, HashSet<String>> degrees = new Hashtable<Integer, HashSet<String>>();

	public Enumeration<String> getNodes() {
		return this.neighbours.keys();
	}

	public int getNumberOfNodes() {
		return this.neighbours.keySet().size();
	}

	public Iterator<String> getNeighboursIterator(String node) {
		return this.neighbours.get(node).iterator();
	}

	public Set<String> getNeighbours(String node) {
		return this.neighbours.get(node);
	}

	public Hashtable<String, HashSet<String>> getNeighbours() {
		return this.neighbours;
	}
	
	public boolean isEmpty() {
		return this.getNumberOfNodes() == 0;
	}

	public void addEdge(String source, String target) {
		// First, we add both nodes to the edges and degrees sets
		if (!this.neighbours.containsKey(source)) {
			this.neighbours.put(source, new HashSet<String>());
		}
		if (!this.neighbours.containsKey(target)) {
			this.neighbours.put(target, new HashSet<String>());
		}
		if (!this.degrees.containsKey(this.neighbours.get(source).size())) {
			this.degrees.put(this.neighbours.get(source).size(), new HashSet<String>());
		}

		if (!this.degrees.containsKey(this.neighbours.get(target).size())) {
			this.degrees.put(this.neighbours.get(target).size(), new HashSet<String>());
		}
		// Then remove them from the degrees hashes
		this.degrees.get(this.neighbours.get(source).size()).remove(source);
		this.degrees.get(this.neighbours.get(target).size()).remove(target);	
		
		// Then we add edges in both directions. In the K-Shell decomposition algorithms we don't bother about directed edges
		this.neighbours.get(source).add(target);
		this.neighbours.get(target).add(source);

		// And then re-add them to the degree hashes
		if (!this.degrees.containsKey(this.neighbours.get(source).size())) {
			this.degrees.put(this.neighbours.get(source).size(), new HashSet<String>());
		}

		if (!this.degrees.containsKey(this.neighbours.get(target).size())) {
			this.degrees.put(this.neighbours.get(target).size(), new HashSet<String>());
		}
		this.degrees.get(this.neighbours.get(source).size()).add(source);
		this.degrees.get(this.neighbours.get(target).size()).add(target);		
	}


	public void addEdge(String line) {
		String[] lineInfo = line.split("\t");
		if(lineInfo.length >= 2) {
			// The edge is reversed because in our graph file A -> B means A mentions B and we are actually interested in the opposite (i.e. B influences A) 
			this.addEdge(lineInfo[1], lineInfo[0]);
		}		
	}
	
	public HashSet<String> getNodesWithDegreeAtMost(int degree){

		HashSet<String> result = new HashSet<String>();
		
		for(int i = 0; i <= degree; i++) {
			result.addAll(this.getNodesWithDegree(i));
		}
		return result;
	}
	
	
	public HashSet<String> getNodesWithDegree(int degree){

		if (this.degrees.containsKey(degree)) {
			return (HashSet<String>) this.degrees.get(degree).clone();
		} else {
			return new HashSet<String>();
		}
	}
	
	public void removeNode(String node) {
		for(String neighbour : (Iterable<String>) this.neighbours.get(node).clone()) {
			// Then, we remove the nodes from the degrees hash
			this.degrees.get(this.neighbours.get(neighbour).size()).remove(neighbour);
			this.neighbours.get(neighbour).remove(node);
			this.degrees.get(this.neighbours.get(neighbour).size()).add(neighbour);			
		}
		this.degrees.get(this.neighbours.get(node).size()).remove(node);
		this.neighbours.remove(node);
	}

}
