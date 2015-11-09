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

	public void addEdge(String source, String target) {
		if (!this.neighbours.containsKey(source)) {
			this.neighbours.put(source, new HashSet<String>());
		}
		if (!this.neighbours.containsKey(target)) {
			this.neighbours.put(target, new HashSet<String>());
		}
		this.neighbours.get(source).add(target);
	}


	public void addEdge(String line) {
		String[] lineInfo = line.split("\t");
		if(lineInfo.length >= 2) {
			// The edge is reversed because in our graph file A -> B means A mentions B and we are actually interested in the opposite (i.e. B influences A) 
			this.addEdge(lineInfo[1], lineInfo[0]);
		}		
	}
	
	public Graph clone() {
		Graph g = new Graph();
		for (String source : Collections.list(neighbours.keys())) {
			HashSet<String> targets = neighbours.get(source);
			for (String target : targets) {
				g.addEdge(source, target);
			}
		}
		return g;
	}

	public void removeIncomingEdge(String node) {
		for (HashSet<String> neighbours : this.neighbours.values()) {
			neighbours.remove(node);
		}
	}

}
