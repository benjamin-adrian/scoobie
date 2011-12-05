/*
    Copyright (c) 2011, 
    Benjamin Adrian <benjamin.horak@gmail.com>
    German Research Center for Artificial Intelligence (DFKI) <info@dfki.de>
    
    All rights reserved.

    This file is part of SCOOBIE.

    SCOOBIE is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    SCOOBIE is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with SCOOBIE.  If not, see <http://www.gnu.org/licenses/>.
 */
package de.dfki.km.perspecting.util;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.jung.graph.DirectedGraph;
import edu.uci.ics.jung.graph.Graph;
import edu.uci.ics.jung.graph.MultiGraph;
import edu.uci.ics.jung.graph.UndirectedGraph;

/**
 * Writes graphs in the DOT format.
 * 
 * @author Benjamin Adrian
 */
public class DotWriter<V, E> {
	/**
	 * Creates a new instance.
	 */
	public DotWriter() {
	}

	public void save(Graph<V, E> graph, Writer w) throws IOException {
		save(graph, w, new HashMap<V, String>(), new HashMap<E, String>());
	}

	public void save(Graph<V, E> graph, Writer w,
			Map<V, String> vertexLabels, Map<E, String> edgeLabels)
			throws IOException {

		BufferedWriter writer = new BufferedWriter(w);

		// declare graph type
		if (graph instanceof DirectedGraph<?, ?>) {
			writer.append("digraph {");
			writer.newLine();
		} else if (graph instanceof UndirectedGraph<?, ?>) {
			writer.append("graph {");
			writer.newLine();
		} else if (graph instanceof MultiGraph<?, ?>) {
			writer.append("digraph  {");
			writer.newLine();
		}

		// default node style

		writer.write("size=\"70,70\";");
		writer.newLine();
//		writer.write("node[style=\"filled\", size=\"30,30\"];");
//		writer.newLine();
		writer.write("graph [fontsize=8, ssize = \"70,140\"];");
		writer.newLine();

	
		for (V currentVertex : graph.getVertices()) {
			String label = vertexLabels.get(currentVertex);
			if (label != null) {
				writer.write(currentVertex.toString());
				writer.write("[label=\"" + label + "\"];");
				writer.newLine();
			}

		}

		for (V currentVertex : graph.getVertices()) {
			for (E e : graph.getOutEdges(currentVertex)) {
			
				String label = edgeLabels.get(e);
				if (label == null)
					label = e.toString();
				writer.write(currentVertex.toString() + "->" + graph.getEndpoints(e).getSecond().toString()+ "[label=\"" + label
						+ "\"];");
				writer.newLine();
			}
		}
		// close graph type declaration
		writer.append("}");
		writer.newLine();
		writer.flush();
	}
}
