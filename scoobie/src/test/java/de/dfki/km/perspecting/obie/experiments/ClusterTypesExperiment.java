/*
    Copyright (c) 2011, 
    Benjamin Adrian <benjamin.horak@gmail.com>
    
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

package de.dfki.km.perspecting.obie.experiments;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.junit.AfterClass;
import org.junit.Test;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.rio.RDFFormat;
import org.openrdf.sail.inferencer.fc.ForwardChainingRDFSInferencer;
import org.openrdf.sail.memory.MemoryStore;
import org.postgresql.jdbc2.optional.PoolingDataSource;

import cern.colt.function.IntIntDoubleFunction;
import cern.colt.matrix.DoubleMatrix1D;
import cern.colt.matrix.DoubleMatrix2D;
import cern.colt.matrix.doublealgo.Statistic;
import de.dfki.km.perspecting.obie.connection.PostgresKB;
import de.dfki.km.perspecting.obie.model.DataSheet;
import de.dfki.km.perspecting.obie.model.DoubleMatrix;

public class ClusterTypesExperiment {

	private static String $PHD_HOME = "experiments/";

	private static String $DATABASE_SERVER_USER = "postgres";
	private static String $DATABASE_SERVER_PW = "scoobie";
	private static int $DATABASE_SERVER_PORT = 5432;

	private static String $DATABASE_DBPEDIA_en2 = "dbpedia_en2";
	private static String $DATABASE_BBC_MUSIC = "bbc_music";
	private static String $DATABASE_BBC_WILDLIFE = "bbc_wildlife";
	
	private static String $DATABASE_SERVER_PC_4327 = "pc-4327.kl.dfki.de";
	private static String $DATABASE_SERVER_LOCALHOST = "localhost";
	
	private static PoolingDataSource pool = new PoolingDataSource();

	private static PostgresKB kb;

	/**
	 * @throws java.lang.Exception
	 */
	public static void setUp(String $DATABASE_SERVER, String $DATABASE) throws Exception {
		pool.setUser($DATABASE_SERVER_USER);
		pool.setPassword($DATABASE_SERVER_PW);
		pool.setPortNumber($DATABASE_SERVER_PORT);
		pool.setDatabaseName($DATABASE);
		pool.setServerName($DATABASE_SERVER);
		pool.setMaxConnections(10);
		kb = new PostgresKB(pool.getConnection(), $DATABASE, new URI("http://test.de"));
	}

	@AfterClass
	public static void shutdownAfterClass() {
		pool.close();
	}

	@Test
	public void testHierarchicalClusteringDBpedia() throws Exception {
		
		setUp($DATABASE_SERVER_PC_4327, $DATABASE_DBPEDIA_en2);
		
		final int countSamples = 20;

		kb.clusterCorrelatingClasses(countSamples, 0.9, 0.175);
		

	}
	
	@Test
	public void testHierarchicalClusteringBBCMusic() throws Exception {
		
		setUp($DATABASE_SERVER_LOCALHOST, $DATABASE_BBC_MUSIC);
		
		final int countSamples = 20;
		kb.clusterCorrelatingClasses(countSamples, 0.9, 0.175);

	}
	
	
	@Test
	public void testHierarchicalClusteringBBCNature() throws Exception {
		
		setUp($DATABASE_SERVER_LOCALHOST, $DATABASE_BBC_WILDLIFE);
		
		final int countSamples = 10;
		kb.clusterCorrelatingClasses(countSamples, 0.9, 0.175);

	}
	
	

	public Map<String, Set<String>> getDBpediaOntology() throws Exception {

		Map<String, Set<String>> ce = new HashMap<String, Set<String>>();

		MemoryStore store = new MemoryStore();
		ForwardChainingRDFSInferencer inferencer = new ForwardChainingRDFSInferencer(
				store);
		SailRepository repos = new SailRepository(inferencer);
		repos.initialize();

		URL url = new URL(
				"http://pc-4323:8890/sparql?default-graph-uri=&should-sponge=&query=construct+{%3Fc1+rdfs%3AsubClassOf+%3Fc2}+where+{%3Fc1+rdfs%3AsubClassOf+%3Fc2.+FILTER+regex%28%3Fc1%2C+%22http%3A%2F%2Fdbpedia.org%2Fontology%2F%22%29+}");
		SailRepositoryConnection conn = repos.getConnection();
		conn.add(url, null, RDFFormat.RDFXML);
		conn.commit();
		TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL,
				"SELECT DISTINCT ?s ?o WHERE {?s <"
						+ RDFS.SUBCLASSOF.toString() + "> ?o}");
		query.setIncludeInferred(true);
		TupleQueryResult rs = query.evaluate();

		while (rs.hasNext()) {
			BindingSet bs = rs.next();

			String s = bs.getBinding("s").getValue().stringValue();
			String o = bs.getBinding("o").getValue().stringValue();

			if (s.startsWith("http://www.w3.org/2000/01/rdf-schema#")
					|| o.startsWith("http://www.w3.org/2000/01/rdf-schema#")) {
				continue;
			}

			if (s.startsWith("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
					|| o
							.startsWith("http://www.w3.org/1999/02/22-rdf-syntax-ns#")) {
				continue;
			}

			Set<String> so = ce.get(s);
			if (so == null) {
				so = new HashSet<String>();
				ce.put(s, so);
			}

			Set<String> os = ce.get(o);
			if (os == null) {
				os = new HashSet<String>();
				ce.put(o, os);
			}

			so.add(o);
			os.add(s);

		}
		// System.out.println(ce);
		rs.close();
		conn.close();
		repos.shutDown();

		return ce;
	}

	private double[] compare(Map<String, Set<String>> cluster,
			Map<String, Set<String>> taxonomy) {

		double R = 0;
		double P = 0;

		for (Entry<String, Set<String>> e : taxonomy.entrySet()) {

			String concept = e.getKey();
			Set<String> taxonomyCE = new HashSet<String>(e.getValue());

			if (cluster.get(concept) != null) {
				Set<String> clusterCE = new HashSet<String>(cluster
						.get(concept));
				Set<String> taxonomyCSC = new HashSet<String>(taxonomyCE);
				//
				// System.out.println();
				// System.out.println(e.getKey());
				// System.out.println("c:" + clusterCE);
				// System.out.println("t:" + taxonomyCE);
				taxonomyCSC.retainAll(clusterCE);

				clusterCE.retainAll(taxonomyCE);

				// System.out.println();

				double localRecall = ((double) clusterCE.size())
						/ ((double) taxonomyCE.size());
				double localPrecision = ((double) clusterCE.size())
						/ ((double) cluster.get(concept).size());
				//
				// System.out.println(localRecall);
				// System.out.println(localPrecision);
				R += localRecall;
				P += localPrecision;
			}

		}

		return new double[] { P / cluster.size(), R / taxonomy.size() };

	}

	@Test
	public void testClustering() throws Exception {
		
		setUp($DATABASE_SERVER_PC_4327, $DATABASE_DBPEDIA_en2);
		
		final int countSamples = 20;
		// final double threshold = 0.75;
		kb.clusterCorrelatingClasses(countSamples, 0.9, 0.175);
		final DoubleMatrix data = kb.getTypeCorrelations(countSamples);
		// final DoubleMatrix2D cp = data.conditionalProbabiltyDoubleMatrix(0);
		//		
		// Formatter f = new Formatter();
		// f.setFormat("%1.1f");
		// f.setColumnSeparator("\t");
		// String s = f.toString(cp);
				
		data.serialize(new FileWriter($PHD_HOME + "results/" + $DATABASE_DBPEDIA_en2
		 + "/types_cooccurence.csv"), kb);

		DoubleMatrix2D cov = data.covarianceMatrix();
		DoubleMatrix2D cor = Statistic.correlation(cov);

		Map<String, Set<String>> taxonomy = getDBpediaOntology();

		System.out.println(taxonomy.size());

		System.setOut(new PrintStream($PHD_HOME + "results/" + $DATABASE_DBPEDIA_en2 + "/clustering.csv", "utf-8"));
		
		double[] thresholds = new double[] { 0.0, 0.025, 0.05, 0.075, 0.1,
				0.125, 0.15, 0.175, 0.2, 0.225, 0.25, 0.275, 0.3, 0.35, 0.4,
				0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1.0 };

		for (double t : thresholds) {

			DoubleMatrix2D pcaMatrix = data.pca(1.0-t, cor);
			HashMap<String, Set<String>> pcaClustering = clusterWithPCA(data,
					pcaMatrix);
			double[] r = compare(pcaClustering, taxonomy);
			System.out.printf("pca\t%1.2f\t%s\t%1.5f\t%1.5f\n", 1.0-t,
					pcaClustering.size(), r[0], r[1]);
			 printClustering(pcaClustering, $PHD_HOME + "results/" + $DATABASE_DBPEDIA_en2 + "/pca"+pcaClustering.size()+".dot");
		}

		DoubleMatrix2D[] hMatrix = data.hierarchicalLabeledClustering(cor, 0.9,
				thresholds);
		int i = 0;
		for (DoubleMatrix2D m : hMatrix) {
			HashMap<String, Set<String>> hcClustering = clusterWithHC(data, m);
			double[] r = compare(hcClustering, taxonomy);
			System.out.printf("hc\t%1.3f\t%s\t%1.5f\t%1.5f\n", thresholds[i++],
					hcClustering.size(), r[0], r[1]);
			printClustering(hcClustering, $PHD_HOME + "results/"
					+ $DATABASE_DBPEDIA_en2 + "/hc" + hcClustering.size() + ".dot");
		}

	}

	private String strip(String uri) {
		int i = uri.lastIndexOf("#");
		if (i < 0)
			i = uri.lastIndexOf("/");

		return uri.substring(i + 1).replaceAll("-", "");
	}

	private void printClustering(Map<String, Set<String>> clustering,
			String path) throws IOException {

		BufferedWriter w = new BufferedWriter(new FileWriter(path));

		w.append("digraph {\n");

		for (String k : clustering.keySet()) {
			for (String v : clustering.get(k)) {
				w.append(strip(v) + " -> " + strip(k) + ";");
				w.newLine();
			}

		}

		w.append("}");
		w.close();
	}

	/**
	 * @param data
	 * @param hMatrix
	 * @return
	 * @throws Exception
	 */
	private HashMap<String, Set<String>> clusterWithHC(final DoubleMatrix data,
			DoubleMatrix2D hMatrix) throws Exception {
		HashMap<String, Set<String>> clusteredHierarchy1 = new HashMap<String, Set<String>>();

		double maxValue = 0;
		String bestLabel = "";
		HashSet<String> clusterValues = new HashSet<String>();

		for (int row = 0; row < hMatrix.rows(); row++) {
			DoubleMatrix1D projectedColumn = hMatrix.viewRow(row);

			clusterValues = new HashSet<String>();
			maxValue = -1;
			bestLabel = "";

			for (int col = 0; col < hMatrix.columns(); col++) {
				if (projectedColumn.get(col) > 0) {
					clusterValues.add(kb.getURI(data.getColKeys()[col]));
					if (projectedColumn.get(col) > maxValue) {
						maxValue = projectedColumn.get(col);
						bestLabel = kb.getURI(data.getColKeys()[col]);
					}
				}
			}

			for (String type : clusterValues) {
				Set<String> s = clusteredHierarchy1.get(bestLabel);
				if (s == null) {
					s = new HashSet<String>();
					clusteredHierarchy1.put(bestLabel, s);
				}
				s.add(type);
			}
		}
		return clusteredHierarchy1;
	}

	/**
	 * @param data
	 * @param pcaMatrix
	 * @return
	 * @throws Exception
	 */
	private HashMap<String, Set<String>> clusterWithPCA(
			final DoubleMatrix data, DoubleMatrix2D pcaMatrix) throws Exception {
		Map<Integer, List<Double[]>> pcaClusters = new HashMap<Integer, List<Double[]>>();
		//
		// System.out.println(pcaMatrix.rows());
		for (int row = 0; row < pcaMatrix.rows(); row++) {
			double maxValue = Double.NEGATIVE_INFINITY;
			int maxCol = 0;
			DoubleMatrix1D projectedColumn = pcaMatrix.viewRow(row);
			for (int col = 0; col < pcaMatrix.columns(); col++) {
				if (projectedColumn.get(col) > maxValue) {
					maxValue = projectedColumn.get(col);
					maxCol = col;
				}
			}
			List<Double[]> entries = pcaClusters.get(maxCol);
			if (entries == null) {
				entries = new ArrayList<Double[]>();
				pcaClusters.put(maxCol, entries);
			}
			entries.add(new Double[] { new Double(data.getColKeys()[row]),
					new Double((int)(maxValue * 100) )});
		}

		// assert that most significant cluster values are at high rank
		for (List<Double[]> c : pcaClusters.values()) {
			Collections.sort(c, new Comparator<Double[]>() {
				@Override
				public int compare(Double[] o1, Double[] o2) {
					return (int) Math.signum(o1[1] - o2[1]);
				}
			});
		}

		HashMap<String, Set<String>> clusteredHierarchy = new HashMap<String, Set<String>>();

		for (Integer k : pcaClusters.keySet()) {
			// System.out.println(k);

			List<Double[]> cluster = pcaClusters.get(k);

			String father = kb.getURI(cluster.get(cluster.size() - 1)[0]
					.intValue());

			Set<String> set = clusteredHierarchy.get(father);
			if (set == null) {
				set = new HashSet<String>();
				clusteredHierarchy.put(father, set);
			}

			for (Double[] d : pcaClusters.get(k)) {
				String son = kb.getURI((int) d[0].intValue());
				set.add(son);
				// System.out.println(son + " " + String.format("%1.2f", d[1]));
			}

			// System.out.println();
		}
		return clusteredHierarchy;
	}

	/**
	 * @param countSamples
	 * @param threshold
	 * @param data
	 * @param mult
	 * @return
	 */
	private DoubleMatrix2D trimMatrix(final int countSamples,
			final double threshold, final DataSheet data, DoubleMatrix2D mult) {
		mult = mult.forEachNonZero(new IntIntDoubleFunction() {
			@Override
			public double apply(int first, int second, double argument) {

				Double d = data.get(data.getColumns()[first],
						data.getColumns()[second]);

				return d != null && d > 0.9 * countSamples
						&& argument >= threshold ? 1 : 0;
			}
		});
		return mult;
	}

	// double[][] conditionalPropabilities = data
	// .conditionalProbabiltyDoubleMatrix(data.getColumns());

	// BitMatrix bm = new BitMatrix(conditionalPropabilities.length,
	// conditionalPropabilities.length);

	// DoubleMatrix2D m = new SparseDoubleMatrix2D(
	// new double[][]
	// {{1.0, 1.0, 1.0, 0.0, 0.0},
	// {0.0, 1.0, 0.0, 0.0, 0.0},
	// {0.0, 0.0, 1.0, 1.0, 1.0},
	// {0.0, 0.0, 0.0, 1.0, 0.0},
	// {0.0, 0.0, 0.0, 0.0, 1.0}
	// }
	// );
	//		
	// m = m.zMult(m, new SparseDoubleMatrix2D(m.columns(),m.columns()));
	// m = m.zMult(m, new SparseDoubleMatrix2D(m.columns(),m.columns()));
	// m = m.zMult(m, new SparseDoubleMatrix2D(m.columns(),m.columns()));
	//		
	// System.out.println(m);
	// final SparseDoubleMatrix2D bm = new SparseDoubleMatrix2D(data
	// .conditionalProbabiltyDoubleMatrixUpper(0.5 * countSamples,
	// data.getColumns()));

	// System.out.println(bm);
	// System.out.println();

	// DoubleMatrix2D mult = bm.copy();
	//
	// int card = mult.cardinality();
	// for (int i = 0; i < 10; i++) {
	// mult = trimMatrix(countSamples, threshold, data, mult);
	// mult = bm.zMult(mult, new SparseDoubleMatrix2D(bm.columns(), bm
	// .columns()));
	// System.out.println(i + 1);
	//
	// mult = mult.forEachNonZero(new IntIntDoubleFunction() {
	// @Override
	// public double apply(int first, int second, double third) {
	// double d = bm.get(first, second);
	// if (third > threshold && d == 0.0) {
	//
	// data.add(data.getColumns()[second],
	// data.getColumns()[first],
	// (double) countSamples / 2);
	// data.add(data.getColumns()[first],
	// data.getColumns()[second],
	// (double) countSamples / 2);
	// bm.set(first, second, 1);
	// try {
	// System.out.print(kb.getURI(Integer.parseInt(data
	// .getColumns()[second])));
	// System.out.println(" "
	// + kb.getURI(Integer.parseInt(data
	// .getColumns()[first])));
	// } catch (Exception e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// return 1.0;
	// }
	// return 0.0;
	// }
	// });
	//
	// if (mult.cardinality() != card) {
	// card = mult.cardinality();
	// System.out.println(card);
	// } else {
	// break;
	// }
	// }

	// final DataSheet data2 = new DataSheet();

	// System.out.println(mult);
	//
	// new File($SCOOBIE_HOME + "results/" + $DATABASE + "/").mkdirs();
	// // data.standardizeGauss();
	// data.serialize(new FileWriter($SCOOBIE_HOME + "results/" + $DATABASE
	// + "/types_cooccurence.csv"), kb);
	// data.covarianceMatrix(new PrintWriter(
	// new FileWriter($SCOOBIE_HOME + "results/" + $DATABASE
	// + "/types_covariance.csv")));
	// data.conditionalProbabiltyMatrix(new PrintWriter(
	// new FileWriter($SCOOBIE_HOME + "results/" + $DATABASE
	// + "/types_cond_prop.csv")), 0.5 * countSamples);
}
