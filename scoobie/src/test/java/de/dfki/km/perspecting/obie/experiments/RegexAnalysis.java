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

import gnu.trove.TIntDoubleHashMap;

import java.net.URI;

import org.junit.BeforeClass;
import org.junit.Test;
import org.postgresql.jdbc2.optional.PoolingDataSource;

import de.dfki.km.perspecting.obie.connection.KnowledgeBase;
import de.dfki.km.perspecting.obie.connection.PostgresKB;
import de.dfki.km.perspecting.obie.workflow.Pipeline;

public class RegexAnalysis {

	/**
	 * 
	 */

	private static PoolingDataSource pool = new PoolingDataSource();

	private static Pipeline pipeline;

	/******************* technical setup ******************************************/

	private static String $PHD_HOME = "/home/adrian/Dokumente/diss/";
	private static String $SCOOBIE_HOME = $PHD_HOME + "scoobie/";
	private static String $CORPUS_HOME = $PHD_HOME + "textcorpus/";

	private static String $DATABASE = "dbpedia_en2";

	private static String $DATABASE_SERVER = "pc-4327.kl.dfki.de";
	private static String $DATABASE_SERVER_USER = "postgres";
	private static String $DATABASE_SERVER_PW = "scoobie";
	private static int $DATABASE_SERVER_PORT = 5432;

	private static KnowledgeBase kb;

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {

		pool.setUser($DATABASE_SERVER_USER);
		pool.setPassword($DATABASE_SERVER_PW);
		pool.setPortNumber($DATABASE_SERVER_PORT);
		pool.setDatabaseName($DATABASE);
		pool.setServerName($DATABASE_SERVER);
		pool.setMaxConnections(100);

		kb = new PostgresKB(pool.getConnection(), $DATABASE, new URI("http://test.de"));
		pipeline = new Pipeline(kb);
	}

	@Test
	public void testRegex() throws Exception {

		String DATE = "(19|20)\\\\d{2}-(0[1-9]|1[012]|[1-9])-(0[1-9]|[1-9]|[12][0-9]|3[01])";
		String MAIL = "[\\\\w]+@[\\\\w]+";
		String ISBN10 = "ISBN\\\\x20(?=.{13}$)\\\\d{1,5}([- ])\\\\d{1,7}\\\\1\\\\d{1,6}\\\\1(\\\\d|X)$";
		String FLOAT = "[-]?[0-9]+\\\\.[0-9]+";
		String POINT = "[-]?[0-9]+\\\\.[0-9]+ [-]?[0-9]+\\\\.[0-9]+";
		String[] patterns = new String[] { DATE, FLOAT, POINT };

//		System.out.println(Pattern.compile(POINT).matcher(
//				"31.8182 34.7541").find());
//		System.out.println( kb.getURI(13390079) );
//		System.out.println(Pattern.compile(MAIL).matcher(
//				"My mail is benjamin.adria@dfki.uni-kl.de.").find());
//		System.out.println(Pattern.compile(ISBN10).matcher(
//				"The isbn is ISBN 1-56389-668-0").find());

//
		kb.calculateRegexDistributions(patterns);
	//	 regexModel.train(); 
		 
		 for(String regex : patterns) {
			 TIntDoubleHashMap set = kb.getDatatypePropertiesForRegex(regex);
			 System.out.println(regex);
			 for(int p : set.keys()) {
				 System.out.println(kb.getURI(p) +" " + set.get(p));
			 }
			 System.out.println();
			 
		 }
	}

}
