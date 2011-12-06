package de.dfki.km.perspecting.obie.corpus;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.search.IndexSearcher;
import org.apache.nutch.analysis.lang.NGramProfile;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import de.dfki.km.perspecting.obie.model.DocumentProcedure;
import de.dfki.km.perspecting.obie.vocabulary.Language;
import de.dfki.km.perspecting.obie.vocabulary.MediaType;

public class TestBBCNatureCorpus {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void test() {

		try {
			BBCNatureCorpus corpus = new BBCNatureCorpus(
					new File(
							"../corpora/bbc_nature/bbc_nature_labels.zip"),
					new TextCorpus(
							new File(
									"../corpora/bbc_nature/bbc_nature_text.zip"),
							MediaType.ZIP, MediaType.HTML, Language.EN));

			@SuppressWarnings({ "unchecked" })
			List<URI> list = (List<URI>) corpus
					.forEach(new DocumentProcedure<URI>() {
						@Override
						public URI process(Reader doc, URI uri)
								throws Exception {
							return uri;
						}
					});

			for (URI e : list) {
//				System.out.println(e);

					BufferedReader reader = new BufferedReader(
							corpus.getGroundTruth(e));
					for (String line = reader.readLine(); line != null; line = reader
							.readLine()) {
//					System.out.println(line);
					}

					reader.close();
			}
			
			
//			File lucene = new File(System.getProperty("java.io.tmpdir")+"/lucene");
//			IndexSearcher searcher = corpus.getLuceneIndex(lucene, true);
//			searcher.close();
//			
//			FileUtils.deleteDirectory(lucene);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
