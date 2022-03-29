# SCOOBIE

This is a project to provide Semantic Web programmers with [Information Extraction](http://gate.ac.uk/ie/) (IE) functionalities. SCOOBIE can be initialised with any kind of RDF graph. It interprets the occurrence of URI references being described with RDF properties as descriptions of formal instances. On the basis of an RDF graph with contained instances, SCOOBIE offers following methods:

* Recognition of literal property values in text.
* Recognition of instances in text with matching literal values.
* Disambiguation of instances sharing similar literal values according to link analyses on the underlying RDF graph
* Rating the relevance of recognised instances according to link and corpus analyses.
* Prediction of yet unknown links between recognised URI references.

## Sub-projects

* SCOOBIE contains the Java software, which implements these IE methods.
* Corpora contains document corpora used for either evaluating the functionalities of SCOOBIE or for creating statistics on words in text.

## Who is behind this?

Initially, SCOOBIE is based on the result of the PHD work by Benjamin Adrian at [DFKI](http://www.dfki.de). Fortunately, DFKI allowed the Open Source publication of SCOOBIE. Currently, Benjamin will proceed developing SCOOBIE. If *you* are interested in contributing to this project with code, discussion, use cases, ideas, ..., please join!

## Who uses SCOOBIE?

During Ben's PHD work, the work on SCOOBIE was applied in or financed by several research projects:

* [DocuTag] German research funding, Stiftung Rheinland-Pfalz für Innovation
* [iDocument] German research funding, Stiftung Rheinland-Pfalz für Innovation
* [Nepomuk] EU FP6 research funding, Grant FP6-027705
* [Perspecting]  German research funding, BMBF, Grant 01IW08002
* [REMIX] German research funding, Zentrales Innovationsprogramm Mittelstand (ZIM)
* [SemoPad] German research funding, Stiftung Rheinland-Pfalz für Innovation

## License

The software and artefacts (such as examples, mappings, etc.) provided through the SCOOBIE project are, if not otherwise stated, published under the conditions of the LGPL. This does not comprise the content of the corpora. These are copyrighted and only published on this web page for being for scientific purposes.

## Roadmap and Ideas

* Make SCOOBIE simple and stable.
 * Cover most of the code with JUnit tests.
 * Create JavaDocs describing classes and methods.


## Similar projects

If you are interested in the functionalities provided by SCOOBIE, you should also have a look at:

* [DBpedia Spotlight](https://www.dbpedia-spotlight.org/)
* [Zemanta](http://www.zemanta.com/)
* [GATE](http://gate.ac.uk)
* [Natural Language Toolkit](http://www.nltk.org/)