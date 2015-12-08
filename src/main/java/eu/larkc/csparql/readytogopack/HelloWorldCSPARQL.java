/*******************************************************************************
 * Copyright 2014 Davide Barbieri, Emanuele Della Valle, Marco Balduini
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * Acknowledgements:
 * 
 * This work was partially supported by the European project LarKC (FP7-215535) 
 * and by the European project MODAClouds (FP7-318484)
 ******************************************************************************/
package eu.larkc.csparql.readytogopack;

import java.io.IOException;
import java.io.PipedReader;
import java.io.PipedWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;

import org.apache.log4j.PropertyConfigurator;
import org.rdfhdt.hdt.tools.Loading;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.larkc.csparql.cep.api.RdfStream;
import eu.larkc.csparql.core.engine.ConsoleFormatter;
import eu.larkc.csparql.core.engine.CsparqlEngine;
import eu.larkc.csparql.core.engine.CsparqlEngineImpl;
import eu.larkc.csparql.core.engine.CsparqlQueryResultProxy;
import eu.larkc.csparql.core.engine.RDFStreamFormatter;
import eu.larkc.csparql.readytogopack.streamer.BasicIntegerRDFStreamTestGenerator;
import eu.larkc.csparql.readytogopack.streamer.BasicRDFStreamTestGenerator;
import eu.larkc.csparql.readytogopack.streamer.CloudMonitoringRDFStreamTestGenerator;
import eu.larkc.csparql.readytogopack.streamer.DoorsTestStreamGenerator;
import eu.larkc.csparql.readytogopack.streamer.LBSMARDFStreamTestGenerator;
import eu.larkc.csparql.readytogopack.streamer.Lecteur;
import eu.larkc.csparql.readytogopack.streamer.MyGenerator;

public class HelloWorldCSPARQL {

	private static Logger logger = LoggerFactory.getLogger(HelloWorldCSPARQL.class);

	public static void main(String[] args) throws IOException {

		try {
			PropertyConfigurator.configure(new URL("http://streamreasoning.org/configuration_files/csparql_readyToGoPack_log4j.properties"));
		} catch (MalformedURLException e) {
			logger.error(e.getMessage(), e);
		}

		// examples of streams and queries

		final int MY_QUERY = 0;
		// put here the example you want to run

		int key = MY_QUERY;

		// initializations

		//		String streamURI = "http://myexample.org/stream";
		String query = null;
		String queryDownStream = null;
		RdfStream tg = null;
		RdfStream anotherTg = null;

		// Initialize C-SPARQL Engine
		CsparqlEngine engine = new CsparqlEngineImpl();
		
		/*
		 * Choose one of the the following initialize methods: 
		 * 1 - initialize() - Inactive timestamp function - Inactive injecter 
		 * 2 - initialize(int* queueDimension) - Inactive timestamp function -
		 *     Active injecter with the specified queue dimension (if 
		 *     queueDimension = 0 the injecter will be inactive) 
		 * 3 - initialize(boolean performTimestampFunction) - if
		 *     performTimestampFunction = true, the timestamp function will be
		 *     activated - Inactive injecter 
		 * 4 - initialize(int queueDimension, boolean performTimestampFunction) - 
		 *     if performTimestampFunction = true, the timestamp function will
		 *     be activated - Active injecter with the specified queue dimension
		 *     (if queueDimension = 0 the injecter will be inactive)
		 */
		engine.initialize(true);

		switch (key) {
		case MY_QUERY:

			logger.debug("My_QUERY example");

			/*query = "REGISTER QUERY MyQuery AS "
					//+ "PREFIX ex: <http://myexample.org/> "
					+ "PREFIX om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#>"
					+ "PREFIX weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#>"
					+ "SELECT ?sensor ?tempvalue ?obs "
					+ "FROM STREAM <http://myexample.org/stream> [RANGE 5s STEP 1s] "
					+ "WHERE { ?sensor ?tempvalue ?obs "
					+ "?obs om-owl:observedProperty weather:_RelativeHumidity . }";
					//+ "?obs om-owl:observedProperty weather:_AirTemperature ; }"; 
				    //+   "om-owl:procedure ?sensor ;"  
				    //+  "om-owl:result [om-owl:floatValue ?tempvalue] .}";*/
			
			query = "REGISTER QUERY MyQuery AS "
					+ "PREFIX om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#>"
					+ "PREFIX weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#>"
					+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
					+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
					+ "SELECT DISTINCT ?sensor ?value ?uom "
					+ "FROM STREAM <http://myexample.org/stream> [RANGE 1s STEP 1s] "
					+ "WHERE { ?sensor ?value ?uom }";
			
			
			break;
		

		default:
			System.exit(0);
			break;
		}

		// Register an RDF Stream

		

		// Start Streaming (this is only needed for the example, normally streams are external
		// C-SPARQL Engine users are supposed to write their own adapters to create RDF streams


		PipedWriter pipedWriter = new PipedWriter();
		PipedReader pipedReader = new PipedReader(pipedWriter);
		
		tg = new LBSMARDFStreamTestGenerator("http://myexample.org/stream", pipedReader);
		engine.registerStream(tg);
		
		Loading myLoading = new Loading(pipedWriter);

		final Thread threadGenerator = new Thread((Runnable) myLoading);
		final Thread threadLecteur = new Thread((Runnable) tg);
		
		
		threadGenerator.start();
		threadLecteur.start();


		// Register a C-SPARQL query

		CsparqlQueryResultProxy c1 = null;
		CsparqlQueryResultProxy c2 = null;

		if (key != 9) {

			try {
				c1 = engine.registerQuery(query, false);
				logger.debug("Query: {}", query);
				logger.debug("Query Start Time : {}", System.currentTimeMillis());
			} catch (final ParseException ex) {
				logger.error(ex.getMessage(), ex);
			}

			// Attach a Result Formatter to the query result proxy

			if (c1 != null) {
				c1.addObserver(new ConsoleFormatter());
			}

		} else {
			try {
				c1 = engine.registerQuery(query, false);
				logger.debug("Query: {}", query);
				logger.debug("Query Start Time : {}", System.currentTimeMillis());
			} catch (final ParseException ex) {
				logger.error(ex.getMessage(), ex);
			}

			// Attach a Result Formatter to the query result proxy

			if (c1 != null) {
				c1.addObserver((RDFStreamFormatter) anotherTg);

				try {
					c2 = engine.registerQuery(queryDownStream, false);
					logger.debug("Query: {}", query);
					logger.debug("Query Start Time : {}", System.currentTimeMillis());
				} catch (final ParseException ex) {
					logger.error(ex.getMessage(), ex);
				}

				if (c2 != null) {
					c2.addObserver(new ConsoleFormatter());

				}

			}
		}

		// leave the system running for a while
		// normally the C-SPARQL Engine should be left running
		// the following code shows how to stop the C-SPARQL Engine gracefully
		try {
			Thread.sleep(200000);
		} catch (InterruptedException e) {
			logger.error(e.getMessage(), e);
		}

		if (key != 9) {
			// clean up (i.e., unregister query and stream)
			engine.unregisterQuery(c1.getId());

			((LBSMARDFStreamTestGenerator) tg).pleaseStop();

			engine.unregisterStream(tg.getIRI());

			if (anotherTg != null) {
				engine.unregisterStream(anotherTg.getIRI());
			}
		} else {
			// clean up (i.e., unregister query and stream) 
			engine.unregisterQuery(c1.getId());
			engine.unregisterQuery(c2.getId());

			((LBSMARDFStreamTestGenerator) tg).pleaseStop();

			engine.unregisterStream(tg.getIRI());
			engine.unregisterStream(anotherTg.getIRI());
		}

		System.exit(0);



	}

}
