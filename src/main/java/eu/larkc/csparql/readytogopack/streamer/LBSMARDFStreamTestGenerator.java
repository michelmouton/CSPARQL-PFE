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
package eu.larkc.csparql.readytogopack.streamer;

import java.io.IOException;
import java.io.PipedReader;
import java.io.StringWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.impl.PropertyImpl;
import com.hp.hpl.jena.rdf.model.impl.ResourceImpl;

import eu.larkc.csparql.cep.api.RdfQuadruple;
import eu.larkc.csparql.cep.api.RdfStream;

public class LBSMARDFStreamTestGenerator extends RdfStream implements Runnable {

	/** The logger. */
	protected final Logger logger = LoggerFactory
			.getLogger(LBSMARDFStreamTestGenerator.class);	

	private int c = 1;
	private int ct = 1;
	private boolean keepRunning = false;

	PipedReader entree;

	public LBSMARDFStreamTestGenerator(final String iri, PipedReader entree) {
		super(iri);
		this.entree = entree;
	}

	public void pleaseStop() {
		keepRunning = false;
	}

	@Override
	public void run() {
		try {
			int ie = 0;
			String subject = "not initialized";
			String predicate = "not initialized";
			String object = "not initialized";

			while(true)
			{
				ie = entree.read();
				
				while(ie != '$')
				{
					subject = "";
					while(ie != '§')
					{
						//System.out.println("suj : " + subject);
						subject += String.valueOf((char)ie);
						ie = entree.read();
					}
					ie = entree.read();

					predicate = "";
					while(ie != '§')
					{
						//System.out.println("pred : " + predicate);
						predicate += String.valueOf((char)ie);
						ie = entree.read();
					}
					ie = entree.read();

					object = "";
					while(ie != '§')
					{
						//System.out.println("obj : " + object);
						object += String.valueOf((char)ie);
						ie = entree.read();
					}
					ie = entree.read();
					
					//System.out.println("Message recu(triplet) : " + subject + "/" + predicate + "/" + object + "(fin predicat)");
					final RdfQuadruple q = new RdfQuadruple(subject,
							predicate, object, System.currentTimeMillis());
					this.put(q);
					//          logger.info(q.toString());
					System.out.println("ct : " + this.ct);
					ct++;
					
					
					try {
						Thread.sleep(10);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				ie = entree.read();

				//System.out.println(ct+ " triples streamed so far");

				System.out.println("c : " + this.ct);
				this.c++;
				

			}

		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}



	}


	public static String dumpRelatedStaticKnowledge(int maxUser) {

		Model m = ModelFactory.createDefaultModel(); 
		for (int j=0;j<maxUser;j++) {
			for (int i=0;i<5;i++) {      
				m.add(new ResourceImpl("http://myexample.org/user" + j+i), new PropertyImpl("http://myexample.org/follows"), new ResourceImpl("http://myexample.org/user" + j));
			}
		}
		StringWriter sw = new StringWriter(); 
		m.write(sw, "RDF/XML");
		return sw.toString();
	}

	public static void main(String[] args) {
		//System.out.println(dumpRelatedStaticKnowledge(10));
	}
}
