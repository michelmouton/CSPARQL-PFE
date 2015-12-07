package org.rdfhdt.hdt.tools;

import org.apache.jena.util.FileManager;
import org.rdfhdt.hdt.util.StopWatch;

import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.rdf.RDFParserCallback;
import org.rdfhdt.hdt.rdf.RDFParserCallback.RDFCallback;
import org.rdfhdt.hdt.rdf.RDFParserFactory;
import org.rdfhdt.hdt.triples.TripleString;
import java.io.*;
import java.math.BigInteger;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


public class Loading implements Runnable {

	private static String inputFileName = "data_29mini.ttl";
	public PipedWriter sortie;
	private boolean keepRunning = false;
	
	public Loading(PipedWriter sortie) {
		super();
		this.sortie = sortie;
	}
	
	public void pleaseStop() {
		keepRunning = false;
	}
	
	
	@Override
	public void run() {
		keepRunning = true;
		
		StopWatch sw = new StopWatch(); // start timer
		InputStream in = FileManager.get().open(inputFileName);
		RDFNotation notation = null;
		String baseURI = null;

		if (in == null) {
			throw new IllegalArgumentException("File: " + inputFileName + " not found");
		}
		if (baseURI == null) {
			baseURI = "file://" + inputFileName;
		}
		if (notation == null) // guess notation by the filename
			try {
				notation = RDFNotation.guess(inputFileName);
			} catch (IllegalArgumentException e) {
				System.out.println("Could not guess notation for  Trying NTriples");
				notation = RDFNotation.TURTLE;
			}

		Loading ld = new Loading(sortie);
		try {
			ld.load(in, baseURI, notation);
		} catch (ParserException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		InputStream in2 = FileManager.get().open(inputFileName);
		System.out.println("- Conversion Time: " + sw.stopAndShow());
		sw = new StopWatch();
		System.out.println("Writing begins");
		try {
			ld.loadshed(in2, baseURI, notation);
		} catch (ParserException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("done " + sw.stopAndShow());
		//System.exit(0);
		try {
			Thread.sleep(200000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	
	
	
	BigInteger GraphId = new BigInteger("0");
	Map<BigInteger, Integer> GraphTable = new HashMap<BigInteger, Integer>(); // GHT
	Map<String, Integer> PredicatTable = new HashMap<String, Integer>(); // PHT
	int NbreGraph = 1, Min = 100, Max = 0, Tmp = 0;
	String subject = "", predicat = "", predicatName = "", SubjectName = "", ObjectName = "", resultat = "";
	Integer PredicatIndice = -1;
	BitSet bits1 = new BitSet(); //requete
	
	public class Logger {
		private File logFile;
		public Logger() {
		}
		public Logger(String fileName) {
			logFile = new File(fileName);
		}
		public Logger(File f) {
			logFile = f;
		}
		public void log(String s) {
			try {
				FileWriter fw = new FileWriter(this.logFile,true);
				fw.write(s);
				fw.write(System.lineSeparator());
				fw.close();
			} catch (IOException ex) {
				System.err.println("Couldn't log this: "+s);
			}
		}
	}

	
	//callback pour premiere lecture : PHT, GHT
	class Callback implements RDFCallback {
		
		private long numTriples = 0;
		private TripleString previous = null;
		private TripleString next;
		private String previousName;
		


		// methode qui traite les triplets
		@Override
		public void processTriple(TripleString triple, long pos) {
			numTriples++;
			if (numTriples == 1) {
				previous = triple;
				previousName = previous.getSubject().toString();
			}
			next = triple;
			// System.out.println(" prev : " + previousName);
			// System.out.println(" actu : " + next.getSubject().toString());
			predicatName = previous.getPredicate().toString();
			predicat = predicatName;
			
			String subjectToSend = next.getSubject().toString();
			String predicateToSend = next.getPredicate().toString();
			String objectToSend = next.getObject().toString();
			
			String tripleToSend = subjectToSend + "§" + predicateToSend + "§" + objectToSend + "§$";
			
			try {
				//System.out.println(numTriples + " Message envoye(tripleToSend) : " + tripleToSend);
				sortie.write(tripleToSend);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			

			if (!PredicatTable.containsKey(predicat)) { // Si nouveau pr�dicat
				PredicatIndice++;
				PredicatTable.put(predicat, PredicatIndice); // l'inserer dans
																// la hashtable
				GraphId = GraphId.setBit(PredicatIndice); // Marquer � 1 le bit
															// correspondant
															// dans l'ID du
															// graphe
			} else {
				GraphId = GraphId.setBit(PredicatTable.get(predicat));
			}
			// System.out.println(" prev dernier : " +
			// previous.getSubject().toString());
			if (!(previousName.equalsIgnoreCase(next.getSubject().toString()))) {
				NbreGraph++;
				if ((!GraphTable.containsKey(GraphId)) && (GraphId.bitCount() != 0)) { // Nouveau
																						// graph
																						// detect�
					GraphTable.put(GraphId, 1);
					GraphId = BigInteger.ZERO;
				} else {
					GraphTable.put(GraphId, (GraphTable.get(GraphId) + 1));
					Tmp = GraphTable.get(GraphId) + 1;
					GraphId = BigInteger.ZERO;
				}
			}
			previous = next;
			previousName = previous.getSubject().toString();
		}

		public long getNumTriples() {
			return numTriples;
		}
	}

	class Callback_shed implements RDFCallback {
		private String predicatName;
		@Override
		public void processTriple(TripleString triple, long pos) {
			predicatName = triple.getPredicate().toString();
			if (PredicatTable.containsKey(predicatName)) {
				Integer numberPred = PredicatTable.get(predicatName);
				boolean queryvalue = bits1.get(numberPred);
				if (queryvalue) {
					//System.out.println(triple.getSubject().toString() + triple.getPredicate().toString()
							//+ triple.getObject().toString());
					
					Logger log1 = new Logger("output_aemet_2.nt");
					boolean http = triple.getObject().toString().startsWith("http");
					//if (http)
					//log1.log("<"+triple.getSubject().toString()+"> "+"<"+triple.getPredicate().toString()+"> "+"<"+triple.getObject().toString()+"> .");
					//else log1.log("<"+triple.getSubject().toString()+"> "+"<"+triple.getPredicate().toString()+"> "+triple.getObject().toString()+" .");
				}
			}
		}
	}

	public void loadshed(InputStream in, String baseURI, RDFNotation notation) throws ParserException {
		Callback_shed call = new Callback_shed();
		RDFParserCallback parser = RDFParserFactory.getParserCallback(notation);
		parser.doParse(in, baseURI, notation, call);

	}

	// methode qui initialise le callback et fais la lecture des donnees
	public void load(InputStream in, String baseURI, RDFNotation notation) throws ParserException {
		Callback callback = new Callback();
		RDFParserCallback parser = RDFParserFactory.getParserCallback(notation);
		// RDFParserSimple parser = new RDFParserSimple();
		parser.doParse(in, baseURI, notation, callback);
		System.out.println("reading done and " + callback.getNumTriples());
		System.out.println(" Nombre de Graphes traites      : " + NbreGraph);
		System.out.println(" Nombre de Graphes   (patterns) : " + GraphTable.size());
		System.out.println(" Nombre de Predicats (patterns) : " + PredicatTable.size());
		Iterator it = PredicatTable.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry pair = (Map.Entry)it.next();
	        System.out.println(pair.getKey() + " = " + pair.getValue());
	    }
		//System.out.println(" Liste des Predicats  : " + PredicatTable.keySet());
		//System.out.println("Liste des Graphes  : " + GraphTable.keySet());
		//bits1.set(53);
		//bits1.set(6);
		bits1.set(3);
		bits1.set(2);
		bits1.set(0);
	}

	/*public static void main(String[] args) throws ParserException {
		StopWatch sw = new StopWatch(); // start timer
		InputStream in = FileManager.get().open(inputFileName);
		RDFNotation notation = null;
		String baseURI = null;
		


		if (in == null) {
			throw new IllegalArgumentException("File: " + inputFileName + " not found");
		}
		if (baseURI == null) {
			baseURI = "file://" + inputFileName;
		}
		if (notation == null) // guess notation by the filename
			try {
				notation = RDFNotation.guess(inputFileName);
			} catch (IllegalArgumentException e) {
				System.out.println("Could not guess notation for  Trying NTriples");
				notation = RDFNotation.TURTLE;
			}

		Loading ld = new Loading();
		ld.load(in, baseURI, notation);
		InputStream in2 = FileManager.get().open(inputFileName);
		System.out.println("- Conversion Time: " + sw.stopAndShow());
		sw = new StopWatch();
		System.out.println("Writing begins");
		ld.loadshed(in2, baseURI, notation);
		System.out.println("done " + sw.stopAndShow());
		System.exit(0);
	}*/
}
