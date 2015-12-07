package eu.larkc.csparql.readytogopack.streamer;

import java.io.IOException;
import java.io.PipedWriter;



public class MyGenerator implements Runnable {
	
	PipedWriter sortie;
	private boolean keepRunning = false;

	public MyGenerator(PipedWriter sortie) {
		super();
		this.sortie = sortie;
	}

	public void pleaseStop() {
		keepRunning = false;
	}

	@Override
	public void run() {
		keepRunning = true;
		
		while (keepRunning) {
			
			String msgToSend = "du texte$";
			String[] messagesToSend = new String[3];
			messagesToSend[0] = "guten tag";
			messagesToSend[1] = "ola";
			messagesToSend[2] = "hello";
			int choix = (int)(Math.random()*3);
			msgToSend = messagesToSend[choix] + "$";
            try {
				sortie.write(msgToSend);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
            System.out.println("Message envoye '" + msgToSend + "'");

			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			

		}
	}


	public static void main(String[] args) {
	}
}
