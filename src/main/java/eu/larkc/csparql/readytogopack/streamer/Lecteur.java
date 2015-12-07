package eu.larkc.csparql.readytogopack.streamer;


import java.io.IOException;
import java.io.PipedReader;

public class Lecteur implements Runnable {
	PipedReader entree;
	
	public Lecteur(PipedReader entree) {
	      super();
	      this.entree = entree;
	}
	
	@Override
	public void run()
	{
		try 
		{
	         int i = entree.read();

	         while(true)
	         {
	        	System.out.print("Message recu : ");
	            while(i != '$')
	            {
	            	System.out.print((char)i);
	            	i = entree.read();
	            }
	            System.out.println("");
	            i = entree.read();
	            
	            try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	         }
		}
		catch (IOException e) 
		{
			e.printStackTrace();
	    }
	}
	
	public static void main(String[] args) {
	}
	
}
