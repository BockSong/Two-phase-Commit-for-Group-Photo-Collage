/* 
 * 
 * Name: Rong Song
 * Andrew ID: rongsong
 *
 * Server.java - Implementation of coordinator in 2PC protocol.
 * 
 * The server class for two-phase commit for group photo collage.
 * 
 */

import java.io.*;
import java.util.concurrent.*;

public class Server implements ProjectLib.CommitServing {
	
	// map the ...
	private static ConcurrentHashMap<Integer, String> child_role = new 
							ConcurrentHashMap<Integer, String>();
	private static Boolean DEBUG = true; 

	/*
	 * send: 
	 */
	private static void send() {
		;
	}

	/*
	 * startCommit: This method is called when a new candidate collage has been
	 * posted, and it should cause a new 2PC commit operation to begin.
	 * @filename: Filename of the collage.
	 * @img: Contents of the collage.
	 * @sources: Source images contributed to the collage, in forms of "Nodeidx:
	 * 			 filename"
	 */
	public void startCommit( String filename, byte[] img, String[] sources ) {
		System.out.println( "Server: Got request to commit "+filename );
		// initiates a 2PC procedure
		int i, num_src;
		num_src = sources.length;
		String[] srcID = new String[num_src];
		String[] srcName = new String[num_src];
		for (i = 0; i < num_src; i++) {
			// get the node ID and image name from each source
			srcID[i] = sources[i].substring(0, sources[i].indexOf(":"));
			srcName[i] = sources[i].substring(sources[i].indexOf(":") + 1, sources[i].length());
			
			// TODO: send inquiries to each involved site
		}

		// TODO: (after 2PC done) generate composite images in the Server directory
	}
	
	public static void main ( String args[] ) throws Exception {
		if (args.length != 1) throw new Exception("Need 1 arg: <port>");
		Server srv = new Server();
		ProjectLib PL = new ProjectLib( Integer.parseInt(args[0]), srv );
		
		// main loop
		while (true) {
			ProjectLib.Message msg = PL.getMessage();
			System.out.println( "Server: Got message from " + msg.addr );
			System.out.println( "Server: Echoing message to " + msg.addr );
			PL.sendMessage( msg );
		}
	}
}

