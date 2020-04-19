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
	
	private static ProjectLib PL;
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
		// TODO: add the transaction
		ProjectLib.Message msg;
		int i, num_src, vote;
		String msg_body, decision;

		num_src = sources.length;
		String[] srcID = new String[num_src];
		String[] srcName = new String[num_src];
		for (i = 0; i < num_src; i++) {
			// get the node ID and image name from each source
			srcID[i] = sources[i].substring(0, sources[i].indexOf(":"));
			srcName[i] = sources[i].substring(sources[i].indexOf(":") + 1, sources[i].length());
			
			// send prepare messages, srcName and the image to each involved site
			System.out.println( "Server: Sending message ^" + msg_body + "^ to " + msg.addr );
			msg = new ProjectLib.Message(srcID[i], "prepare".getBytes());
			PL.sendMessage(msg);
			System.out.println( "Server: Sending message ^" + srcName[i] + "^ to " + msg.addr );
			msg = new ProjectLib.Message(srcID[i], srcName[i]);
			PL.sendMessage(msg);
			msg = new ProjectLib.Message(srcID[i], img);
			PL.sendMessage(msg);
		}

		vote = 0;
		// receive votes
		for (i = 0; i < num_src; i++) {
			// block until vote arrive?
			msg = PL.getMessage();
			msg_body = new String(msg.body);
			// client's reponse is notok
			if (msg.equals("notok")) {
				// TODO: cancel the transaction

				decision = "quit";
			}
			// client's reponse is ok
			else if (msg.equals("ok")) {
				vote += 1;
			}
			else {
				System.out.println("Error: unexpected message type");
			}
		}

		if (vote == num_src) {
			// apporved
			decision = "complete";

			// save the composite image in the Server directory
			RandomAccessFile writer = new RandomAccessFile("./Server/" + filename, "rw");
			writer.write(img);
		}

		// inform the decesion to (every?) clients
		for (i = 0; i < num_src; i++) {
			msg = new ProjectLib.Message(srcID[i], decision.getBytes());
			System.out.println( "Server: Sending message ^" + msg_body + "^ to " + msg.addr );
			PL.sendMessage(msg);
		}

		// receive acks
		// TODO: retry until all sites ack
		for (i = 0; i < num_src; i++) {
			// block until vote arrive?
			msg = PL.getMessage();
			msg_body = new String(msg.body);
			
			if (msg.equals("ask")) {
				continue;
			}
			else {
				System.out.println("Error: unexpected message type");
			}
		}
	}
	
	public static void main ( String args[] ) throws Exception {
		if (args.length != 1) throw new Exception("Need 1 arg: <port>");
		Server srv = new Server();
		PL = new ProjectLib( Integer.parseInt(args[0]), srv );
		
		/*// main loop
		while (true) {
			ProjectLib.Message msg = PL.getMessage();
			String msg_body = new String(msg.body);
			System.out.println( "Server: Got message ^" + msg_body + "^ from " + msg.addr );
			System.out.println( "Server: Echoing message to " + msg.addr );
			PL.sendMessage( msg );
		}*/
	}
}

