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
		
		ProjectLib.Message mmsg;
		int i, num_src, vote;
		String msg_body, decision = "cancel";

		// initiates a 2PC procedure
		// TODO: add the transaction

		num_src = sources.length;
		String[] srcID = new String[num_src];
		String[] srcName = new String[num_src];
		for (i = 0; i < num_src; i++) {
			// get the node ID and image name from each source
			srcID[i] = sources[i].substring(0, sources[i].indexOf(":"));
			srcName[i] = sources[i].substring(sources[i].indexOf(":") + 1, sources[i].length());
			
			// send prepare messages, containing srcName, image and sources
			mmsg = new MyMessage(srcID[i], "prepare".getBytes(), srcName[i], img, sources);
			System.out.println( "Server: Sending prepare message to " + mmsg.addr );
			PL.sendMessage(mmsg);

		}

		vote = 0;
		// receive votes
		// TODO: this logic won't work, since other types like ack might come at this time
		for (i = 0; i < num_src; i++) {
			// block until vote arrive?
			mmsg = PL.getMessage();
			msg_body = new String(mmsg.body);
			// client's reponse is notok
			if (msg_body.equals("notok")) {
				// TODO: cancel the transaction

				System.out.println( "Server: Got message notok from " + mmsg.addr );
			}
			// client's reponse is ok
			else if (msg_body.equals("ok")) {
				vote += 1;
				System.out.println( "Server: Got message ok from " + mmsg.addr );
			}
			else {
				System.out.println("Error in voting: unexpected message type from " + mmsg.addr + " to Server");
			}
		}

		if (vote == num_src) {
			// apporved
			decision = "done";

			try {
				// save the composite image in the Server directory
				RandomAccessFile writer = new RandomAccessFile("./Server/" + filename, "rw");
				writer.write(img);
				System.out.println( "Server: successfully save the image. " );

			} catch (Exception e) {
				System.out.println( "I/O Error in saving the images. ");
			}
		}

		// inform the decesion to (every?) clients
		for (i = 0; i < num_src; i++) {
			mmsg = new MyMessage(srcID[i], "decision".getBytes(), decision, filename);
			System.out.println( "Server: Sending decision of \"" + decision + "\" to " + mmsg.addr );
			PL.sendMessage(mmsg);
		}

		// receive acks
		// TODO: retry until all sites ack
		for (i = 0; i < num_src; i++) {
			// block until vote arrive?
			mmsg = PL.getMessage();
			msg_body = new String(mmsg.body);
			
			if (msg_body.equals("ask")) {
				System.out.println( "Server: Got ack from " + mmsg.addr );
				continue;
			}
			else {
				System.out.println("Error in ack: unexpected message type from " + mmsg.addr + " to Server");
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

