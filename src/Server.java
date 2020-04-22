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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

public class Server implements ProjectLib.CommitServing {
	
	private static ProjectLib PL;
	// number (maximum) of generated transaction ID
	private static Integer num_ID = 0;
	// map all existing transaction ID with their status (if finished)
	private static ConcurrentHashMap<Integer, Boolean> trans_status = new 
							ConcurrentHashMap<Integer, Boolean>();
	
	// lock for accessing num_ID
	private static Object ID_lock = new Object();
	
	private static Boolean DEBUG = true; 

	/*
	 * get_ID: generate an sequence of positive integer ID for each transaction.
	 */
	private static int get_ID() {
		synchronized (ID_lock) {
			num_ID += 1;
			return num_ID;
		}
	}

	/*
	 * startCommit: This method is called when a new candidate collage has been
	 * posted, and it should cause a new 2PC commit operation to begin.
	 * @filename: Filename of the collage.
	 * @img: Contents of the collage.
	 * @sources: Source images contributed to the collage, in forms of "Nodeidx:
	 * 			 filename"
	 */
	public synchronized void startCommit( String filename, byte[] img, String[] sources ) {
		System.out.println( "Server: Got request to commit "+filename );
		
		ProjectLib.Message mmsg;
		int i, num_src, vote, trans_ID;
		String srcID, srcName, msg_body, decision;

		// initiates a 2PC procedure, add the transaction
		trans_ID = get_ID();
		trans_status.put(trans_ID, false);

		HashMap<String, ArrayList<String>> contributers = new HashMap<>();

		// get the node ID and image name from each source into contributors
		for (i = 0; i < sources.length; i++) {
			srcID = sources[i].substring(0, sources[i].indexOf(":"));
			srcName = sources[i].substring(sources[i].indexOf(":") + 1, sources[i].length());

			ArrayList<String> new_value = contributers.getOrDefault(srcID, new ArrayList<String>());
			new_value.add(srcName);
			contributers.put( srcID, new_value );
		}

		// send prepare messages containing srcNames, image and sources
		for (Map.Entry<String, ArrayList<String>> entry : contributers.entrySet()) {
			mmsg = new MyMessage(entry.getKey(), "prepare".getBytes(), trans_ID, entry.getValue(), img, sources);
			System.out.println( "Server: Sending prepare message to " + mmsg.addr );
			PL.sendMessage(mmsg);
		}

		vote = 0;
		num_src = contributers.size();
		// receive votes
		// TODO: fix this pure blocking logic (use thread?)
		for (i = 0; i < num_src; i++) {
			// block until vote arrive
			mmsg = PL.getMessage();
			msg_body = new String(mmsg.body);
			// client's reponse is ok
			if (msg_body.equals("ok")) {
				vote += 1;
				System.out.println( "Server: Got message ok from " + mmsg.addr );
			}
			// client's reponse is notok
			else if (msg_body.equals("notok")) {
				System.out.println( "Server: Got message notok from " + mmsg.addr );
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
				RandomAccessFile writer = new RandomAccessFile("../Server/" + filename, "rw");
				writer.write(img);
				System.out.println( "Server: successfully save the image. " );

				// mark the transaction as done
				if (trans_status.containsKey(trans_ID)) {
					trans_status.put(trans_ID, true);
				}
				else {
					System.out.println( "Error: cannot find this transaction.");
				}

			} catch (Exception e) {
				System.out.println( "I/O Error in saving the images. " );
			}
		}
		else {
			decision = "cancel";
			// cancel the transaction
			if (trans_status.containsKey(trans_ID)) {
				trans_status.remove(trans_ID);
			}
			else {
				System.out.println( "Error: cannot find this transaction.");
			}
		}

		// inform the decesion to involved UserNodes
		for (Map.Entry<String, ArrayList<String>> entry : contributers.entrySet()) {
			mmsg = new MyMessage(entry.getKey(), "decision".getBytes(), trans_ID, entry.getValue(), decision, filename);
			System.out.println( "Server: Sending decision of \"" + decision + "\" to " + mmsg.addr );
			PL.sendMessage(mmsg);
		}

		// receive acks
		// TODO (ck2): retry until all sites ack
		for (i = 0; i < num_src; i++) {
			// block until ack arrive
			mmsg = PL.getMessage();
			msg_body = new String(mmsg.body);
			
			if (msg_body.equals("ack")) {
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

