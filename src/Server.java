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
	// map all existing transaction ID with their status (whether finished)
	private static ConcurrentHashMap<Integer, Boolean> trans_status = new 
							ConcurrentHashMap<Integer, Boolean>();
	// map existing transaction ID with some attributes contained in the message
	private static ConcurrentHashMap<Integer, MyMessage> trans_attri = new 
							ConcurrentHashMap<Integer, MyMessage>();
	// TODO: move these into attri
	// map existing transaction ID with already received votes
	private static ConcurrentHashMap<Integer, Integer> trans_votes = new 
							ConcurrentHashMap<Integer, Integer>();
	// map existing transaction ID with received okvotes
	private static ConcurrentHashMap<Integer, Integer> trans_okvotes = new 
							ConcurrentHashMap<Integer, Integer>();

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
		int i, vote, trans_ID;
		String srcID, srcName, msg_body;

		// initiates a 2PC procedure, add the transaction
		trans_ID = get_ID();
		trans_status.put(trans_ID, false);

		HashMap<String, ArrayList<String>> contributers = new HashMap<>();

		// get the node ID and image name from each source into contributers
		for (i = 0; i < sources.length; i++) {
			srcID = sources[i].substring(0, sources[i].indexOf(":"));
			srcName = sources[i].substring(sources[i].indexOf(":") + 1, sources[i].length());

			ArrayList<String> new_value = contributers.getOrDefault(srcID, new ArrayList<String>());
			new_value.add(srcName);
			contributers.put( srcID, new_value );
		}

		// save attributes to global structure
		MyMessage attri = new MyMessage("Server", "local".getBytes(), trans_ID, filename, img, contributers);
		trans_attri.put(trans_ID, attri);

		// send prepare messages containing srcNames, image and sources
		for (Map.Entry<String, ArrayList<String>> entry : contributers.entrySet()) {
			mmsg = new MyMessage(entry.getKey(), "prepare".getBytes(), trans_ID, entry.getValue(), img, sources);
			System.out.println( "Server: Sending prepare message to " + entry.getKey() );
			PL.sendMessage(mmsg);
		}

		// init global structures to store both number of votes and oks
		trans_votes.put(trans_ID, 0);
		trans_okvotes.put(trans_ID, 0);
	}
	
	/*
	 * handleMessage: Handle a message received by the server.
	 */
	public static synchronized void handleMessage( ProjectLib.Message msg ) {
		String msg_body = new String(msg.body);
		//System.out.println( "Server: A " + msg_body + " message come from " + msg.addr );

		MyMessage mmsg = (MyMessage) msg;
		int trans_ID = mmsg.trans_ID;

		MyMessage attri = trans_attri.get(trans_ID);
		String filename = attri.filename;
		byte[] img = attri.img;
		HashMap<String, ArrayList<String>> contributers = attri.contributers;
		int num_src = contributers.size();

		// if it's a opinion message
		if (msg_body.equals("opinion")) {
			String decision;

			// update votes
			trans_votes.put(trans_ID, trans_votes.get(trans_ID) + 1);
			
			if (mmsg.opinion.equals("ok")) {
				trans_okvotes.put(trans_ID, trans_okvotes.get(trans_ID) + 1);
				System.out.println( "Server: Got message ok from " + msg.addr );
			}
			else if (mmsg.opinion.equals("notok")) {
				System.out.println( "Server: Got message notok from " + msg.addr );
			}
			else {
				System.out.println("Error in voting: unexpected opinion type from " + msg.addr + " to Server");
			}
	
			// TODO: use lock here for commit done operation?
			if (trans_votes.get(trans_ID) == num_src) {
				// apporved
				if (trans_okvotes.get(trans_ID) == num_src){
					decision = "done";
		
					try {
						// save the composite image in the Server directory
						RandomAccessFile writer = new RandomAccessFile(filename, "rw");
						writer.write(img);
						System.out.println( "Server: successfully save the image. " );

					} catch (Exception e) {
						System.out.println( "I/O Error in saving the images. " );
					}

					// mark the transaction as done
					if (trans_status.containsKey(trans_ID)) {
						trans_status.put(trans_ID, true);
					}
					else {
						System.out.println( "Error: cannot find this transaction.");
					}
				}
				// cancelled
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
					System.out.println( "Server: Sending decision of \"" + decision + "\" to " + entry.getKey() );
					PL.sendMessage(mmsg);
				}
			}
		}
		// if it's a ack
		else if (msg_body.equals("ack")) {
			// TODO (ck2): count ack and update some structure
			// TODO (ck2): resend informing until all sites ack
			System.out.println( "Server: Got ack from " + msg.addr );
		}
		else {
			System.out.println("Error in handleMessage: unexpected message type from " + msg.addr + " to Server");
		}
	}

	public static void main ( String args[] ) throws Exception {
		if (args.length != 1) throw new Exception("Need 1 arg: <port>");
		Server srv = new Server();
		PL = new ProjectLib( Integer.parseInt(args[0]), srv );
		
		// main loop
		while (true) {
			ProjectLib.Message msg = PL.getMessage();
			// TODO: do this func in a new thread?
			handleMessage(msg);
		}
	}
}

