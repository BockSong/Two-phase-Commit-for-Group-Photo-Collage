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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

public class Server implements ProjectLib.CommitServing {
	
	private static ProjectLib PL;
	// threshold of timeout
	private static int TIMEOUT_TH = 6000;
	// number (maximum) of generated transaction ID
	private static int num_ID = 0;
	// status of transaction
	private static enum Tstatus {
		startPhase1, startPhase2, transDone;
	}
	// decisions in phase 2 in 2PC
	private static enum Decisions {
		undecided, done, cancel;
	}

	// map transaction ID with status
	private static ConcurrentHashMap<Integer, String> trans_status = new 
							ConcurrentHashMap<Integer, String>();
	// map existing transaction ID with some attributes contained in the message
	private static ConcurrentHashMap<Integer, MyMessage> trans_attri = new 
							ConcurrentHashMap<Integer, MyMessage>();
	// store the number received votes corresponding to transaction ID
	private static ConcurrentHashMap<Integer, Integer> trans_votes = new 
							ConcurrentHashMap<Integer, Integer>();
	// store the number ok votes corresponding to transaction ID
	private static ConcurrentHashMap<Integer, Integer> trans_okvotes = new 
							ConcurrentHashMap<Integer, Integer>();
	// store the list of UserNodes who haven't reply an ack, corresponding to transaction ID
	private static ConcurrentHashMap<Integer, ArrayList<String>> trans_unrevACK = new 
							ConcurrentHashMap<Integer, ArrayList<String>>();

	// lock for accessing num_ID
	private static Object ID_lock = new Object();
	// lock for accessing votes
	private static Object vote_lock = new Object();

	private static String log_name = "server.log";
	private static Boolean DEBUG = false; 

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
	 * write_log_status: write the status information to log before phase 2.
	 */
	private static void write_log_status(int trans_ID, Tstatus status, String decision) {
		try {
			// write log
			BufferedOutputStream writer = new
			BufferedOutputStream(new FileOutputStream(log_name, true));
			
			// format: <transaction_ID: transaction_status\n>
			String log_line = Integer.toString(trans_ID) + ":" + status.toString() + "\n";
			writer.write(log_line.getBytes());

			// write decision
			writer.write((decision + "\n").getBytes());

			writer.flush();
			writer.close();
			PL.fsync();
			
		} catch (Exception e) {
			System.err.println( "Server: Error " + e.getMessage());
			e.printStackTrace();
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
		if (DEBUG)  System.out.println( "Server: Got request to commit " + filename );
		
		ProjectLib.Message mmsg;
		int i, vote, trans_ID;
		String srcID, srcName, msg_body;
		HashMap<String, ArrayList<String>> contributers = new HashMap<>();

		trans_ID = get_ID();

		// get the node ID and image name from each source into contributers
		for (i = 0; i < sources.length; i++) {
			srcID = sources[i].substring(0, sources[i].indexOf(":"));
			srcName = sources[i].substring(sources[i].indexOf(":") + 1, sources[i].length());

			ArrayList<String> new_value = contributers.getOrDefault(srcID, 
																	new ArrayList<String>());
			new_value.add(srcName);
			contributers.put( srcID, new_value );
		}

		// save attributes to global structure
		MyMessage attri = new MyMessage("Server", "local".getBytes(), trans_ID, filename, 
																	img, contributers);
		trans_attri.put(trans_ID, attri);

		// initiates a 2PC transaction
		trans_status.put(trans_ID, Tstatus.startPhase1.toString());

		try {
			// write log before phase 1
			BufferedOutputStream writer = new
			BufferedOutputStream(new FileOutputStream(log_name, true));
			
			// format: <transaction_ID: transaction_status\n>
			String log_line = Integer.toString(trans_ID) + ":" + Tstatus.startPhase1.toString()
																						 + "\n";
			writer.write(log_line.getBytes());

			// write filename
			writer.write((filename + "\n").getBytes());

			// write contributors by line
			// Format: <trans_ID: filename\n>, <source_ID1@source_name1, source_name2, ...
			// 			source_ID2@source_name1, source_name2, ...\n> 
			// also explained in the design doc
			log_line = "";
			for (Map.Entry<String, ArrayList<String>> entry : contributers.entrySet()) {
				log_line += entry.getKey() + "@";
				for (i = 0; i < entry.getValue().size(); i++) {
					log_line += entry.getValue().get(i);
					if (i != entry.getValue().size() - 1) {
						log_line += ",";
					}
				}
				log_line += " ";
			}
			log_line += "\n";
	
			writer.write(log_line.getBytes());

			writer.flush();
			writer.close();
			PL.fsync();
			
		} catch (Exception e) {
			System.err.println( "Server: Error " + e.getMessage());
			e.printStackTrace();
		}

		// send prepare messages containing srcNames, image and sources
		for (Map.Entry<String, ArrayList<String>> entry : contributers.entrySet()) {
			mmsg = new MyMessage(entry.getKey(), "prepare".getBytes(), trans_ID, 
														entry.getValue(), img, sources);
			if (DEBUG)  System.out.println( "Server: Sending prepare message to " + entry.getKey() );
			PL.sendMessage(mmsg);
		}

		// init global structures to store both number of votes and ok-vote
		trans_votes.put(trans_ID, 0);
		trans_okvotes.put(trans_ID, 0);

		// start a new thread processPhase1
		processPhase1(trans_ID);
	}
	
	/*
	 * processPhase1: Process the first phase. If it ends or timeout, start next phase.
	 */
	public static void processPhase1( int trans_ID ) {
		new Thread(new Runnable(){
		
			@Override
			public void run() {
				MyMessage attri = trans_attri.get(trans_ID);
				String filename = attri.filename;
				byte[] img = attri.img;
				HashMap<String, ArrayList<String>> contributers = attri.contributers;
				int num_src = contributers.size();

				// init timestamp for phase 1
				long startTime = System.currentTimeMillis();
				Decisions decision = Decisions.undecided;	

				// keeping check if timeout or receive all
				while (decision == Decisions.undecided) {
					// use lock to avoid race condition in accessing votes
					synchronized (vote_lock) {
						if (trans_votes.get(trans_ID) == num_src) {
							// apporved
							if (trans_okvotes.get(trans_ID) == num_src){
								decision = Decisions.done;
							}
							// cancelled
							else {
								decision = Decisions.cancel;
							}
						}
						// timeout
						else if (System.currentTimeMillis() - startTime > TIMEOUT_TH) {
							decision = Decisions.cancel; // this is an abort for timeout
						}
					}
					if (decision == Decisions.done) {
						try {
							// save the composite image in the Server directory
							RandomAccessFile writer = new RandomAccessFile(filename, "rw");
							writer.write(img);
							if (DEBUG)  
								System.out.println( "Server: successfully save the image. " );

						} catch (Exception e) {
							System.err.println( "Server: Error in saving the images." );
						}
					}
				}

				// save the decision
				attri.decision = decision.toString();
				trans_attri.put(trans_ID, attri);

				// mark the transaction as phase 2
				if (trans_status.containsKey(trans_ID)) {
					trans_status.put(trans_ID, Tstatus.startPhase2.toString());
				}
				else {
					System.err.println( "Server: Error, cannot find this transaction.");
				}
				
				// write log before phase 2
				write_log_status(trans_ID, Tstatus.startPhase2, decision.toString());

				// start the second thread
				processPhase2(trans_ID);
			}
		}).start();
	}

	/*
	 * processPhase2: Process the second phase and keep checking that if 2PC ends.
	 */
	public static void processPhase2( int trans_ID ) {
		new Thread(new Runnable(){
		
			@Override
			public void run() {
				MyMessage attri = trans_attri.get(trans_ID);
				String decision = attri.decision;
				String filename = attri.filename;
				HashMap<String, ArrayList<String>> contributers = attri.contributers;
				int num_src = contributers.size();
				
				// mark all acks as unreceived
				ArrayList<String> userList = new ArrayList<String>();
				for (Map.Entry<String, ArrayList<String>> entry : contributers.entrySet()) {
					userList.add(entry.getKey());
				}
				trans_unrevACK.put(trans_ID, userList);

				// repeat until get ack from all involved users
				while (userList.size() != 0) {
					// inform the decision to all unreceived and involved UserNodes
					for (String userID: userList) {
						MyMessage mmsg = new MyMessage(userID, "decision".getBytes(), 
									trans_ID, contributers.get(userID), decision, filename);
						if (DEBUG)  
							System.out.println( "Server: Sending decision of \"" + decision + 
																"\" to " + userID );
						PL.sendMessage(mmsg);
					}

					// init timestamp for phase 2
					long startTime = System.currentTimeMillis();
	
					// keeping check if timeout or receive all
					while (System.currentTimeMillis() - startTime <= TIMEOUT_TH) {
						userList = trans_unrevACK.get(trans_ID);
						// all received
						if (userList.size() == 0) {
							// mark this transcation as done
							if (trans_status.containsKey(trans_ID)) {
								trans_status.put(trans_ID, Tstatus.transDone.toString());
							}
							else {
								System.err.println( "Server: Error cannot find this transaction.");
							}

							try {
								// write log before 2PC done
								BufferedOutputStream writer = new
								BufferedOutputStream(new FileOutputStream(log_name, true));
								
								// format: <transaction_ID: transaction_status\n>
								String log_line = Integer.toString(trans_ID) + ":" + 
															Tstatus.transDone.toString() + "\n";
								writer.write(log_line.getBytes());
								writer.flush();
								writer.close();
								PL.fsync();
								
							} catch (Exception e) {
								System.err.println( "Server: Error " + e.getMessage());
								e.printStackTrace();
							}
					
							if (DEBUG)  System.out.println( "Server: Got all ack, transaction " 
																	+ trans_ID + " done." );
			
							// terminate the thread
							return;
						}
					}
				}
			}
		}).start();
	}

	/*
	 * continuePhase1: Continue the first phase from server interuption.
	 */
	public static void continuePhase1( int trans_ID ) {
		MyMessage attri = trans_attri.get(trans_ID);

		// cannot fully recover, abort the transaction
		attri.decision = Decisions.cancel.toString();
		trans_attri.put(trans_ID, attri);

		// if the composite image was saved, delete it
		File pic = new File(attri.filename);
		if (pic.exists() && (!pic.delete())) {
			System.err.println( "Server: Error delete composite image failed.");
		}

		// mark the transaction as phase 2
		if (trans_status.containsKey(trans_ID)) {
			trans_status.put(trans_ID, Tstatus.startPhase2.toString());
		}
		else {
			System.err.println( "Server: Error cannot find this transaction.");
		}
		
		// write log before phase 2
		write_log_status(trans_ID, Tstatus.startPhase2, attri.decision);

		// start another thread
		processPhase2(trans_ID);
	}

	/*
	 * recovery: Every time the server restart, check the log file and execute any 
	 * 			 interrupted operations (in phase 1 or 2).
	 */
	public static synchronized void recovery() {
		// check if log file exist
		File f_log = new File(log_name);
		if (f_log.exists()) {
			if (DEBUG)  System.out.println( "Server: find log, start recovery. " );

			byte buffer[] = new byte[(int) f_log.length()];

			try {
				// read from the log
				BufferedInputStream reader = new 
				BufferedInputStream(new FileInputStream(log_name));

				reader.read(buffer);
				reader.close();
				
			} catch (Exception e) {
				System.err.println( "Server: Error " + e.getMessage());
				e.printStackTrace();
			}

			String[] parsed_log = new String(buffer).split("\n");
			String status, filename, decision;
			int trans_ID, max_transID = 0;
		
			// update trans status, filename and contributors
			for (int i = 0; i < parsed_log.length; i++) {
				trans_ID = Integer.parseInt(parsed_log[i].split(":")[0]);
				status = parsed_log[i].split(":")[1];
				trans_status.put(trans_ID, status);

				if (trans_ID > max_transID) {
					max_transID = trans_ID;
				}

				if (status.equals(Tstatus.startPhase1.toString())) {
					filename = parsed_log[i + 1];

					HashMap<String, ArrayList<String>> contributers = new HashMap<>();

					// parse contributors
					String[] raw_contri = parsed_log[i + 2].split(" ");
					for (int j = 0; j < raw_contri.length; j++) {
						String srcID = raw_contri[j].split("@")[0];
						String[] srcName = raw_contri[j].split("@")[1].split(",");

						contributers.put( srcID, new ArrayList<String>(Arrays.asList(srcName)) );
					}

					// save recovered attributes to global structure
					MyMessage attri = new MyMessage("Server", "local".getBytes(), trans_ID, 
																filename, null, contributers);
					trans_attri.put(trans_ID, attri);

					// these two lines have been parsed
					i += 2;
				}
				else if (status.equals(Tstatus.startPhase2.toString())) {
					decision = parsed_log[i + 1];
					// attri structure should be recovered in previous line
					MyMessage attri = trans_attri.get(trans_ID);
					attri.decision = decision;
					trans_attri.put(trans_ID, attri);

					// this line has been parsed
					i += 1;
				}
			}

			// resume trans ID count
			num_ID = max_transID;

			// If any transaction is interrupted during phase 1 or 2, continue process
			for (Map.Entry<Integer, String> entry : trans_status.entrySet()) {
				trans_ID = entry.getKey();
				status = entry.getValue();
				if (status.equals(Tstatus.startPhase1.toString())) {
					if (DEBUG)  System.out.println( "Server: continue phase 1. " );
					continuePhase1(trans_ID);
				}
				else if (status.equals(Tstatus.startPhase2.toString())) {
					// redo phase 2
					if (DEBUG)  System.out.println( "Server: continue phase 2. " );
					processPhase2(trans_ID);
				}
			}
		}
	}

	/*
	 * handleMessage: Handle a message received by the server.
	 */
	public static synchronized void handleMessage( ProjectLib.Message msg ) {
		String msg_body = new String(msg.body);

		MyMessage mmsg = (MyMessage) msg;
		int trans_ID = mmsg.trans_ID;

		// if the transaction is already finished, do nothing
		if (trans_status.get(trans_ID).equals(Tstatus.transDone.toString())) {
			return;
		}

		MyMessage attri = trans_attri.get(trans_ID);
		String filename = attri.filename;
		byte[] img = attri.img;
		HashMap<String, ArrayList<String>> contributers = attri.contributers;
		int num_src = contributers.size();

		// if it's a opinion message
		if (msg_body.equals("opinion")) {
			// update votes
			synchronized (vote_lock) {
				trans_votes.put(trans_ID, trans_votes.get(trans_ID) + 1);
				
				if (mmsg.opinion.equals("ok")) {
					trans_okvotes.put(trans_ID, trans_okvotes.get(trans_ID) + 1);
					if (DEBUG)  System.out.println( "Server: Got message ok from " + msg.addr );
				}
				else if (mmsg.opinion.equals("notok")) {
					if (DEBUG)  System.out.println( "Server: Got message notok from " + msg.addr );
				}
				else {
					System.err.println("Error in voting: unexpected opinion type from " 
															+ msg.addr + " to Server");
				}
			}
		}
		// if it's a ack
		else if (msg_body.equals("ack")) {
			// remove this userNodes from unrevUser
			ArrayList<String> userList = trans_unrevACK.get(trans_ID);
			for (int i = 0; i < userList.size(); i++) {
				if (userList.get(i).equals(msg.addr)) {
					userList.remove(i);
					break;
				}
			}
			if (DEBUG)  System.out.println( "Server: Got ack from " + msg.addr );
		}
		else {
			System.err.println("Error in handleMessage: unexpected message type from " 
															+ msg.addr + " to Server");
		}
	}

	public static void main ( String args[] ) throws Exception {
		if (args.length != 1) throw new Exception("Need 1 arg: <port>");
		Server srv = new Server();
		PL = new ProjectLib( Integer.parseInt(args[0]), srv );
		if (DEBUG)  System.out.println( "Server initialized. " );

		recovery();
		
		// main loop
		while (true) {
			ProjectLib.Message msg = PL.getMessage();
			handleMessage(msg);
		}
	}
}
