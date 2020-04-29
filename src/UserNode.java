/* 
 *
 * Name: Rong Song
 * Andrew ID: rongsong
 *
 * UserNode.java 
 * 
 * The UserNode class for two-phase commit for group photo collage.
 * 
 */

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class UserNode implements ProjectLib.MessageHandling {
	public static String myId;
	public static ProjectLib PL;
	// type of messages
	private static enum msgType {
		prepare, decision;
	}
	// type of log lines
	private static enum logType {
		trans, fileLock, fileUnlock;
	}
	private static enum Tstatus {
		start, done, cancel;
	}
	// Opinions in phase 1 in 2PC
	private static enum Opinions {
		ok, notok;
	}
	// decisions in phase 2 in 2PC
	private static enum Decisions {
		done, cancel;
	}

	// map all existing transaction ID with their status (if it is finished)
	private static ConcurrentHashMap<Integer, Boolean> trans_status = new 
							ConcurrentHashMap<Integer, Boolean>();
	// store <srcPath, trans_ID> pairs for exclusive access to every occupied files
	private static ConcurrentHashMap<String, Integer> file_lock = new 
							ConcurrentHashMap<String, Integer>();

	// lock for read and then modify file lock
	private static Object rw_lock = new Object();
	
	private static String log_name = "user.log";
	private static Boolean DEBUG = true; 

	public UserNode( String id ) {
		myId = id;
	}

	/*
	 * get_opinion: get the opinon (ok or notok) for a prepare message from the server.
	 */
	private synchronized static String get_opinion(MyMessage mmsg) {
		for (String srcName: mmsg.srcNames) {
			File pic = new File(srcName);

			// check if the image is still there
			if ( !pic.exists() ) {
				return Opinions.notok.toString();
			}

			synchronized (rw_lock) {
				// if the image is occupied, return notok
				if (file_lock.containsKey(srcName)) {
					return Opinions.notok.toString();
				}
				// lock the involved image
				file_lock.put(srcName, mmsg.trans_ID);
			}

			try {
				// write log for file lock
				BufferedOutputStream writer = new
				BufferedOutputStream(new FileOutputStream(log_name, true));
				
				writer.write((logType.fileLock.toString() + "\n").getBytes());
				// format: <transaction_ID: source_name\n>
				String log_line = Integer.toString(mmsg.trans_ID) + ":" + srcName + "\n";
				writer.write(log_line.getBytes());
				writer.flush();
				writer.close();
				
			} catch (Exception e) {
				System.err.println( "I/O Error " + e.getMessage());
				e.printStackTrace();
			}
		}

		// check if user agree with that
        String[] arr = new String[mmsg.srcNames.size()]; 
        arr = mmsg.srcNames.toArray(arr); 
		if (PL.askUser(mmsg.img, arr)) {
			return Opinions.ok.toString();
		}
		else {
			return Opinions.notok.toString();
		}
	}

	/*
	 * deliverMessage: Callback called when message received.
	 * Return: True if the message is handled, or False if not.
	 */
	public synchronized boolean deliverMessage( ProjectLib.Message msg ) {
		String msg_body = new String(msg.body);
		if (DEBUG)  System.out.println( myId + ": Got message " + msg_body );

		MyMessage mmsg = (MyMessage) msg;
		int trans_ID = mmsg.trans_ID;
		ArrayList<String> srcNames = mmsg.srcNames;

		// if it's a prepare message
		if (msg_body.equals(msgType.prepare.toString())) {
			// If has handled this message before, skip it
			if (trans_status.containsKey(trans_ID)) {
				if (DEBUG)  System.out.println( myId + ": Do nothing to the resend message. ");
				return true;
			}

			byte[] img = mmsg.img;
			String[] sources = mmsg.sources;

			// init a transaction
			trans_status.put(trans_ID, false);

			try {
				// write log when transaction start
				BufferedOutputStream writer = new
				BufferedOutputStream(new FileOutputStream(log_name, true));
				
				writer.write((logType.trans.toString() + "\n").getBytes());
				// format: <transaction_ID: status\n>
				String log_line = Integer.toString(trans_ID) + ":" + Tstatus.start.toString() 
																						+ "\n";
				writer.write(log_line.getBytes());
				writer.flush();
				writer.close();
				PL.fsync();
				
			} catch (Exception e) {
				System.out.println( "I/O Error " + e.getMessage());
				e.printStackTrace();
			}
	
			String opinion = get_opinion(mmsg);

			// send the reply
			mmsg = new MyMessage("Server", "opinion".getBytes(), trans_ID, opinion);
			if (DEBUG)  System.out.println( myId + ": Sending opinion of " + opinion );
			PL.sendMessage(mmsg);
		}
		// if it's a decision
		else if (msg_body.equals(msgType.decision.toString())) {
			// decision of done
			if (mmsg.decision.equals(Decisions.done.toString())) {
				// delete sources images from the UserNode directories
				for (String srcName: mmsg.srcNames) {
					File pic = new File(srcName);

					if (!pic.delete()) {
						System.err.println(myId + ": Error delete image failed " + srcName);
					}
					else {
						if (DEBUG)  System.out.println( myId + ": " + srcName + 
																" deleted successfully. " );
					}
				}

				// mark the transaction as finished
				if (trans_status.containsKey(trans_ID)) {
					trans_status.put(trans_ID, true);

					try {
						// write log when transaction done
						BufferedOutputStream writer = new
						BufferedOutputStream(new FileOutputStream(log_name, true));
						
						writer.write((logType.trans.toString() + "\n").getBytes());
						// format: <transaction_ID: status\n>
						String log_line = Integer.toString(trans_ID) + ":" + 
															Tstatus.done.toString() + "\n";
						writer.write(log_line.getBytes());
						writer.flush();
						writer.close();
						
					} catch (Exception e) {
						System.err.println( "I/O Error " + e.getMessage());
						e.printStackTrace();
					}
				}
				else {
					System.err.println( myId + ": Error, cannot find this transaction.");
				}
	
				// unlock occupied resources
				for (Map.Entry<String, Integer> entry : file_lock.entrySet()) {
					if (entry.getValue() == trans_ID) {
						file_lock.remove(entry.getKey());
					}

					try {
						// write log for file lock
						BufferedOutputStream writer = new
						BufferedOutputStream(new FileOutputStream(log_name, true));
						
						writer.write((logType.fileUnlock.toString() + "\n").getBytes());
						// format: <transaction_ID: source_name\n>
						String log_line = Integer.toString(entry.getValue()) + ":"
															 + entry.getKey() + "\n";
						writer.write(log_line.getBytes());
						writer.flush();
						writer.close();
						
					} catch (Exception e) {
						System.err.println( "I/O Error " + e.getMessage());
						e.printStackTrace();
					}
				}
			}
			// decision of cancel
			else if (mmsg.decision.equals(Decisions.cancel.toString())) {
				// cancel the transaction
				if (trans_status.containsKey(trans_ID)) {
					trans_status.remove(trans_ID);

					try {
						// write log when transaction cancel
						BufferedOutputStream writer = new
						BufferedOutputStream(new FileOutputStream(log_name, true));
						
						writer.write((logType.trans.toString() + "\n").getBytes());
						// format: <transaction_ID: status\n>
						String log_line = Integer.toString(trans_ID) + ":" + 
															Tstatus.cancel.toString() + "\n";
						writer.write(log_line.getBytes());
						writer.flush();
						writer.close();
						
					} catch (Exception e) {
						System.err.println( "I/O Error " + e.getMessage());
						e.printStackTrace();
					}
			
					// unlock occupied resources
					// TODO: modularity, remove repeated code
					for (Map.Entry<String, Integer> entry : file_lock.entrySet()) {
						if (entry.getValue() == trans_ID) {
							file_lock.remove(entry.getKey());
						}

						try {
							// write log for file lock
							BufferedOutputStream writer = new
							BufferedOutputStream(new FileOutputStream(log_name, true));
							
							writer.write((logType.fileUnlock.toString() + "\n").getBytes());
							// format: <transaction_ID: source_name\n>
							String log_line = Integer.toString(entry.getValue()) + ":" + 
																	entry.getKey() + "\n";
							writer.write(log_line.getBytes());
							writer.flush();
							writer.close();
							
						} catch (Exception e) {
							System.err.println( "I/O Error " + e.getMessage());
							e.printStackTrace();
						}
					}
				}
				else {
					if (DEBUG)  System.out.println( myId + ": cancel decision re-received or"
																+ " prepare message missed.");
				}
			}
			else {
				System.err.println(myId + ": Error, unexpected message type in " + msg.addr);
			}
			// send back ack
			msg = new MyMessage("Server", "ack".getBytes(), trans_ID);
			if (DEBUG)  System.out.println( myId + ": Sending back ack for " + mmsg.decision );
			PL.sendMessage(msg);
		}
		else {
			System.err.println(myId + ": Error, unexpected message type in " + msg.addr);
		}

		return true;
	}
	
	/*
	 * recovery: When UserNode restart, check the log file and recover any lost status.
	 */
	public static synchronized void recovery() {
		// check if log file exist
		File f_log = new File(log_name);
		if (f_log.exists()) {
			if (DEBUG)  System.out.println( myId + ": find log, start recovery. " );

			byte buffer[] = new byte[(int) f_log.length()];

			try {
				// read from the log
				BufferedInputStream reader = new 
				BufferedInputStream(new FileInputStream(log_name));

				reader.read(buffer);
				reader.close();
				
			} catch (Exception e) {
				System.err.println( "I/O Error " + e.getMessage());
				e.printStackTrace();
			}

			String[] parsed_log = new String(buffer).split("\n");
			String status, srcName;
			int trans_ID;
		
			// update trans status and file lock; read 2 lines one time
			for (int i = 0; i < parsed_log.length; i += 2) {
				if (parsed_log[0].equals(logType.trans.toString())) {
					// format: <transaction_ID: status\n>
					trans_ID = Integer.parseInt(parsed_log[i + 1].split(":")[0]);
					status = parsed_log[i + 1].split(":")[1];
					if (status.equals(Tstatus.start.toString())) {
						trans_status.put(trans_ID, false);
					}
					else if (status.equals(Tstatus.done.toString())) {
						trans_status.put(trans_ID, true);
					}
					else if (status.equals(Tstatus.cancel.toString())) {
						trans_status.remove(trans_ID);
					}
				}
				else {
					// format: <transaction_ID: source_name\n>
					trans_ID = Integer.parseInt(parsed_log[i + 1].split(":")[0]);
					srcName = parsed_log[i + 1].split(":")[1];
					// lock or unlock resources
					if (parsed_log[0].equals(logType.fileLock.toString())) {
						file_lock.put(srcName, trans_ID);
					}
					else if (parsed_log[0].equals(logType.fileUnlock.toString())) {
						file_lock.remove(srcName);
					}
				}
			}
		}
	}

	public static synchronized void main ( String args[] ) throws Exception {
		if (args.length != 2) throw new Exception("Need 2 args: <port> <id>");
		UserNode UN = new UserNode(args[1]);
		PL = new ProjectLib( Integer.parseInt(args[0]), args[1], UN );
		System.out.println( args[1] + " initialized. ");
		
		recovery();
	}
}

