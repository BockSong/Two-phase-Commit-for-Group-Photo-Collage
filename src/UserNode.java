import java.io.File;

/* 
 *
 * Name: Rong Song
 * Andrew ID: rongsong
 *
 * UserNode.java 
 * 
 * The usernode (client) class for two-phase commit for group photo collage.
 * 
 */

public class UserNode implements ProjectLib.MessageHandling {
	public final String myId;
	public static ProjectLib PL;

	public UserNode( String id ) {
		myId = id;
	}

	/*
	 * deliverMessage: Callback called when message received.
	 * Return: True if the message is handled, or False if not.
	 */
	public boolean deliverMessage( ProjectLib.Message msg ) {
		String msg_body = new String(msg.body);
		System.out.println( myId + ": Got message ^" + msg_body + "^ from " + msg.addr );
		// if it's a prepare message
		if (msg_body.equals("prepare")) {
			File pic;
			byte[] img;
			String[] sources;
			String srcName, reply;

			// TODO: init a transaction

			// receive the image name from the server
			msg = PL.getMessage();
			srcName = new String(msg.body);
			pic = new File("." + myId + "/" + srcName);

			// receive the composite image from the server
			msg = PL.getMessage();
			img = msg.body;

			// TODO: lock the involved image. if it's already occupied, return notok
			// file lock: just manually maintain a hashmap of <filename, lock_object> should work?

			// check that this image is still there
			if (!pic.exists()) {
				// if not, return notok
				reply = "notok";
			}
			// ask user for decision
			else if (PL.askUser(img, sources)) {
				// acceptable, reply is ok
				reply = "ok";
			}
			else {
				// unacceptable, reply is notok
				reply = "notok";
			}

			// send the reply
			msg = new ProjectLib.Message("Server", reply.getBytes());
			System.out.println( "Server: Sending message to " + msg.addr );
			PL.sendMessage(msg);
		}
		// if it's a decision
		else {
			// decision of complete
			if (msg_body.equals("complete")) {
				File pic;
	
				// receive the image name from the server
				msg = PL.getMessage();
				srcName = new String(msg.body);
				pic = new File("." + myId + "/" + srcName);
	
				// delete sources images from the UserNode directories
				if (!pic.delete()) {
					System.out.println("Error: delete file failed from " + myId + "/" + srcName);
				}
	
				// TODO: mark the transaction as end
	
				// TODO: unlock resources?
	
			}
			// decision of quit
			else if (msg_body.equals("quit")) {
				// TODO: cancel transaction, unlock the image (do we do it here, or earlier once askUser is false?)
			}
			else {
				System.out.println("Error: unknown message type");
			}
			// send back ack
			msg = new ProjectLib.Message("Server", "ack".getBytes());
			System.out.println( "Server: Sending message to " + msg.addr );
			PL.sendMessage(msg);
		}

		return true;
	}
	
	public static void main ( String args[] ) throws Exception {
		if (args.length != 2) throw new Exception("Need 2 args: <port> <id>");
		UserNode UN = new UserNode(args[1]);
		PL = new ProjectLib( Integer.parseInt(args[0]), args[1], UN );
		System.out.println( args[1] + " initialized. ");
		
		//ProjectLib.Message msg = new ProjectLib.Message( "Server", "hello".getBytes() );
		//System.out.println( args[1] + ": Sending message to " + msg.addr );
		//PL.sendMessage( msg );
	}
}

