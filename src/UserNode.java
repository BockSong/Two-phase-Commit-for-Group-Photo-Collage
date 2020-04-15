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
		System.out.println( myId + ": Got message from " + msg.addr );
		if (msg.addr.equals("Server")) {
			// TODO: if it's inquiry
			// TODO: how to receive img from the server?
			byte[] img;
			String[] sources;
			if (PL.askUser(img, sources)) {
				// acceptable, return ack msg
				;
			}
			else {
				// unacceptable, return reject msg
				;
			}

			// TODO: if it's informing the success of commitment
			// remove sources images from the UserNode directories
			// perform other actions after commitment
		}

		return true;
	}
	
	public static void main ( String args[] ) throws Exception {
		if (args.length != 2) throw new Exception("Need 2 args: <port> <id>");
		UserNode UN = new UserNode(args[1]);
		PL = new ProjectLib( Integer.parseInt(args[0]), args[1], UN );
		
		ProjectLib.Message msg = new ProjectLib.Message( "Server", "hello".getBytes() );
		System.out.println( args[1] + ": Sending message to " + msg.addr );
		PL.sendMessage( msg );
	}
}

