/* MyMessage.java */

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MyMessage extends ProjectLib.Message{
    // defalut attributes
    public String addr;
    public byte[] body; // indicate the type of the message

    // added message attributes
    public Integer trans_ID;
    public ArrayList<String> srcNames;
    public byte[] img;
    public String[] sources;
    public String decision;
    public String filename;
    public String opinion;

    // local attributes for server
    public int votes;
    public int okvotes;
    public HashMap<String, ArrayList<String>> contributers;

    public MyMessage( String addr, byte[] body ) {
        super(addr, body);
    }

    /* For Server */
    // local usage
    public MyMessage( String addr, byte[] body, int trans_ID, String filename, byte[] img, 
                                        HashMap<String, ArrayList<String>> contributers ) {
        super(addr, body);
        this.trans_ID = trans_ID;
        this.filename = filename;
        this.img = img;
        this.contributers = contributers;
    }

    // prepare type
    public MyMessage( String addr, byte[] body, int trans_ID, ArrayList<String> srcNames, 
                                            byte[] img, String[] sources ) {
        super(addr, body);
        this.trans_ID = trans_ID;
        this.srcNames = srcNames;
        this.img = img;
        this.sources = sources;
    }

    // decision type
    public MyMessage( String addr, byte[] body, int trans_ID, ArrayList<String> srcNames, 
                                            String decision, String filename ) {
        super(addr, body);
        this.trans_ID = trans_ID;
        this.srcNames = srcNames;
        this.decision = decision;
        this.filename = filename;
    }

    /* For UserNode */
    // opinion type
    public MyMessage( String addr, byte[] body, int trans_ID, String opinion ) {
        super(addr, body);
        this.trans_ID = trans_ID;
        this.opinion = opinion;
    }

    // ask type
    public MyMessage( String addr, byte[] body, int trans_ID ) {
        super(addr, body);
        this.trans_ID = trans_ID;
    }
}
