import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.omg.CORBA.portable.InputStream;

public class ServerNode extends Thread {
	
	int Id;
	ProcessInfo pred;
	ProcessInfo[] fingerTable;
    IntervalInfo[] ft_info;
	boolean[] keys;
	boolean[] duplicates;
    ProcessInfo self_info;
    ProcessInfo client;
    ProcessInfo node0;
    InetAddress localhost = InetAddress.getByName("127.0.0.1");
    boolean failStarter;
    Thread failChecker;
    boolean crash;
	
	ServerSocket server;
	
	public ServerNode(int Id, int port, ProcessInfo client) throws IOException
	{
		this.Id = Id;
		this.server = new ServerSocket(port, 10, localhost);
		this.pred = null;
		this.fingerTable = new ProcessInfo[8];
        // Stores useful interval information for easy comparison.
        this.ft_info = new IntervalInfo[8];
        this.failStarter = false;
        // Initialized to false.
		this.keys = new boolean[256];
		this.duplicates = new boolean[256];
        this.self_info = new ProcessInfo(Id, port, localhost);
        this.client = client;
        this.crash = false;
        for (int i = 0; i < 8; i++)
        {
            this.ft_info[i] = new IntervalInfo(Id, i);
            // Initialized for debugging purposes.
            this.fingerTable[i] = this.self_info;
        }
    }
	
	//sends the object to the specified node
	public void Send(ProcessInfo Node, Object toSend) throws IOException
	{
		Socket sender = new Socket(Node.IP, Node.portNumber);
		ObjectOutputStream mapSender = new ObjectOutputStream(sender.getOutputStream());
		
		//packet carries the object with a type identifying tag for the receiver.
		Object[] packet = new Object[2];
		
		if(toSend instanceof HashMap)
		{
			packet[0] = 1;
		}
		else if(toSend instanceof String)
		{
			packet[0] = 2;
		}
			
		packet[1] = toSend;
		mapSender.writeObject(packet);
		sender.close();
		
	}
	
    
    public void ping()
    {
    	this.failChecker = new Pinger(this.pred, this.self_info);
    	this.failChecker.start();

    }

	private void failDetectHandler() {
		int crashedId = pred.Id;
		System.out.println("Crashed => " + crashedId);

		int newPredecessorId = newPredFinder(crashedId);
		
		for(int i = newPredecessorId+1 ; i < (crashedId+1)%256; i++)
		{
			this.keys[i] = true;
		}
		
		this.pred = new ProcessInfo(newPredecessorId, this.node0.portNumber + newPredecessorId, localhost);
		this.failChecker.interrupt();
		ping();
		
		String message = "fail " + crashedId + " " + this.Id + " " + this.pred ;
		Runnable send = new ClientSender(this.pred, message);
		new Thread(send).start();
		
		String succMessage = "dupsUpdate "+ this.pred.Id + " "+ crashedId;
		Runnable send2 = new ClientSender(this.fingerTable[0], message);
		new Thread(send2).start();
	}
	
	public void dupsUpdate(int start, int end )
	{
		for(int i = start+1 ; i < (end+1)%256; i++)
		{
			this.duplicates[i] = true;
		}
	}

	private int newPredFinder(int crashedId) {
		int newPredecessorId = -1;

		for(int i = crashedId ; i>0; i--)
		{
			if(this.duplicates[i] = false)
			{
				newPredecessorId = i;
				break;
			}
		}
		
		return newPredecessorId;
	}
    
    
    public void crashUpdater(int crashId, int replace, int crashPred)
    {
    	if(this.failStarter || ( (this.Id+128)%256 <= crashPred ) )
    	{
    		String message = "crash" + crashId;
    		ack_sender(message);
    	}
    	
    	for(int i = 0; i< fingerTable.length ; i++)
    	{
    		if(fingerTable[i].Id == crashId)
    		{
    			fingerTable[i] = new ProcessInfo(replace, this.node0.portNumber + replace, localhost);
    		}
    	}
    	
    	try {
			Socket sock = new Socket(this.pred.IP, this.pred.portNumber);
			String message = "fail " + crashId + " "+ replace + " " + crashPred;
			DataOutputStream failSend = new DataOutputStream(sock.getOutputStream());
			failSend.writeUTF(message);
			sock.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
	
	public void nodeJoin(int node_id, String[] tokens)
	{
        // All nodes must be able to contact Node 0.
        this.node0 = new ProcessInfo(0,
                                    Integer.parseInt(tokens[3]),
                                    localhost);
        // Special case to initialize Node 0.
        if (node_id == 0)
        {
            for (int i = 0; i < 8; i++)
                this.fingerTable[i] = this.self_info;
            // All keys stored at Node 0 initially.
            for (int j = 0; j < 256; j++)
            {
                this.keys[j] = true;
                this.duplicates[j] = true;
            }
            this.pred = this.self_info;
            this.ack_sender("A");
        }
        else
        {
            this.init_ft(node_id, this.node0);
//            this.update_others();
        }
	}

    /*
     * Sends request for predecessor, successor, and successor finger table
     * to node0.
     */
    public void init_ft(int node_id, ProcessInfo node0)
    {
        String message = "j " + Integer.toString(node_id);
        Runnable sender = new ClientSender(node0, message);
        new Thread(sender).start();
    }

    /*
     * Checks if it is the successor of the new node. If it is not,
     * it will pass on the message to the node in its finger table
     * that is closest to and precedes the new node.
     */
    public void find_successor(int node_id, String purpose)
    {
        String[] tokens = purpose.split(" ");
        String message = "";

//        System.out.println("node id: " + Integer.toString(node_id)
//                            + " pred.Id " + Integer.toString(this.pred.Id)
//                            + " this.Id " + Integer.toString(this.Id));

        // Checks if current node is node_id's successor.
        if (contains(node_id, this.pred.Id, this.Id))
        {
            switch(tokens[0]) 
            {
                case "start":
                    this.update_as_successor(node_id);
                    break;
                case "i":
                    message = "u " + Integer.toString(this.Id) + " " + tokens[2];
                    int requester_id = Integer.parseInt(tokens[3]);
                    ProcessInfo receiver = new ProcessInfo(requester_id,
                                                            this.node0.portNumber+requester_id,
                                                            localhost);
                    Runnable sender = new ClientSender(receiver, message);
                    new Thread(sender).start();
                    break;
            }
        }
        // If this node is not the successor of node_id, then forward the
        // request to the closest preceding finger of node_id based on this
        // node's finger table.
        else {
            ProcessInfo cpf = this.closest_preceding_finger(node_id);
            switch(tokens[0]) {
                case "start":
                    message = "j " + Integer.toString(node_id);
                    break;
                case "i":
                    message = purpose;
                    break;
            }
            Runnable sender = new ClientSender(cpf, message);
            new Thread(sender).start();
//            System.out.println("pred_id: " + Integer.toString(this.pred.Id)
//                                + " node_id: " + Integer.toString(node_id)
//                                + " succ_id: " + Integer.toString(this.Id));
        }
    }

    /*
     * May not be necessary. Concurrency issues? Probably not...
     */
    public void update_as_pred(int node_id)
    {
        this.fingerTable[0] = new ProcessInfo(node_id,
                                              this.node0.portNumber+node_id,
                                              localhost);
    }

    /*
     * Called when a node realizes that it is the successor of the
     * new node being added to the network. This node's predecessor
     * and keys will be updated. This node's predecessor, finger table,
     * and duplicate keys will be sent to the new node.
     */
    public void update_as_successor(int node_id)
    {
        String duplicates = "";
        for (int i = 0; i < 256; i++)
        {
            if (this.duplicates[i])
            {
                duplicates += " " + Integer.toString(i);
                this.duplicates[i] = false;
            }
        }
        for (int j = this.pred.Id+1; j % 256 != (node_id + 1) % 256 ; j++)
        { 
            this.keys[j % 256] = false;
            this.duplicates[j % 256] = true;
        }

        // Notify the current predecessor to update its successor to the newly joined node.
        // TODO: find out if possible to update successor and predecessor's ft with these messages.
        int old_pred_id = this.pred.Id;
        String update_message = "p " + Integer.toString(node_id);
        Runnable update_sender = new ClientSender(this.pred, update_message);
        new Thread(update_sender).start();

        // Update predecessor to the newly joined node.
        this.pred = new ProcessInfo(node_id,
                                    this.node0.portNumber+node_id,
                                    localhost);
        if (old_pred_id == this.Id)
            this.fingerTable[0] = this.pred;

        // Gives new node its predecessor's id, successor's id,
        // successor's finger table, and successor's original duplicates.
        String message = "successor"
                        + " " + Integer.toString(old_pred_id)
                        + " " + Integer.toString(this.Id)
                        + " " + Integer.toString(this.fingerTable[0].Id)
                        + "," + Integer.toString(this.fingerTable[1].Id)
                        + "," + Integer.toString(this.fingerTable[2].Id)
                        + "," + Integer.toString(this.fingerTable[3].Id)
                        + "," + Integer.toString(this.fingerTable[4].Id)
                        + "," + Integer.toString(this.fingerTable[5].Id)
                        + "," + Integer.toString(this.fingerTable[6].Id)
                        + "," + Integer.toString(this.fingerTable[7].Id)
                        + duplicates;
        Runnable sender = new ClientSender(this.pred, message);
        new Thread(sender).start();
    }

    // Find the closest node that is less than node_id.
    public ProcessInfo closest_preceding_finger(int node_id)
    {
        for (int i = 7; i >= 0; i--)
            if (this.ft_info[i].contains(this.fingerTable[i].Id))
                return this.fingerTable[i];
        // TODO: Figure out if this is necessary.
        return this.fingerTable[0];
    }

    /*
     * Upon receiving a message from its successor, the new node will
     * set its predecessor and initialize its finger table. It will
     * also update its keys as necessary.
     */
    public void update_ft(int node_id, String[] tokens)
    {
        // Set the successor
        int succ_id = Integer.parseInt(tokens[2]);
        this.fingerTable[0] = new ProcessInfo(succ_id,
                                              this.node0.portNumber+succ_id,
                                              localhost);
        // Set the predecessor
        int pred_id = Integer.parseInt(tokens[1]);
        this.pred = new ProcessInfo(pred_id,
                                    this.node0.portNumber+pred_id,
                                    localhost);
        // Initialize the finger table.
        // Successor's finger table is provided in tokens[3], could be useful for
        // optimizations later on.
        for (int i = 0; i < 7; i++)
        {
            // TODO: Figure out whether the ft[i].id+1 is necessary.
            if (contains(this.ft_info[i+1].start, this.Id, this.fingerTable[i].Id+1))
                this.fingerTable[i+1] = this.fingerTable[i];
            else
            {
                // "i <start> <ft index> <node_id>"
                // start is the thing we want the successor for.
                // ft index is the index in which we will store our returned value.
                // node_id is so that the successor knows where to send its id.
                String message = "i"
                                + " " + this.ft_info[i+1].start
                                + " " + Integer.toString(i+1)
                                + " " + Integer.toString(this.Id);
                Runnable sender = new ClientSender(node0, message);
                new Thread(sender).start();
            }
//                System.out.println("my id: " + Integer.toString(this.Id)
//                                    + " start: " + Integer.toString(node_id)
//                                    + " end: " + Integer.toString(this.fingerTable[i].Id)
//                                    + " value: " + Integer.toString(this.ft_info[i+1].start));
        }
        // Update the keys.
        for (int j = pred_id+1; j % 256 != (this.Id + 1) % 256 ; j++)
            this.keys[j % 256] = true;
        for (int k = 4; k < tokens.length; k++)
            this.duplicates[Integer.parseInt(tokens[k])] = true;
        // TODO: remove this once finger table updating is implemented.
        // This ACK should actually be sent after all other nodes are finished updating
        // their finger tables.
        this.ack_sender("A");
    }

    /*
     * Updates a finger table index to point to the info of the successor of ft[i].start
     */
    public void update_ft_entry(String[] tokens)
    {
        int index = Integer.parseInt(tokens[2]);
        int succ_id = Integer.parseInt(tokens[1]);
        this.fingerTable[index] = new ProcessInfo(succ_id,
                                                  this.node0.portNumber+succ_id,
                                                  localhost);
//        System.out.println("New FT at " + Integer.toString(this.Id));
//        for (int i = 0; i < 8; i++)
//            System.out.println(Integer.toString(i) + ": " + Integer.toString(this.fingerTable[i].Id));
//        System.out.println("Pred: " + Integer.toString(this.pred.Id));
    }

    public void update_others(int node_id)
    {
        // An idea: in order to wait for all other nodes to update, send the update message,
        // the last node to update should send a message back here, so wait until you receive
        // that message to send the ACK to the client. This assumes that this node's ft has
        // already been updated and that key transferring is complete.
        ProcessInfo new_node = new ProcessInfo(node_id,
                                               this.node0.portNumber+node_id,
                                               localhost);
        // Update this.fingerTable[...]
        for (int i = 0; i < 8; i++)
        {
            if (this.fingerTable[i].Id >= node_id)
                this.fingerTable[i] = new_node;
            else
                break;
        }
    }
	
    /*
     * Responds to client with node_id, finger table entries, and keys as an ACK.
     * The client cant directly print this message out since nodeShow takes care of
     * the formatting.
     */
    public void nodeShow(int node_id, String[] tokens)
    {
        String keys = "";
        for (int i = 0; i < 256; i++)
            if (this.keys[i])
                keys += " " + Integer.toString(i);
        String response = Integer.toString(node_id)
                          + "\n" + "FingerTable: " + this.fingerTable[0].Id
                          + "," + this.fingerTable[1].Id
                          + "," + this.fingerTable[2].Id
                          + "," + this.fingerTable[3].Id
                          + "," + this.fingerTable[4].Id
                          + "," + this.fingerTable[5].Id
                          + "," + this.fingerTable[6].Id
                          + "," + this.fingerTable[7].Id
                          + "\n" + "Keys:" + keys + "\n"
                          // temp remove this after
                          + "pred: " + Integer.toString(this.pred.Id) + "\n";
        this.ack_sender(response);
    }
    
    public void find(int node_id, int key)
    {
    	if(keys[key%256] == true)
    	{
    		String message = "find " + node_id;
            ack_sender(message);
    	}
    	else
    	{
    		ProcessInfo nextNode = null;
    		for(int i = 0; i< fingerTable.length; i++)
    		{
    			if(fingerTable[i].Id <= node_id)
    			{
    				if(fingerTable[i].Id > nextNode.Id)
    				{
    					nextNode = fingerTable[i];
    				}
    			}
        		
    		}
    		
			String message = "find " + node_id + " " + key+ " " + client.portNumber ;
    		Runnable sender = new ClientSender(nextNode, message);
    		new Thread(sender).start();
    		
    	}
    	
    }

	//handles received messages
	private void receiver() throws IOException, ClassNotFoundException {
		
		ping();
		Socket receiver = server.accept();
		
        DataInputStream input = new DataInputStream(receiver.getInputStream());
        String message = "";
        message = input.readUTF();
        System.out.println("Server " + Integer.toString(this.Id) + " received: " + message);
        if (this.pred != null) System.out.println(this.pred.Id);
        System.out.println(this.fingerTable[0].Id);
        String[] tokens = message.split(" ");
        String action = tokens[0];
        int node_id = Integer.parseInt(tokens[1]);
        switch (action) {
            case "join": 
                this.nodeJoin(node_id, tokens);
                break;
            case "dupsUpdate":
            	int start = Integer.parseInt(tokens[1]);
            	int end = Integer.parseInt(tokens[1]);
            	this.dupsUpdate(start, end);
            case "predFailed":
            	this.failDetectHandler();
            	break;
            case "fail":
            	int crashId = Integer.parseInt(tokens[1]);
            	int replace = Integer.parseInt(tokens[2]);
            	int crashPred = Integer.parseInt(tokens[3]);
            	this.crashUpdater(crashId,replace, crashPred);
            case "crash":
            	this.crash = true;
            	this.failChecker.interrupt();
            	break;
            case "find":
            	int node = Integer.parseInt(tokens[1]);
            	int key = Integer.parseInt(tokens[2]);
            	client = new ProcessInfo(-1, Integer.parseInt(tokens[3]) , localhost);
            	this.find(node, key);
                break;
            case "show":
                nodeShow(node_id, tokens);
                break;
            case "j":
                this.find_successor(node_id, "start");
                break;
            case "i":
                this.find_successor(node_id, message);
                break;
            case "u":
                this.update_ft_entry(tokens);
                break;
            case "successor":
                update_ft(node_id, tokens);
                break;
            case "p":
                update_as_pred(node_id);
                break;
        }
        
        this.failStarter = false; 

	}

    /*
     * Sends a simple acknowledgement to the client.
     */
    public void ack_sender(String ack)
    {
        try {
            Socket sendSock = new Socket(this.client.IP, this.client.portNumber);	   
            DataOutputStream out = new DataOutputStream(sendSock.getOutputStream());
            out.writeUTF(ack);
            sendSock.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
	
	public void run()
	   {
	      while(true)
	      {
	         try
	         {	 
	        	if(!crash)
	        	{
	        		receiver();
	        	}
	            
	         }catch(SocketTimeoutException s)
	         {
	            System.out.println("Socket timed out!");
	            break;
	         }catch(IOException e)
	         {
	            e.printStackTrace();
	            break;
	         } catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	      }
	   }
	
	public static void main(String args[]) throws InterruptedException
	{
//		 try
//	      {
//	    	 int port = Integer.parseInt(args[0]);
//	         int Id = Integer.parseInt(args[1]);
//	         //starts the server
//	    	 ServerNode receiver = new ServerNode(Id, port);
//	         receiver.start();
//	    
//	         Map<Integer, ProcessInfo> fingers = new HashMap<Integer, ProcessInfo>();
//	         fingers.put(80, new ProcessInfo(80, 1231, InetAddress.getLocalHost()));
//	         fingers.put(81, new ProcessInfo(81, 1233, InetAddress.getLocalHost()));
//	         fingers.put(82, new ProcessInfo(82, 1234, InetAddress.getLocalHost()));
//	         
//	         ProcessInfo test = new ProcessInfo(Id, port, InetAddress.getLocalHost());
//	         Thread.sleep(2000);
//	         receiver.Send( test, fingers);
//	       
//	      }
//	      catch(IOException e)
//	      {
//	         e.printStackTrace();
//	      }
	}

    /* 
     * Returns whether or not a given id falls
     * within this interval [start,end).
     */
    public static boolean contains(int id, int start, int end)
    {
        return   (start < end && id >= start && id < end)
               ||(start > end && (id >= start || id < end))
               ||(start == end);
    }

    /*
     * Computes the size of an interval. Useful in loops since
     * it is possible that the start > end.
     */
    public static int compute_size(int start, int end)
    {
        if (start > end)
            return 256 - start + end;
        else
            return start - end;
    }
}

/*
 * 1) Crash => kills thread
 * 2) duplicates storing predecessor's keys
 * 3)
 * 
 * 
 */

