import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
	boolean[] keys;
	boolean[] duplicates;
    ProcessInfo self_info;
    ProcessInfo client;
    InetAddress localhost = InetAddress.getByName("127.0.0.1");
	
	ServerSocket server;
	
	public ServerNode(int Id, int port, ProcessInfo client) throws IOException
	{
		this.Id = Id;
		this.server = new ServerSocket(port, 10, localhost);
		this.pred = null;
		this.fingerTable = new ProcessInfo[8];
        // Initialized to false.
		this.keys = new boolean[256];
		this.duplicates = new boolean[256];
        this.self_info = new ProcessInfo(Id, port, localhost);
        this.client = client;
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
	
	public void nodeJoin(int node_id, String[] tokens)
	{
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
            ProcessInfo node0 = new ProcessInfo(0,
                                            Integer.parseInt(tokens[3]),
                                            localhost);
            this.init_ft(node_id, node0);
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
     * Upon receiving a message from its successor, the new node will
     * set its predecessor and initialize its finger table. It will
     * also update its keys as necessary.
     */
    public void update_ft(int node_id, String[] tokens)
    {
        // Set the predecessor
        int pred_id = Integer.parseInt(tokens[1]);
        this.pred = new ProcessInfo(pred_id,
                                    2000+pred_id,
                                    localhost);
        // Initialize the finger table.
        int succ_id = Integer.parseInt(tokens[2]);
        String[] ft_entries = tokens[3].split(",");
        int[] successor_ft = new int[8];
        // Use as many entries from the successor's table as possible.
        for (int i = 0; i < 8; i++)
        {
//            this.fingerTable[i] = new ProcessInfo(Integer.parseInt(ft_entries),
//                                                  2000+succ_id,
//                                                  localhost);
        }
        // Update the keys.
        for (int j = pred_id+1; j <= this.Id; j++)
            this.keys[j] = true;
        for (int k = 4; k < tokens.length; k++)
            this.duplicates[Integer.parseInt(tokens[k])] = true;
        this.ack_sender("A");
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
        if (this.pred.Id == 0 && this.Id == 0)
        {
            for (int i = 1; i <= node_id; i++)
            {
                this.keys[i] = false;
                this.duplicates[i] = true;
            }
            for (int j = node_id+1; j <= 256; j++)
            {
                this.keys[j % 256] = true;
                this.duplicates[j % 256] = false;
                duplicates += " " + Integer.toString(j % 256);
            }
        }

        else {
            for (int i = 0; i < 256; i++)
            {
                // Remove all duplicates.
                // Store them in a string for the new node.
                if (this.duplicates[i])
                {
                    duplicates += " " + Integer.toString(i);
                    this.duplicates[i] = false;
                }
                // Keep transferred keys as duplicates.
                if (i > this.pred.Id && i<= node_id)
                {
                    this.keys[i] = false;
                    this.duplicates[i] = true;
                }
            }
        }
        // TODO: find a way of getting the base port
        // Update predecessor
        int old_pred_id = this.pred.Id;
        this.pred = new ProcessInfo(node_id,
                                    2000+node_id,
                                    localhost);

        // Gives new node its predecessor's id, successor's id,
        // successor's finger table, and successor's original duplicates.
        String message = "neighbors"
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

    public void update_others(int node_id)
    {
        // TODO: find a way of getting the port
        ProcessInfo new_node = new ProcessInfo(node_id, 2000+node_id, localhost);
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
     * Checks if it is the successor of the new node. If it is not,
     * it will pass on the message to the node in its finger table
     * that is closest to and precedes the new node.
     */
    public void find_successor(int node_id)
    {
        // Checks if current node is node_id's successor.
        // Account for wrap-around.
        if ((this.Id == this.pred.Id) ||
            (this.pred.Id > this.Id && node_id > this.pred.Id && node_id < this.Id) ||
            (this.pred.Id < this.Id && node_id < this.Id && node_id > this.pred.Id))
        {
            this.update_as_successor(node_id);
            this.update_others(node_id);
        }
        else {
            // Find the closest node that is less than node_id and forward message.
            int i;
            for (i = 7; i >= 0; i--)
            {
                if (this.fingerTable[i].Id < node_id)
                    break;
            } 
            String message = "j " + Integer.toString(node_id);
            Runnable sender = new ClientSender(fingerTable[i], message);
            new Thread(sender).start();
        }
    }
	
	//handles received messages
	private void receiver() throws IOException, ClassNotFoundException {
		
		Socket receiver = server.accept();
		
        DataInputStream input = new DataInputStream(receiver.getInputStream());
        String message = "";
        message = input.readUTF();
        System.out.println("Server " + Integer.toString(this.Id) + " received: " + message);
        String[] tokens = message.split(" ");
        String action = tokens[0];
        int node_id = Integer.parseInt(tokens[1]);
        switch (action) {
            case "join":
                this.nodeJoin(node_id, tokens);
                break;
            case "crash":
                break;
            case "find":
                break;
            case "show":
                break;
            case "j":
                this.find_successor(node_id);
                break;
            case "neighbors":
                update_ft(node_id, tokens);
                break;
        }
//		ObjectInputStream input = new ObjectInputStream(receiver.getInputStream());
//		Object[] message = (Object[]) input.readObject();
//		int type = (int) message[0];
//		
//		System.out.println("type = " + type);
//		
//		if(type == 1)
//		{
//			@SuppressWarnings("unchecked")
//			Map<Integer, ProcessInfo> yourMap = (HashMap<Integer, ProcessInfo>) message[1];
//			
//			
//			for(ProcessInfo p : yourMap.values())
//			System.out.println("Id = "+ p.Id + ", port ="+ p.portNumber + ", inet =" + p.IP.toString());
//		}
//		else if(type == 2)
//		{
//			String myString = (String) message[1];
//			
//			System.out.println(myString);
//		}
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
	            receiver();
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
	

}
