import java.io.DataOutputStream;
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
	ServerNode prev;
	Map<Integer, ProcessInfo> fingerTable;
	ArrayList<Integer> keys;
	ArrayList<Integer> duplicates;
	
	ServerSocket server;
	
	public ServerNode(int Id, int port) throws IOException
	{
		this.Id = Id;
		this.server = new ServerSocket(port, 10, InetAddress.getLocalHost());
		this.prev = null;
		this.fingerTable = new HashMap<Integer, ProcessInfo>();
		this.keys = new ArrayList<>();
		this.duplicates = new ArrayList<>();
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
	
	public void nodeJoin()
	{
		
	}
	
	//handles received messages
	private void receiver() throws IOException, ClassNotFoundException {
		
		Socket reciever = server.accept();
		
		ObjectInputStream input = new ObjectInputStream(reciever.getInputStream());
		Object[] message = (Object[]) input.readObject();
		int type = (int) message[0];
		
		System.out.println("type = " + type);
		
		if(type == 1)
		{
			@SuppressWarnings("unchecked")
			Map<Integer, ProcessInfo> yourMap = (HashMap<Integer, ProcessInfo>) message[1];
			
			
			for(ProcessInfo p : yourMap.values())
			System.out.println("Id = "+ p.Id + ", port ="+ p.portNumber + ", inet =" + p.IP.toString());
		}
		else if(type == 2)
		{
			String myString = (String) message[1];
			
			System.out.println(myString);
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
		 try
	      {
	    	 int port = Integer.parseInt(args[0]);
	         int Id = Integer.parseInt(args[1]);
	         //starts the server
	    	 ServerNode reciever = new ServerNode(Id, port);
	         reciever.start();
	    
	         Map<Integer, ProcessInfo> fingers = new HashMap<Integer, ProcessInfo>();
	         fingers.put(80, new ProcessInfo(80, 1231, InetAddress.getLocalHost()));
	         fingers.put(81, new ProcessInfo(81, 1233, InetAddress.getLocalHost()));
	         fingers.put(82, new ProcessInfo(82, 1234, InetAddress.getLocalHost()));
	         
	         ProcessInfo test = new ProcessInfo(Id, port, InetAddress.getLocalHost());
	         Thread.sleep(2000);
	         reciever.Send( test, fingers);
	       
	      }
	      catch(IOException e)
	      {
	         e.printStackTrace();
	      }
	}
	

}
