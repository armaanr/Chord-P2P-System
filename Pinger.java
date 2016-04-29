import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class Pinger extends Thread {
	
	ProcessInfo toPing;
	ProcessInfo parent;
	
	public Pinger(ProcessInfo ping, ProcessInfo prnt)
	{
		this.toPing = ping;
		this.parent = prnt;
	}
	
	public void run()
    {
		while(true)
		{
			Socket sock = null;
			Socket reply = null;
			try {
				sock = new Socket(this.toPing.IP, this.toPing.portNumber);
			} catch (IOException e) {
				
				try {
					reply = new Socket(this.parent.IP, this.parent.portNumber);
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				
				DataOutputStream out = null;
				try {
					out = new DataOutputStream(reply.getOutputStream());
				} catch (IOException e2) {
					// TODO Auto-generated catch block
					e2.printStackTrace();
				}
	            try {
					out.writeUTF("predFailed "+ -1);
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
	            try {
					reply.close();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				
			}
			
			try {
				sock.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			try {
				this.sleep(4000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
		
    }

}
