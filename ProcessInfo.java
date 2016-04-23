import java.io.Serializable;
import java.net.InetAddress;

@SuppressWarnings("serial")
public class ProcessInfo implements Serializable
{
	int Id;
	int portNumber;
	InetAddress IP;
	
	public ProcessInfo(int Id, int port, InetAddress IP)
	{
		this.Id = Id;
		this.portNumber = port;
		this.IP = IP;
	}
	
}
