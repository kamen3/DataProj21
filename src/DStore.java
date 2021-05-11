import java.io.*;
import java.net.Socket;

public class DStore
{
    public static void main(String[] args) throws IOException
    {
        final int port = Integer.getInteger(args[0]);
        final int cport = Integer.getInteger(args[1]);
        final int timeout = Integer.getInteger(args[2]);
        final String file_folder = args[3];

        try
        {
            Socket controller = new Socket("localhost", cport);
            PrintWriter out = new PrintWriter(controller.getOutputStream(), true);
            BufferedReader bfin = new BufferedReader(new InputStreamReader (controller.getInputStream()));

            out.println(Protocol.JOIN_TOKEN + " " + Integer.toString(port));

            String line;

            while((line=bfin.readLine()) != null)
            {
                System.out.println("liine " + line);
            }
        }
        catch(Exception e){System.out.println("error"+e);}
    }
}
