import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class DStore
{
    public static void main(String[] args) throws IOException
    {
        final int port = Integer.parseInt(args[0]);
        final int cport = Integer.parseInt(args[1]);
        final int timeout = Integer.parseInt(args[2]);
        final String file_folder = args[3];

        ServerSocket ss = new ServerSocket(port);
        Socket controller = new Socket("localhost", cport);
        PrintWriter contrPrOut = new PrintWriter(controller.getOutputStream(), true);
        BufferedReader contrBfIn = new BufferedReader(new InputStreamReader(controller.getInputStream()));

        new Thread(new DStoreToContrThread(port, cport, timeout, file_folder, contrPrOut, contrBfIn)).start();

        for(;;)
        {
            try
            {
                Socket client = ss.accept();
                new Thread(new DStoreToClientThread(client, port, cport, timeout, file_folder, contrPrOut, contrBfIn)).start();
            }
            catch(Exception e){}
        }
    }
}
