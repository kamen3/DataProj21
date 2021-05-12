import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class DStoreToContrThread implements Runnable
{
    private int port;
    private int cport;
    private int timeout;
    private String file_folder;
    private PrintWriter contrPrOut;
    private BufferedReader contrBfIn;

    public DStoreToContrThread(int port_, int cport_, int timeout_, String file_folder_, PrintWriter contrPrOut_, BufferedReader contrBfIn_)
    {
        port = port_;
        cport = cport_;
        timeout = timeout_;
        file_folder = file_folder_;
        contrPrOut = contrPrOut_;
        contrBfIn = contrBfIn_;
    }

    public void run()
    {
        try
        {
            contrPrOut.println(Protocol.JOIN_TOKEN + " " + Integer.toString(port));

            String line;

            while ((line = contrBfIn.readLine()) != null)
            {
                /** Wait for input from controller */
            }
        }
        catch(Exception e) {}
    }
}
