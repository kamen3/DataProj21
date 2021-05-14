import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Paths;

public class DStoreToContrThread implements Runnable
{
    private int port;
    private int cport;
    private int timeout;
    private String file_folder;
    private PrintWriter contrPrOut;
    private BufferedReader contrBfIn;

    String inpLine;
    String[] comArgs;
    String command;

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

            while ((inpLine = contrBfIn.readLine()) != null)
            {
                comArgs = inpLine.split(" ");
                command = comArgs[0];

                /** Wait for input from controller */
                if(command.equals(Protocol.REMOVE_TOKEN)) actOnRemove();

            }
        }
        catch(Exception e) {}
    }

    private void actOnRemove()
    {
        if(comArgs.length != 2)
        {
            /** Do some logging */
            return;
        }

        String filename = comArgs[1];

        try
        {
            File file = new File(Paths.get(".").toAbsolutePath().normalize().toString() + File.separator +
                    file_folder + File.separator + filename);

            if(!file.exists())
            {
                contrPrOut.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                return;
            }

            file.delete();

            contrPrOut.println(Protocol.REMOVE_ACK_TOKEN + " " + filename);
        }
        catch(Exception e) {System.out.println("Excuse me, " + e);}
    }
}
