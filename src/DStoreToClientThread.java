import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Paths;

public class DStoreToClientThread implements Runnable
{
    private int port;
    private int cport;
    private int timeout;
    private String file_folder;
    private PrintWriter contrPrOut;
    private BufferedReader contrBfIn;
    private Socket client;
    private PrintWriter clientPrOut;
    private BufferedReader clientBfIn;

    String inpLine;
    String[] comArgs;
    String command;

    public DStoreToClientThread(Socket client_, int port_, int cport_, int timeout_, String file_folder_, PrintWriter contrPrOut_, BufferedReader contrBfIn_)
    {
        client = client_;
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
            clientPrOut = new PrintWriter(client.getOutputStream(), true);
            clientBfIn = new BufferedReader(new InputStreamReader(client.getInputStream()));

            while ((inpLine = clientBfIn.readLine()) != null)
            {
                comArgs = inpLine.split(" ");
                command = comArgs[0];

                if(command.equals(Protocol.STORE_TOKEN)) actOnStore();

                //else System.out.println("unrecognised command");// Will probably have to call logger here
            }
        }
        catch(Exception e) {}
    }

    private void actOnStore()
    {
        if(comArgs.length != 3)
        {
            /** Do some logging */
            return;
        }

        String filename = comArgs[1];
        int filesize = Integer.parseInt(comArgs[2]);
        byte[] fileContent = null;

        try
        {
            clientPrOut.println(Protocol.ACK_TOKEN);
            int readsize = client.getInputStream().readNBytes(fileContent, 0, filesize);
            if(readsize != filesize) throw new Exception();
        }
        catch(Exception e) {/** Probably log error here */}

        File file = new File(Paths.get(".").toAbsolutePath().normalize().toString() + File.separator +
                file_folder + File.separator + filename);

        try
        {
            FileOutputStream fileOut = new FileOutputStream(file);
            fileOut.write(fileContent);
            fileOut.close();
        }
        catch(Exception e) {}

        contrPrOut.println(Protocol.STORE_ACK_TOKEN);

        try
        {
            client.close();
        }
        catch(Exception e){}
    }
}
