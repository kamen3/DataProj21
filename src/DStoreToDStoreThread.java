import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Vector;

public class DStoreToDStoreThread implements Runnable
{
    private int portTo;
    private String filename;
    private String file_folder;
    private Socket client;
    private PrintWriter clientPrOut;
    private BufferedReader clientBfIn;
    private int portFrom;

    Vector<String> toRemove = new Vector<String>();

    String inpLine;
    String[] comArgs;
    String command;

    public DStoreToDStoreThread(int portTo_, String file_folder_, String filename_, int portFrom_, Vector<String> toRemove_)
    {
        portTo = portTo_;
        file_folder = file_folder_;
        filename = filename_;
        portFrom = portFrom_;
        toRemove = toRemove_;
    }

    public void run()
    {
        try
        {
            client = new Socket("localhost", portTo);
            clientPrOut = new PrintWriter(client.getOutputStream(), true);
            clientBfIn = new BufferedReader(new InputStreamReader(client.getInputStream()));

            File file = new File(Paths.get(".").toAbsolutePath().normalize().toString() + File.separator +
                    file_folder + File.separator + filename);
            int filesize = (int)file.length();

            clientPrOut.println(Protocol.REBALANCE_STORE_TOKEN + " " + filename + " " + filesize);

            /**
             *
             *
             *
             * Might have to check for timing out
             *
             *
             * */
            inpLine = clientBfIn.readLine();

            comArgs = inpLine.split(" ");
            command = comArgs[0];

            if(comArgs.length != 1 && !command.equals(Protocol.ACK_TOKEN))
            {
                System.out.println("really stinky");
                /** Probably logging */
            }

            byte[] fileContent = new byte[filesize];
            FileInputStream fileReader = new FileInputStream(file);
            fileReader.read(fileContent);
            fileReader.close();

            client.getOutputStream().write(fileContent);

            client.close();

            if(toRemove.contains(filename)) file.delete();
        }
        catch(Exception e) {System.out.println("Something wrong in DStore on DStore"); e.printStackTrace();}
    }
}
