import java.awt.*;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
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

                if(command.equals(Protocol.STORE_TOKEN)) actOnStore(); // Client
                else if(command.equals(Protocol.LOAD_DATA_TOKEN)) actOnLoadData(); // Client
                else if(command.equals(Protocol.REBALANCE_STORE_TOKEN)) actOnRebalanceStore();

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
        byte[] fileContent = new byte[filesize];

        try
        {
            clientPrOut.println(Protocol.ACK_TOKEN);
            int readsize = client.getInputStream().readNBytes(fileContent, 0, filesize);
            if(readsize != filesize) throw new Exception("file"  + filename + " received malformed");

            File file = new File(Paths.get(".").toAbsolutePath().normalize().toString() + File.separator +
                    file_folder + File.separator + filename);
            file.createNewFile();

            FileOutputStream fileOut = new FileOutputStream(file);
            fileOut.write(fileContent);
            fileOut.close();

            contrPrOut.println(Protocol.STORE_ACK_TOKEN + " " + filename);

            client.close();
        }
        catch(Exception e) {System.out.println("Excuse me, " + e);}
    }

    private void actOnLoadData()
    {
        if(comArgs.length != 2)
        {
            /** Do some logging */
            return;
        }

        String filename = comArgs[1];
        File file = new File(Paths.get(".").toAbsolutePath().normalize().toString() + File.separator +
                file_folder + File.separator + filename);

        try
        {
            if (!file.exists())
            {
                client.close();
                return;
            }

            byte[] fileContent = new byte[(int)file.length()];
            FileInputStream fileReader = new FileInputStream(file);
            fileReader.read(fileContent);
            fileReader.close();

            client.getOutputStream().write(fileContent);
            client.close();
        }
        catch(Exception e) {System.out.println("Error when loading file"); e.printStackTrace();}
    }

    private void actOnRebalanceStore()
    {
        if(comArgs.length != 3)
        {
            System.out.println("Something wornginwong");
        }

        String filename = comArgs[1];
        int filesize = Integer.parseInt(comArgs[2]);

        try
        {
            System.out.println("I am creating " + filename);
            File file = new File(Paths.get(".").toAbsolutePath().normalize().toString() + File.separator +
                    file_folder + File.separator + filename);

            clientPrOut.println(Protocol.ACK_TOKEN);

            byte[] fileContent = new byte[filesize];
            int readSize = client.getInputStream().readNBytes(fileContent, 0, filesize);

            if(readSize != filesize) throw new Exception("file"  + filename + " received malformed");

            Files.write(file.toPath(), fileContent);
            System.out.println("I created " + filename);
        }
        catch(Exception e) {System.out.println("Did not store correctly"); e.printStackTrace();}
    }
}
