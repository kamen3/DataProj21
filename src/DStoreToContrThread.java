import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.file.Paths;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

public class DStoreToContrThread implements Runnable
{
    private int port;
    private int cport;
    private int timeout;
    private String file_folder;
    private PrintWriter contrPrOut;
    private BufferedReader contrBfIn;
    private Socket contrSocket;

    String inpLine;
    String[] comArgs;
    String command;

    public DStoreToContrThread(int port_, int cport_, int timeout_, String file_folder_, Socket contrSocket_)
    {
        port = port_;
        cport = cport_;
        timeout = timeout_;
        file_folder = file_folder_;
        contrSocket = contrSocket_;
        try
        {
            contrPrOut = new PrintWriter(contrSocket.getOutputStream(), true);
            contrBfIn = new BufferedReader(new InputStreamReader(contrSocket.getInputStream()));
        }
        catch(Exception e) {System.out.println("don't even log");}
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
                else if(command.equals(Protocol.REBALANCE_TOKEN)) actOnRebalance();
                else if(command.equals(Protocol.LIST_TOKEN)) actOnList();
                else System.out.println("Some unknown command given from Controller");
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

    private void actOnRebalance()
    {
        ConcurrentHashMap<String, Vector<Integer>> commandsToSend = new ConcurrentHashMap<String, Vector<Integer>>();
        Vector<String> toRemove = new Vector<String>();
        Vector<String> toSend = new Vector<String>();

        System.out.println("REB " + inpLine);

        int j=2;

        for(int i=0; i<Integer.parseInt(comArgs[1]); i++)
        {
            Vector<Integer> tmp = new Vector<Integer>();

            String filename = comArgs[j++];
            int numTos = Integer.parseInt(comArgs[j++]);
            toSend.add(filename);

            for(int y=0; y<numTos; y++)
            {
                int portTo = Integer.parseInt(comArgs[j++]);
                tmp.add(portTo);
            }
            commandsToSend.put(filename, tmp);
        }

        j++;

        for(; j<comArgs.length; j++)
        {
            toRemove.add(comArgs[j]);

            if(toSend.contains(comArgs[j])) continue;
            File file = new File(Paths.get(".").toAbsolutePath().normalize().toString() + File.separator +
                    file_folder + File.separator + comArgs[j]);
            file.delete();
        }

        for(int i=0; i<toSend.size(); i++)
        {
            Vector<Integer> ports = commandsToSend.get(toSend.get(i));
            for(int y=0; y<ports.size(); y++)
            {
                new Thread(new DStoreToDStoreThread(ports.get(y), file_folder, toSend.get(i), port, toRemove)).start();
            }
        }

        contrPrOut.println(Protocol.REBALANCE_COMPLETE_TOKEN);
    }

    private void actOnList()
    {
        String message = Protocol.LIST_TOKEN;

        File folder = new File(Paths.get(".").toAbsolutePath().normalize().toString() + File.separator +
                file_folder);
        File[] files = folder.listFiles();
        for(File file : files)
        {
            message += " " + file.getName();
        }

        contrPrOut.println(message);
    }
}
