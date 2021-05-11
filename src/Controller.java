import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.*;

public class Controller
{
    public static void main(String[] args) throws IOException
    {
        try
        {
            final int cport = Integer.getInteger(args[0]);
            final int R = Integer.getInteger(args[1]);
            final int timeout = Integer.getInteger(args[2]);
            final int rebalance_period = Integer.getInteger(args[3]);

            ServerSocket ss = new ServerSocket(cport);
            ConcurrentLinkedQueue<String[]> commandQueue = new ConcurrentLinkedQueue<String[]>();
            Vector<DStoreIndex> storeIndex = new Vector<DStoreIndex>();
            ConcurrentHashMap<String, FileIndex> fileIndex = new ConcurrentHashMap<String, FileIndex>();
            ConcurrentHashMap<Integer, StreamPair> streamIndex = new ConcurrentHashMap<Integer, StreamPair>();

            for(;;)
            {
                try
                {
                    Socket client = ss.accept();

                    new Thread(new Runnable()
                    {
                        public void run()
                        {
                            try
                            {
                                BufferedReader bfin = new BufferedReader(new InputStreamReader (client.getInputStream()));
                                PrintWriter prout = new PrintWriter(client.getOutputStream(), true);

                                String inpLine;
                                inpLine = bfin.readLine();

                                String[] comArgs = inpLine.split(" ");
                                String command = comArgs[0];

                                /**
                                 * DStore joining
                                 */
                                if(command.equals(Protocol.JOIN_TOKEN))
                                {
                                    DStoreIndex newDStore = new DStoreIndex(Integer.parseInt(comArgs[1]), prout, bfin);
                                    storeIndex.add(newDStore);
                                    streamIndex.put(Integer.parseInt(comArgs[1]), new StreamPair(bfin, prout));
                                    /** Rebalance later */
                                }

                                /** Otherwise it's a client asking for something
                                 *  And in here there will be the while loops on the streams
                                 *  cuz they're all just running in different threads.
                                 *
                                 *  And once the connection is closed from the client's side
                                 *  then it is closed on this, too, and the thread terminates.
                                 *  Beautiful.
                                 * */

                                /**
                                 * Client asks for a list
                                 */
                                else if(command.equals(Protocol.LIST_TOKEN))
                                {
                                    PrintWriter out = new PrintWriter(client.getOutputStream(), true);

                                    if(storeIndex.size() < R)
                                    {
                                        out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                                        client.close();
                                    }

                                    String message = "LIST";
                                    String[] files = fileIndex.keySet().toString().split(", ");

                                    for(String name: files)
                                    {
                                        message += " " + name.substring(1, name.length()-2);
                                    }

                                    out.println(message);

                                    out.close();
                                    client.close();
                                }


                                else System.out.println("unrecognised command");// Will probably have to call logger here


                            }catch(Exception e){System.out.println("Actually something went wrong somewhere");}
                        }
                    }).start();
                }
                catch(Exception e){System.out.println("error "+e);}
            }
        }
        catch(Exception e){System.out.println("error "+e);}
        System.out.println();
    }
}
