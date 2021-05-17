import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Paths;

public class DStore
{
    public static void main(String[] args) throws IOException
    {
        final int port = Integer.parseInt(args[0]);
        final int cport = Integer.parseInt(args[1]);
        final int timeout = Integer.parseInt(args[2]);
        final String file_folder = args[3];

        DstoreLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL, port);

        File folder = new File(Paths.get(".").toAbsolutePath().normalize().toString() + File.separator + file_folder);
        folder.mkdirs();

        ServerSocket ss = new ServerSocket(port);
        Socket controller = new Socket("localhost", cport);
        PrintWriter contrPrOut = new PrintWriter(controller.getOutputStream(), true);
        BufferedReader contrBfIn = new BufferedReader(new InputStreamReader(controller.getInputStream()));

        new Thread(new DStoreToContrThread(port, cport, timeout, file_folder, controller)).start();

        for(;;)
        {
            try
            {
                Socket client = ss.accept();
                new Thread(new DStoreToClientThread(client, port, cport, timeout, file_folder, contrPrOut, contrBfIn, controller)).start();
            }
            catch(Exception e){}
        }
    }
}
