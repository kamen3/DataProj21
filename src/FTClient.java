import java.io.*;
import java.net.Socket;

public class FTClient
{
    public static void main(String[] args) throws IOException
    {
        System.out.println(args[0]+" "+args[1]+" "+args[2]+" "+args[3]);
        final int port = Integer.getInteger(args[0]);
        final int cport = Integer.getInteger(args[1]);
        final int timeout = Integer.getInteger(args[2]);
        final String file_folder = args[3];

        if(args[1].equals("put"))
        {
            File inputFile = new File(args[2]);
            FileInputStream in = new FileInputStream(inputFile);
            try
            {
                Socket socket = new Socket(args[0],4323);

                OutputStream out = socket.getOutputStream();
                out.write(("put"+" "+args[3]+" ").getBytes());

                byte[] buf = new byte[1000];
                int buflen;
                while ((buflen=in.read(buf)) != -1)
                {
                    System.out.print("*");
                    out.write(buf,0,buflen);
                }
                out.close();
            }
            catch(Exception e){System.out.println("error"+e);}
            System.out.println();
            in.close();
        }
        else
        if(args[1].equals("get"))
        {
            File outputFile = new File(args[2]);
            FileOutputStream outf = new FileOutputStream(outputFile);
            try
            {
                Socket socket = new Socket(args[0],4323);

                OutputStream out = socket.getOutputStream();
                InputStream in = socket.getInputStream();
                out.write(("get"+" "+args[3]+" ").getBytes());
                byte[] buf = new byte[1000];
                int buflen;

                while ((buflen=in.read(buf)) != -1)
                {
                    String write = new String(buf, 0 ,buflen);

                    if(write.equals("FILE_DOES_NOT_EXIST")) throw new Exception("Error: File does not exist");

                    System.out.print("*");
                    outf.write(buf,0,buflen);
                }
                out.close();
                in.close();
            }
            catch(Exception e){System.out.println("error"+e);}
            System.out.println();
            outf.close();
        }
        else System.out.println("unrecognised command");
    }
}
