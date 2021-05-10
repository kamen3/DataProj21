import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class FTServerMultithreaded
{
    public static void main(String[] args) throws IOException
    {
        try
        {
            ServerSocket ss = new ServerSocket(4323);
            for(;;)
            {
                try
                {
                    System.out.println("waiting for connection");
                    Socket client = ss.accept();

                    new Thread(new Runnable()
                    {
                        public void run()
                        {
                            try
                            {
                                System.out.println("connected");

                                InputStream in = client.getInputStream();
                                byte[] buf = new byte[1000];
                                int buflen;
                                buflen=in.read(buf);
                                String firstBuffer=new String(buf,0,buflen);
                                int firstSpace=firstBuffer.indexOf(" ");
                                String command=firstBuffer.substring(0,firstSpace);
                                System.out.println("command "+command);

                                Thread.sleep(1000);

                                if(command.equals("put"))
                                {
                                    int secondSpace=firstBuffer.indexOf(" ",firstSpace+1);
                                    String fileName=
                                            firstBuffer.substring(firstSpace+1,secondSpace);
                                    System.out.println("fileName "+fileName);

                                    File outputFile = new File(fileName);
                                    FileOutputStream out = new FileOutputStream(outputFile);
                                    out.write(buf,secondSpace+1,buflen-secondSpace-1);

                                    while ((buflen=in.read(buf)) != -1)
                                    {
                                        System.out.print("*");
                                        out.write(buf,0,buflen);
                                    }

                                    in.close();
                                    client.close();
                                    out.close();
                                }


                                else if(command.equals("get"))
                                {
                                    int secondSpace=firstBuffer.indexOf(" ",firstSpace+1);
                                    String fileName=
                                            firstBuffer.substring(firstSpace+1,secondSpace);
                                    System.out.println("fileName "+fileName);

                                    File inputFile = new File(fileName);
                                    OutputStream out = client.getOutputStream();

                                    if(inputFile.exists())
                                    {
                                        FileInputStream inf = new FileInputStream(inputFile);

                                        while ((buflen = inf.read(buf)) != -1)
                                        {
                                            System.out.print("*");
                                            out.write(buf, 0, buflen);
                                        }
                                        inf.close();
                                    }
                                    else out.write("FILE_DOES_NOT_EXIST".getBytes());

                                    in.close();
                                    out.close();
                                    client.close();
                                }


                                else System.out.println("unrecognised command");


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
