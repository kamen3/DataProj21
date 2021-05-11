import java.io.BufferedReader;
import java.io.PrintWriter;
import java.util.Vector;

/** Mine */
public class DStoreIndex implements Comparable
{
    private int port;
    private Vector<FileInfoPair> files;
    private PrintWriter printWrOut;
    private BufferedReader buffReadIn;

    public DStoreIndex(int port_, PrintWriter printWrOut_, BufferedReader buffReadIn_)
    {
        port = port_;
        printWrOut = printWrOut_;
        buffReadIn = buffReadIn_;
        files = new Vector<FileInfoPair>();
    }

    public void addFile(FileInfoPair f)
    {
        files.add(f);
    }

    public int getPort()
    {
        return port;
    }

    public int getNumFiles()
    {
        return files.size();
    }

    public Vector<FileInfoPair> getFiles()
    {
        return files;
    }

    public PrintWriter getPrintWrOut()
    {
        return printWrOut;
    }

    public BufferedReader getBuffReadIn()
    {
        return buffReadIn;
    }

    @Override
    public int compareTo(Object o)
    {
        int osize = ((DStoreIndex)o).getNumFiles();

        if(files.size() > osize) return 1;
        if(files.size() < osize) return -1;
        return 0;
    }
}