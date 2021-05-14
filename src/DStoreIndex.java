import java.util.concurrent.ConcurrentHashMap;

/** Mine */
public class DStoreIndex implements Comparable
{
    private int port;
    private ConcurrentHashMap<String, Integer> files;
    //private Socket socket; unnecessary - can be looked up in storeIndex using the port number

    public DStoreIndex(int port_)
    {
        port = port_;
        files = new ConcurrentHashMap<String, Integer>();
    }

    public void addFile(String filename, int filesize) { files.put(filename, filesize); }

    public void removeFile(String filename) { files.remove(filename); }

    public int getPort()
    {
        return port;
    }

    public int getNumFiles()
    {
        return files.size();
    }

    public ConcurrentHashMap<String, Integer> getFiles()
    {
        return files;
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