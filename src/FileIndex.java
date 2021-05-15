import java.util.Vector;

/** Mine */
public class FileIndex
{
    private Integer filesize;
    private Vector<Integer> DStores; // the ports

    public FileIndex(int filesize_)
    {
        filesize = filesize_;
        DStores = new Vector<Integer>();
    }

    public Vector<Integer> getDStores()
    {
        return DStores;
    }

    public void addNewDStore(int port)
    {
        DStores.add(port);
    }

    /** Will probably be used in the rebalancing operation, not in the remove,
     * since there we remove the file wholesale, rather than from certain
     * DStores
     * */
    public void removeDStore(Integer port)
    {
        DStores.remove((Object)port);
    }

    public int getFilesize() { return filesize; }
}