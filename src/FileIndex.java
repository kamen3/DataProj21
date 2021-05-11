import java.util.Vector;

/** Mine */
public class FileIndex implements Comparable
{
    private String filename;
    private Integer filesize;
    private Vector<Integer> DStores; // the ports

    public FileIndex(String filename_)
    {
        filename = filename_;
        DStores = new Vector<Integer>();
    }

    public String getFilename()
    {
        return filename;
    }

    @Override
    public int compareTo(Object o)
    {
        String oname = ((FileIndex)o).getFilename();
        int comp = filename.compareTo(oname);

        if(comp == 1) return 1;
        if(comp == -1) return -1;
        return 0;
    }
}