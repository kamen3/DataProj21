/** Mine */
public class FileInfoPair
{
    private String filename;
    private int fileLength;

    public FileInfoPair(String filename_, int fileLength_)
    {
        filename = filename_;
        fileLength = fileLength_;
    }

    public String getFilename()
    {
        return filename;
    }

    public int getFileLength()
    {
        return fileLength;
    }
}