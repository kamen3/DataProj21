import java.io.BufferedReader;
import java.io.PrintWriter;

/** Mine */
public class StreamPair
{
    private BufferedReader bufRd;
    private PrintWriter prntWrt;

    public StreamPair(BufferedReader bufRd_, PrintWriter prntWrt_)
    {
        bufRd = bufRd_;
        prntWrt = prntWrt_;
    }

    public BufferedReader getBufferedReader()
    {
        return bufRd;
    }

    public PrintWriter getPrintWriter()
    {
        return prntWrt;
    }
}