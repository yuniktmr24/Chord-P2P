package csx55.transport;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;

public class TCPSenderThread implements Runnable {

    protected DataOutputStream dout;

    private LinkedBlockingQueue<byte[]> queue;


    public TCPSenderThread(Socket socket) throws IOException {
        final int defaultQueueSize = 1000;
        this.queue = new LinkedBlockingQueue<>( defaultQueueSize );
        this.dout = new DataOutputStream( socket.getOutputStream() );
    }

    public void sendData(final byte[] data) throws InterruptedException {
        queue.put( data );
    }

    @Override
    public void run() {
        while ( true )
        {
            try
            {
                byte[] data = queue.take();
                int len = data.length;
                dout.writeInt( len );
                dout.write( data, 0, len );
                dout.flush();

            } catch ( InterruptedException | IOException e )
            {
                e.printStackTrace();
            }
        }
    }
}
