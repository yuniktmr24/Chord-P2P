package csx55.transport;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;

public class TCPSenderThread implements Runnable {

    protected ObjectOutputStream dout;

    private LinkedBlockingQueue<Object> queue;


    public TCPSenderThread(Socket socket) throws IOException {
        final int defaultQueueSize = 1000;
        this.queue = new LinkedBlockingQueue<>( defaultQueueSize );
        this.dout = new ObjectOutputStream( socket.getOutputStream() );
    }

    public void sendData(final Object obj) throws InterruptedException {
        queue.put( obj );
    }

    public void sendObject(final Object obj) throws IOException, InterruptedException {
       queue.put(obj);
    }

    @Override
    public void run() {
        while (true) {
            try {
                Object obj = queue.take();
                try {
                    dout.writeObject(obj);
                    dout.flush();
                    //byte[] data = bos.toByteArray();
                    //sendData(data);
//                } finally {
//                    try {
//                        bos.close();
//                    } catch (IOException ex) {
//                        // Log or handle the close exception
//                    }
//                }

                } catch (IOException e) {
                    e.printStackTrace();
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
