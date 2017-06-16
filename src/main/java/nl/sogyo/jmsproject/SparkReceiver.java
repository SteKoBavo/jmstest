package nl.sogyo.jmsproject;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

public class SparkReceiver extends Receiver<Integer> {
    public SparkReceiver(StorageLevel storageLevel) {
        super(storageLevel);
    }

    @Override
    public void onStart() {
        Thread thread = new Thread(new StreamListener());
        thread.start();
    }

    @Override
    public void onStop() {
    }


    private class StreamListener implements Runnable {
        public void run() {
            try {
                Socket s = new Socket("127.0.0.1", 5083);

                InputStreamReader streamreader = new InputStreamReader(s.getInputStream());
                BufferedReader reader = new BufferedReader(streamreader);

                store(13);

                String test = reader.readLine();
                System.out.println(test);

                reader.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }
}
