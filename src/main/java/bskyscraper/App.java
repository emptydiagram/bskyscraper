package bskyscraper;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class App {

    private static final Logger logger = LoggerFactory.getLogger(App.class);

    private static final int numWorkerThreads = 2;
    private static final int queueBatchSize = 10;


    public static void main(String[] args) {
        var consumer = new JetstreamConsumerPrint(numWorkerThreads, queueBatchSize);
        consumer.start(new String[]{"app.bsky.graph.follow"}, new String[]{"did:plc:ncwcsl2uejt5ci5qgn7spgis"});
    }

}
