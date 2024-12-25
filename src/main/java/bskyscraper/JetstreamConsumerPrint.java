package bskyscraper;

import java.util.List;

public class JetstreamConsumerPrint extends JetstreamConsumer<String> {

    public JetstreamConsumerPrint(int numWorkerThreads, int queueBatchSize) {
        super(numWorkerThreads, queueBatchSize);
    }

    @Override
    protected void performIngestionSetup() throws Exception {
    }

    @Override
    protected void ingestBatch(List<String> batch) throws Exception {
        for(var msg : batch) {
            System.out.println(msg);
        }
    }

    @Override
    protected String processRawMessage(String message) throws Exception {
        return message;
    }

}
