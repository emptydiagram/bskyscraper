package bskyscraper;

import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;

public class JetstreamConsumerOpenSearch extends JetstreamConsumer<JsonNode> {
    private final OpenSearchManager esMgr;

    public JetstreamConsumerOpenSearch(int numWorkerThreads, int queueBatchSize, OpenSearchManager esMgr) {
        super(numWorkerThreads, queueBatchSize);
        this.esMgr = esMgr;
    }

    @Override
    protected void performIngestionSetup() throws Exception {
        this.esMgr.performSetup();
    }

    @Override
    protected void ingestBatch(List<JsonNode> batch) throws Exception {
        this.esMgr.insertPayloads(batch);
    }

    @Override
    protected JsonNode processRawMessage(String message) throws Exception {
        JsonNode jsonNode = objectMapper.readTree(message);
        return jsonNode;
    }
}
