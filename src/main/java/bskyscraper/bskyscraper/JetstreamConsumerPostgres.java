package bskyscraper.bskyscraper;

import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import bskyscraper.bskyscraper.dto.FollowEvent;
import bskyscraper.bskyscraper.service.FollowRecordService;


@Component
public class JetstreamConsumerPostgres extends JetstreamConsumer<FollowEvent> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final FollowRecordService recordService;

    public JetstreamConsumerPostgres(FollowRecordService recordService, @Value("${app.numWorkerThreads}") int numWorkerThreads,
            @Value("${app.numWorkerThreads}") int queueBatchSize) {
        super(numWorkerThreads, queueBatchSize);
        this.recordService = recordService;
    }

    @Override
    protected void performIngestionSetup() throws Exception {
    }

    @Override
    protected void ingestBatch(List<FollowEvent> batch) throws Exception {
        for (var event : batch) {
            this.recordService.saveEvent(event);
        }

    }

    @Override
    protected FollowEvent processRawMessage(String message) throws Exception {
        var event = this.objectMapper.readValue(message, FollowEvent.class);
        return event;
    }
    
}
