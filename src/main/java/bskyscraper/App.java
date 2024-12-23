package bskyscraper;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.WebSocketClient;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.handler.AbstractWebSocketHandler;

import com.fasterxml.jackson.databind.ObjectMapper;



public class App {

    private static final Logger logger = LoggerFactory.getLogger(App.class);
    // private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final int numWorkerThreads = 2;
    private static final int queueBatchSize = 10;


    public static void main(String[] args) {

        CountDownLatch latch = new CountDownLatch(1);
        BlockingQueue<String> messageQueue = new LinkedBlockingQueue<String>();
        ElasticSearchManager esMgr = new ElasticSearchManager();

        try {
            esMgr.performSetup();
        } catch (Exception e) {
            logger.error("Error performing Elasticsearch setup: {}", e);
            latch.countDown();
            return;
        }

        var url = Util.makeJetstreamSubUrl(new String[]{});
        System.out.println(url);

        WebSocketClient wsClient = new StandardWebSocketClient();
        WebSocketHandler wsHandler = new AbstractWebSocketHandler() {
            @Override
            public void afterConnectionEstablished(WebSocketSession session) throws Exception {
                logger.info("Connected to WebSocket server: {}", url);
            }

            @Override
            public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
                logger.info("WebSocket connection closed: {}", status);
            }

            @Override
            protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
                String payload = message.getPayload();

                try {
                    if(Util.keepMessage(payload)) {
                        messageQueue.add(payload);
                    }
                    // JsonNode jsonNode = objectMapper.readTree(payload);
                    // logger.info("Parsed message: {}", jsonNode.toPrettyString());
                    // objectMapper.readValue(payload, JetstreamEvent.class);
                } catch (Exception e) {
                    logger.error("Failed to parse message as JSON: {}", e);
                }
            }
        };
        CompletableFuture<Void> wsFuture = wsClient.execute(wsHandler, url)
        .thenAccept(session -> {
            logger.info("WebSocket session established: {}", session.getId());
        }).exceptionally(ex -> {
            logger.error("Error establishing WebSocket connection: {}", ex);
            latch.countDown();
            return null;
        });

        ExecutorService threadPool = Executors.newFixedThreadPool(numWorkerThreads);
        for(int i = 0; i < numWorkerThreads; i++) {
            threadPool.submit(() -> processMessages(messageQueue, queueBatchSize, esMgr));
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Interruption: {}", e);
            Thread.currentThread().interrupt();
        }
    }

    private static void processMessages(BlockingQueue<String> queue, int batchSize, ElasticSearchManager mgr) {
        List<String> batch = new ArrayList<>();
        while (true) {
            try {
                batch.add(queue.take());
                queue.drainTo(batch, batchSize - 1);
                processBatch(batch, mgr);
                batch.clear();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.info("Worker thread interrupted, shutting down.");
                break;
            } catch (Exception e) {
                logger.error("Error processing batch: {}", e);
                // add messages back, we are trying to ingest em all
                queue.addAll(batch);
            }
        }
    }

    private static void processBatch(List<String> batch, ElasticSearchManager mgr) throws Exception{
        int maxRetries = 4;
        int retryCount = 0;
        int sleepDuration = 5000;
        while (retryCount <= maxRetries) {
            try {
                mgr.insertPayloads(batch);
                break;
            } catch (Exception e) {
                logger.error("Error processing batch: {}", e.getMessage());
                retryCount++;
                if (retryCount > maxRetries) {
                    throw e;
                }

                try {
                    logger.warn("Try {}/{} failed, retrying in {} s", retryCount, maxRetries + 1, sleepDuration/1000);
                    Thread.sleep(sleepDuration);
                    sleepDuration *= 2;
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    logger.error("Retry interrupted: {}", ie.getMessage());
                    throw new RuntimeException("Retry interrupted");
                }
            }

        }
    }
}
