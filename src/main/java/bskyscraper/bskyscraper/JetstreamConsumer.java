package bskyscraper.bskyscraper;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.WebSocketClient;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.handler.AbstractWebSocketHandler;

public abstract class JetstreamConsumer<TMessage>  {
    private static final Logger logger = LoggerFactory.getLogger(JetstreamConsumer.class);

    protected final CountDownLatch latch = new CountDownLatch(1);
    protected final BlockingQueue<TMessage> messageQueue = new LinkedBlockingQueue<>();
    protected ExecutorService threadPool;
    protected final int numWorkerThreads;
    protected final int queueBatchSize;

    public JetstreamConsumer(int numWorkerThreads, int queueBatchSize) {
        this.numWorkerThreads = numWorkerThreads;
        this.queueBatchSize = queueBatchSize;
    }

    protected abstract void performIngestionSetup() throws Exception;

    protected abstract void ingestBatch(List<TMessage> batch) throws Exception;

    protected abstract TMessage processRawMessage(String message) throws Exception;

    public void start(String[] wantedCollections, String[] wantedDids) {
        try {
            performIngestionSetup();
        } catch (Exception e) {
            logger.error("Error performing ingestion setup: {}", e);
            latch.countDown();
            return;
        }

        String url = Util.makeJetstreamSubUrl(wantedCollections, wantedDids);

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
                    if (Util.keepMessage(payload)) {
                        messageQueue.add(processRawMessage(payload));
                    }
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

        threadPool = Executors.newFixedThreadPool(numWorkerThreads);
        for (int i = 0; i < numWorkerThreads; i++) {
            threadPool.submit(() -> processMessages());
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Interruption: {}", e);
            Thread.currentThread().interrupt();
        } finally {
            shutdown();
        }
    }

    private void processMessages() {
        List<TMessage> batch = new ArrayList<>();
        while (true) {
            try {
                batch.add(this.messageQueue.take());
                messageQueue.drainTo(batch, queueBatchSize - 1);
                this.processBatch(batch);
                batch.clear();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.info("Worker thread interrupted, shutting down.");
                break;
            } catch (Exception e) {
                logger.error("Error ingesting batch: {}", e);
                this.messageQueue.addAll(batch);
            }
        }
    }

    private void processBatch(List<TMessage> batch) throws Exception{
        int maxRetries = 4;
        int retryCount = 0;
        int sleepDuration = 5000;
        while (retryCount <= maxRetries) {
            try {
                this.ingestBatch(new ArrayList<>(batch));
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


    // Method to shutdown resources
    protected void shutdown() {
        if (threadPool != null && !threadPool.isShutdown()) {
            threadPool.shutdown();
            try {
                if (!threadPool.awaitTermination(60, TimeUnit.SECONDS)) {
                    threadPool.shutdownNow();
                }
            } catch (InterruptedException ex) {
                threadPool.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        // Additional cleanup if necessary
    }
}
