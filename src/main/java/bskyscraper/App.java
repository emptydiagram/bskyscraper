package bskyscraper;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.WebSocketClient;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.handler.AbstractWebSocketHandler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


public class App {

    private static final Logger logger = LoggerFactory.getLogger(App.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();


    public static void main(String[] args) {

        CountDownLatch latch = new CountDownLatch(1);

        try {
            ElasticSearchManager esMgr = new ElasticSearchManager();
            esMgr.performSetup();
        } catch (Exception e) {
            logger.error("Error performing Elasticsearch setup: {}", e);
            latch.countDown();
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
                    JsonNode jsonNode = objectMapper.readTree(payload);
                    logger.info("Parsed message: {}", jsonNode.toPrettyString());
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

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Interruption: {}", e);
            Thread.currentThread().interrupt();
        }
    }
}
