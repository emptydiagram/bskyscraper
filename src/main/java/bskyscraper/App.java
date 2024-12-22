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

public class App {

    private static final Logger logger = LoggerFactory.getLogger(App.class);
    private static final String jetStreamHost = "jetstream2.us-east.bsky.network";

    public static String subscribeToJetstream(String[] wantedCollections) {
        var urlBuilder = new StringBuilder();
        urlBuilder.append(String.format("wss://%s/subscribe", jetStreamHost));

        if (wantedCollections.length > 0) {
            urlBuilder.append("?");
            boolean firstColl = true;
            for(String coll : wantedCollections) {
                if(firstColl) {
                    firstColl = false;
                } else {
                    urlBuilder.append("&");
                }
                urlBuilder.append(String.format("wantedCollections=%s", coll));
            }
        }
        return urlBuilder.toString();
    }

    public static void main(String[] args) {
        CountDownLatch latch = new CountDownLatch(1);
        var url = subscribeToJetstream(new String[] {"app.bsky.feed.post"});
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
                logger.info("Received message: {}", payload);
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
