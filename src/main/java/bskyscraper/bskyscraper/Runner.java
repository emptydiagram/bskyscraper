package bskyscraper.bskyscraper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("!test")
public class Runner implements CommandLineRunner {
    private final JetstreamConsumerPostgres consumer;

    @Autowired
    public Runner(JetstreamConsumerPostgres consumer) {
        this.consumer = consumer;
    }

    @Override
    public void run(String... args) throws Exception {
        this.consumer.start(new String[]{"app.bsky.graph.follow"}, null);
    }
    
}
