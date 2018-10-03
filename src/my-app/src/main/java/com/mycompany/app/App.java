package com.mycompany.app;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.Subscription;

/**
 * Hello world!
 *
 */
public class App 
{
 static final String usageString = "error";
    public static void main(String args[]) {
        String subject;
        int msgCount;
        String server;

        if (args.length == 3) {
            server = args[0];
            subject = args[1];
            msgCount = Integer.parseInt(args[2]);
        } else if (args.length == 2) {
            server = Options.DEFAULT_URL;
            subject = args[0];
            msgCount = Integer.parseInt(args[1]);
        } else {
            usage();
            return;
        }

        try {
            
            Options options = new Options.Builder().server(server).noReconnect().build();
            Connection nc = Nats.connect(options);
            Subscription sub = nc.subscribe(subject);
            nc.flush(Duration.ZERO);

            for(int i=0;i<msgCount;i++) {
                Message msg = sub.nextMessage(Duration.ofHours(1));

                System.out.format("Received message %s on subject %s",
                                        new String(msg.getData(), StandardCharsets.UTF_8), 
                                        msg.getSubject());
            }

            nc.close();
            
        } catch (Exception exp) {
            exp.printStackTrace();
        }
    }

    static void usage() {
        System.err.println(usageString);
        System.exit(-1);
    }
    
}
