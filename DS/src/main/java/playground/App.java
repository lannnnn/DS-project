package playground;

// system

//sender

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import akka.actor.AbstractActor;
import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;


public class App {
    final static int n_client = 2;
    final static int n_l2 = 2;
    final static int n_l1 = 1;


    public static void main(String[] args) {
        // Create an actor system named "helloakka"
        final ActorSystem system = ActorSystem.create("test1");

        List<ActorRef> l2s = new ArrayList<>();
        List<ActorRef> l1s = new ArrayList<>();
        List<ActorRef> cs = new ArrayList<>();

        int id_counter = 0;

        // Create a single Database actor
        final ActorRef DB = system.actorOf(
                Database.props(),
                "DB"
        );

        // Create Level 1 cache actors
        for(int i = 0; i<n_l1; i++){
            l1s.add(system.actorOf(
                    L1C.props(DB,id_counter++),    // actor class
                    "L1_"+i    // the new actor name (unique within the system)
            ));
        }

        // Create Level 2 cache actors
        for(int i = 0; i<n_l2; i++){
            l2s.add(system.actorOf(
                    L2C.props(l1s,id_counter++),    // actor class
                    "L2_"+i    // the new actor name (unique within the system)
            ));
        }

        // Create client actors
        for(int i=0; i < n_client; i++){
            cs.add(system.actorOf(
                    Client.props(l2s,id_counter++),    // actor class
                    "C_"+i     // the new actor name (unique within the system)
            ));
        }

        final ActorRef GD = system.actorOf(
                God.props(cs,l2s,l1s,DB,100),
                "God"
        );





        System.out.println(">>> Press ENTER to exit <<<");
        try {
            System.in.read();
        }
        catch (IOException ioe) {}
        finally {
            system.terminate();
        }
    }




}
