package it.unitn.ds1.actors;

import akka.actor.*;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

// client actor, will not crash, can contact with a given L2 cache
public abstract class ClientActor extends BaseActor {
    // client actor parameters
    private ActorRef parent; // L2 cache parent
    private boolean isExecutingRequest = false; // check if the preview request is finished

    // client actor constucure
    public ClientActor( ) {

    }

    static public Props props() {
        return Props.create(ClientActor.class, () -> new ClientActor());
    }

    // override
    protected void onAck() {

    }

    // override
    protected void onTimeout() {

    }

    // Here we define the mapping between the received message types
    // and our actor methods
    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match()
            .match()
            .build();
  }
}