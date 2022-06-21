package it.unitn.ds1.actors;

import akka.actor.*;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

// basic abstracted actor for client, cache and database actor,
// basically handling common issues like timeout, ack etc.
public abstract class BaseActor extends AbstractActor {
    // base actor parameters
    public static final int TIMEOUT = 1000;
    public static final int RECOVERY_MIN_TIME = 3000;
    public static final int RECOVERY_MAX_TIME = 10000;

    public enum Stage {
        Read, Write, Refill, Remove, Result
    }

    // base actor constucure
    public BaseActor( ) {

    }

    // functions to be overwrite by successors
    protected abstract void onAck();
    protected abstract void onTimeout();

    private void onAckMsg( ) {
        // some logical option...

        //
        onAck();
    }

    private void onTimeoutMsg( ) {
        // some logical option...

        //
        onTimeout();
    }

    // Here we define the mapping between the received message types
    // and our actor methods
    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(AckMsg.class,  this::onAckMsg)
            .match(TimeoutMsg.class,  this::onTimeoutMsg)
            .build();
  }
}