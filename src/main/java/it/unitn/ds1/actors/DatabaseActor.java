package it.unitn.ds1.actors;

import akka.actor.*;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

// database actor, will not crash, save final data
public abstract class DatabaseActor extends BaseActor {
    // database actor parameters

    // database actor constucure
    public DatabaseActor( ) {

    }

    // override
    protected void onAck() {

    }

    // override
    protected void onTimeout() {

    }

    // read operation
    private void onReadMsg() {
        getSender().tell();
    }

    //write operation
    private void onWriteMsg() {
        
    }

    // Here we define the mapping between the received message types
    // and our actor methods
    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(ReadMsg.class,  this::onReadMsg)
            .match(WriteMsg.class,  this::onWriteMsg)
            .build();
  }
}