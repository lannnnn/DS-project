package it.unitn.ds1.actors;

import akka.actor.*;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

// cache actor, may crash
public abstract class CacheActor extends BaseActor {
    // cache actor parameters
    private final Map<Integer, CacheElement> cache;
    private ActorRef parent;
    private List<ActorRef> children;
    private List<ActorRef> clients;
    private ActorRef database;

    // cache actor constucure
    public CacheActor() {
        super();
        cache = new Hashtable<>();

    }

    static public Props props() {
        return Props.create(CacheActor.class, () -> new CacheActor());
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
            .match()
            .build();
    }

    public static class CrashMessage implements Serializable {
        public final Stage stage;
        public final int recoverTime;

        public CrashMessage(Stage stage) {
            this(stage, new Random(System.nanoTime()).nextInt(RECOVERY_MAX_TIME - RECOVERY_MIN_TIME) - RECOVERY_MIN_TIME);
        }

        public CrashMessage(Stage stage, int recoverTime) {
            this.stage = stage;
            this.recoverTime = recoverTime;
        }
    }
}