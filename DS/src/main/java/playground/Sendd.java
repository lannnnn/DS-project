package playground;
//package it.unitn.ds1;

import scala.concurrent.duration.Duration;
import java.util.concurrent.TimeUnit;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Cancellable;

import static akka.pattern.Patterns.ask;


public class Sendd extends AbstractActor{

    private ActorRef receiver;
    public Sendd(ActorRef receiver) {
        this.receiver = receiver;preStart();
    }

    static public Props props(ActorRef receiverActor) {
        return Props.create(Sendd.class, () -> new Sendd(receiverActor));
    }

    public void preStart(){
        ask(receiver, "Hello?", 10);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder().match(Integer.class, s -> System.out.println(s*s)).build();
    }
}


//    @Override
//    public Receive createReceive() {
//        return receiveBuilder().build(); // this actor does not handle any incoming messages
//    }