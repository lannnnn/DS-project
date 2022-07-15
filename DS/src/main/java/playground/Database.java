package playground;

import akka.actor.*;

import javax.swing.*;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import akka.actor.ActorRef;
import scala.concurrent.duration.Duration;


public class Database extends AbstractActor {
    private HashMap<String, String> data = new HashMap<String, String>();
    private List<ActorRef> L1s = new ArrayList<>();
    private boolean cw_waiting;
    private List<Message.CWRITE> continer;
    private String checkingState;
    private String Mylog;
    private Random rnd = new Random();

    private Boolean CWisGoingOn;
    private Message.CWRITE lastCWMessage;
    private int cwWaitingTime;
    private Cancellable timer;

    private List<ActorRef> L1sIsReady;
    private HashMap<Integer, Boolean> timeOutCheck;


    public Database(){
        // generate the fake data
        for (int i = 0; i<11 ;i++){
            this.data.put(String.valueOf(i), String.valueOf(i*i));
        }

        System.out.println(this.data);
        this.CWisGoingOn = false;
        this.checkingState = "";
        this.Mylog = getSelf().path().name()+": \n";
        this.L1sIsReady = new ArrayList<>();
        this.cwWaitingTime = 300;
        this.continer = new ArrayList<>();

    }
    static public Props props() {
        return Props.create(Database.class, () -> new Database());
    }

    private void read(Message.READ msg){
        msg.value = this.data.get(msg.key);
        msg.forward = false;
        Mylog += " {READ REQURE FROM "+ getSender().path().name() +" FINISHED WITH VALUE ("+msg.key+","+msg.value+")}\n";
        getSender().tell(msg, getSelf());
        try { Thread.sleep(rnd.nextInt(20)); }
        catch (InterruptedException e) { e.printStackTrace(); }
    }

    private void write(Message.WRITE msg){
        this.data.put(msg.key, msg.value);
        Mylog += " {WRITE ("+msg.key+","+msg.value+") FROM "+ getSender().path().name() +" FINISHED}\n";
        msg.forward = false;
        msg.done = true;
        // checking if L1 didn't send the message, sending the result to the sender (L2)
        if(msg.L1 != getSender()){
            getSender().tell(msg,getSelf());
        }
        // sends the notification of the update to all its L1 caches
        for(int i = 0; i < L1s.toArray().length; i++){
            L1s.get(i).tell(msg,getSelf());
        }
        try { Thread.sleep(rnd.nextInt(10)); }
        catch (InterruptedException e) { e.printStackTrace(); }

    }

    private void cread(Message.CREAD msg) {
        msg.value = this.data.get(msg.key);
        msg.forward = false;
        Mylog += " {CRITICAL READ VALUE OF KEY: " + msg.key + " FROM " + getSender().path().name() +"}\n";
        getSender().tell(msg, getSelf());
        try { Thread.sleep(rnd.nextInt(10)); }
        catch (InterruptedException e) { e.printStackTrace(); }

    }
    private void cwrite(Message.CWRITE msg){
        if(CWisGoingOn){
            this.continer.add(msg);
        }else {
            if(getSender() != msg.L1){
                msg.forward = false;
                msg.done = "Abort";
                Mylog += " {CWRITE ("+msg.key+","+msg.value+", Abort!) FROM "+ getSender().path().name() +" FINISHED}\n";
                getSender().tell(msg, getSelf());
            }else {
                this.CWisGoingOn = true;
                this.lastCWMessage = new Message.CWRITE(
                        msg.key,
                        msg.value,
                        msg.c,
                        msg.L2,
                        msg.L1,
                        msg.forward,
                        msg.id
                );
                this.L1sIsReady.clear();

                for (int i = 0; i < L1s.toArray().length; i++){
                    Message.CW_check cwCheckmsg = new Message.CW_check(msg.key);
                    L1s.get(i).tell(cwCheckmsg, getSelf());
                }
                setTimeout(this.cwWaitingTime);
                try { Thread.sleep(rnd.nextInt(10)); }
                catch (InterruptedException e) { e.printStackTrace(); }
            }

        }

    }
    private void checking() {
        L1sIsReady.add(getSender());
        // System.out.println(L1sIsReady.toArray().length);
        if(L1sIsReady.toArray().length == L1s.toArray().length){
            L1sIsReady.clear();
            timer.cancel();
            sendWrite();
            this.lastCWMessage.done = "Done!";
            Message.CWRITE cwMsg = this.lastCWMessage;
            cwMsg.forward = false;
            cwMsg.L1.tell(cwMsg, getSelf());
            this.data.put(this.lastCWMessage.key, this.lastCWMessage.value);
            this.lastCWMessage = null;
            Mylog += " {CWRITE ("+cwMsg.key+","+cwMsg.value+", Done!) FROM "+ cwMsg.L1.path().name() +" FINISHED}\n";
            try { Thread.sleep(rnd.nextInt(10)); }
            catch (InterruptedException e) { e.printStackTrace(); }
            this.CWisGoingOn = false;
            if(!continer.isEmpty()){
                Message.CWRITE msg = continer.get(0);
                continer.remove(0);
                cwrite(msg);
            }
        }
        // if list of answers or same as L1s
        // send commit
    }

    private void sendWrite() {
        for (int i = 0; i < L1s.toArray().length; i++){
            Message.WriteCW msg = new Message.WriteCW(this.lastCWMessage.key, this.lastCWMessage.value);
            L1s.get(i).tell(msg, getSelf());
        }
    }

    void setTimeout(int time) {

        timer = getContext().system().scheduler().scheduleOnce(
                Duration.create(time, TimeUnit.MILLISECONDS),
                getSelf(),
                new Message.Timeout(), // the message to send
                getContext().system().dispatcher(), getSelf()
        );
    }


    private void addingChild(ActorRef receiver){
        this.L1s.add(receiver);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Message.READ.class, s -> read(s))
                .match(Message.WRITE.class, s -> write(s))
                .match(Message.CREAD.class, s -> cread(s))
                .match(Message.CWRITE.class, s ->cwrite(s))
                .match(Message.CW_check.class, s -> checking())
                .match(ActorRef.class, s -> addingChild(s))
                .match(Message.Timeout.class, s -> Abort())
                .match(Message.printLogs.class, s -> {System.out.println(this.Mylog);
                    System.out.println(this.data);})
                .build();
    }

    private void Abort() {
        L1sIsReady.clear();
        for (int i = 0; i < L1s.toArray().length; i++){
            Message.Abort msg = new Message.Abort();
            L1s.get(i).tell(msg, getSelf());
        }

        this.lastCWMessage.forward = false;
        this.lastCWMessage.done = "Abort";
        Mylog += " {CWRITE ("+this.lastCWMessage.key+","+this.lastCWMessage.value+", Abort!) FROM "+ this.lastCWMessage.L1.path().name() +" FINISHED}\n";
        this.lastCWMessage.L1.tell(this.lastCWMessage, getSelf());
        this.lastCWMessage = null;

        try { Thread.sleep(rnd.nextInt(10)); }
        catch (InterruptedException e) { e.printStackTrace(); }
        this.CWisGoingOn = false;

        if(!continer.isEmpty()){
            Message.CWRITE newCWmsg = continer.get(0);
            continer.remove(0);
            cwrite(newCWmsg);
        }
    }
}
