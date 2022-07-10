package playground;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import javax.swing.*;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import akka.actor.ActorRef;



public class Database extends AbstractActor {
    private HashMap<String, String> data = new HashMap<String, String>();
    private List<ActorRef> L1s = new ArrayList<>();
    private boolean cw_waiting;
    private List<Message> continer;
    private String checkingState;
    private String Mylog;
    private Random rnd = new Random();

    public Database(){
        // generate the fake data
        for (int i = 0; i<11 ;i++){
            this.data.put(String.valueOf(i), String.valueOf(i*i));
        }

        System.out.println(this.data);
        this.cw_waiting = false;
        this.checkingState = "";
        this.Mylog = getSelf().path().name()+": \n";
    }
    static public Props props() {
        return Props.create(Database.class, () -> new Database());
    }

    private void read(Message.READ msg){
        if(cw_waiting){
            continer.add(msg);
        }else {
            msg.value = this.data.get(msg.key);
            msg.forward = false;
            Mylog += " {READ REQURE FROM "+ getSender().path().name() +" FINISHED WITH VALUE ("+msg.key+","+msg.value+")}\n";
            getSender().tell(msg, getSelf());
            try { Thread.sleep(rnd.nextInt(20)); }
            catch (InterruptedException e) { e.printStackTrace(); }
        }
    }

    private void write(Message.WRITE msg){
        if (cw_waiting){
            continer.add(msg);
        }else {
            this.data.put(msg.key, msg.value);
            Mylog += " {WRITE ("+msg.key+","+msg.value+") FROM "+ getSender().path().name() +" FINISHED}\n";
            msg.forward = false;
            msg.done = true;
            // sends the notification of the update to all its L1 caches
            for(int i = 0; i < L1s.toArray().length; i++){
                L1s.get(i).tell(msg,getSelf());
            }
            try { Thread.sleep(rnd.nextInt(20)); }
            catch (InterruptedException e) { e.printStackTrace(); }
        }
    }

    private void cread(Message.CREAD msg) {
        if (cw_waiting){
            continer.add(msg);
        }else {
            msg.value = this.data.get(msg.key);
            msg.forward = false;
            Mylog += " {CRITICAL READ VALUE OF KEY: " + msg.key + " FROM " + getSender().path().name() +"}\n";
            getSender().tell(msg, getSelf());
            try { Thread.sleep(rnd.nextInt(20)); }
            catch (InterruptedException e) { e.printStackTrace(); }
        }

    }
    private void cwrite(Message.CWRITE msg){
        // put all other messages to continer and wait to finish this task!
        //// Is it really necessary? I hold the idea to maintain a black list that the value of key is not reachable
        this.cw_waiting = true;
        this.checkingState = "R";
        // creat cw_check and sent to childs
        Message.CW_check chck = new Message.CW_check(msg);
        for(int i = 0; i < L1s.toArray().length; i++){
            L1s.get(i).tell(chck,getSelf());
        }
    }
    private void checking(Message.CW_check msg) {
        // if R = true
        //send to your childs
        if(msg.R && this.checkingState == "R" && !msg.A){
            this.checkingState = "P";
            sendChecktoChilds(msg);
        }else if (msg.P && this.checkingState == "P" && !msg.A){
            this.checkingState = "C";
            commit(msg);
            sendChecktoChilds(msg);
            this.cw_waiting = false;
        }else {
            sendAbort(msg);
            this.cw_waiting = false;
        }
        // R false change the done to false and send the cw!
        // send abort

        // if p true send to childs
        //p false change the done to false and send the cw!
        // if c is true, change and send to childs

    }

    private void commit(Message.CW_check msg) {
        this.data.put(msg.cwrite.key,msg.cwrite.value);
        msg.cwrite.forward = false;
        msg.cwrite.done = true;
        msg.cwrite.L1.tell(msg.cwrite, getSelf());
    }

    private void sendAbort(Message.CW_check msg) {
        msg.A = true;
        msg.forward = false;
        for(int i = 0; i < L1s.toArray().length; i++){
            L1s.get(i).tell(msg,getSelf());
        }
    }

    private void sendChecktoChilds(Message.CW_check msg) {
        msg.forward = false;
        for(int i = 0; i < L1s.toArray().length; i++){
            L1s.get(i).tell(msg,getSelf());
        }
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
                .match(Message.CW_check.class, s -> checking(s))
                .match(ActorRef.class, s -> addingChild(s))
                .match(Message.printLogs.class, s -> {System.out.println(this.Mylog);})
                .build();
    }
}
