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
    private Random rnd = new Random();

    private List<ActorRef> L1s = new ArrayList<>();
    private boolean cw_waiting;
    private List<Message> continer;
    private String checkingState;
    private String Mylog;



    private HashMap<String, String> data = new HashMap<String, String>();

    public Database(){

        for (int i = 0; i<11 ;i++){
            this.data.put(String.valueOf(i), String.valueOf(i*i));
        }

        System.out.println(this.data);
        System.out.println(!false);
        this.cw_waiting = false;
        this.checkingState = "";
        this.Mylog = getSelf().path().name()+": ";
    }
    static public Props props() {
        return Props.create(Database.class, () -> new Database());
    }


    private void read(Message.READ s){
        if(cw_waiting){
            continer.add(s);
        }else {
            s.value = this.data.get(s.key);
            s.forward = false;
            Mylog += " {R"+ getSender().path().name() + ">(" + s.key + ","+s.value+") } ";
            getSender().tell(s, getSelf());
            try { Thread.sleep(rnd.nextInt(20)); }
            catch (InterruptedException e) { e.printStackTrace(); }
        }


    }

    private void write(Message.WRITE s){
        if (cw_waiting){
            continer.add(s);
        }else {
            this.data.put(s.key,s.value);
            Mylog += " {W"+ getSender().path().name() + ">(" + s.key + ","+s.value+") } ";
            s.forward = false;
            s.done = true;
            if(s.L1 != getSender()){
                getSender().tell(s,getSelf());
            }
            for(int i = 0; i < L1s.toArray().length; i++){
                L1s.get(i).tell(s,getSelf());
            }
            try { Thread.sleep(rnd.nextInt(20)); }
            catch (InterruptedException e) { e.printStackTrace(); }
        }


    }

    private void cread(Message.CREAD s) {
        if (cw_waiting){
            continer.add(s);
        }else {
            s.value = this.data.get(s.key);
            s.forward = false;
            Mylog += " {CR"+ getSender().path().name() + ">(" + s.key + ","+s.value+") } ";
            getSender().tell(s, getSelf());
            try { Thread.sleep(rnd.nextInt(20)); }
            catch (InterruptedException e) { e.printStackTrace(); }
        }

    }
    private void cwrite(Message.CWRITE s){
        // put all other messages to continer and wait to finish this task!
        this.cw_waiting = true;
        this.checkingState = "R";
        // creat cw_check and sent to childs
        Message.CW_check chck = new Message.CW_check(s);
        for(int i = 0; i < L1s.toArray().length; i++){
            L1s.get(i).tell(chck,getSelf());
        }
    }
    private void checking(Message.CW_check s) {
        // if R = true
        //send to your childs
        if(s.R && this.checkingState == "R" && !s.A){
            this.checkingState = "P";
            sendChecktoChilds(s);
        }else if (s.P && this.checkingState == "P" && !s.A){
            this.checkingState = "C";
            commit(s);
            sendChecktoChilds(s);
            this.cw_waiting = false;

        }else {
            sendAbort(s);
            this.cw_waiting = false;
        }
        // R false change the done to false and send the cw!
        // send abort

        // if p true send to childs
        //p false change the done to false and send the cw!
        // if c is true, change and send to childs

    }

    private void commit(Message.CW_check s) {
        this.data.put(s.cwrite.key,s.cwrite.value);
        s.cwrite.forward = false;
        s.cwrite.done = true;
        s.cwrite.L1.tell(s.cwrite, getSelf());
    }

    private void sendAbort(Message.CW_check s) {
        s.A = true;
        s.forward = false;
        for(int i = 0; i < L1s.toArray().length; i++){
            L1s.get(i).tell(s,getSelf());
        }
    }

    private void sendChecktoChilds(Message.CW_check s) {
        s.forward = false;
        for(int i = 0; i < L1s.toArray().length; i++){
            L1s.get(i).tell(s,getSelf());
        }
    }


    private void gg(ActorRef receiver){
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
                .match(ActorRef.class, s -> gg(s))
                .match(Message.printLogs.class, s -> {
                    System.out.println(this.Mylog);
                })
                .build();

    }




}
