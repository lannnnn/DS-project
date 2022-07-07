package playground;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class Client extends AbstractActor {

    private int n_reads;
    private int n_writes;
    private int n_cread;
    private int n_cwrites;
    private String MyLog;
    private Boolean sent;
    private List<ActorRef> L2Crefs;
    private ActorRef receiver;
    private final int id;
    private Random rnd = new Random();

    private List<Message> continer;


    public Client(List<ActorRef> receiverActors, int id) {
        this.L2Crefs = receiverActors;
        this.id = id;
        this.MyLog = getSelf().path().name() + ": ";
        this.continer = new ArrayList<>();
        this.sent = false;

    }

    private void sendReadMessage(Message.READ msg){
        this.sent = true;
        msg.L2.tell(msg, getSelf());
        this.MyLog = this.MyLog + " {R "+msg.key+">"+msg.L2.path().name()+"}";
        try { Thread.sleep(rnd.nextInt(100)); }
        catch (InterruptedException e) { e.printStackTrace(); }
    }

    private void sendWriteMessage(Message.WRITE msg){
        this.sent = true;
        this.MyLog = this.MyLog + " {W ("+msg.key+","+msg.value+")>"+msg.L2.path().name()+"}";
        msg.L2.tell(msg, getSelf());
        try { Thread.sleep(rnd.nextInt(100)); }
        catch (InterruptedException e) { e.printStackTrace(); }
    }

    private void sendCReadMessage(Message.CREAD msg){
        this.sent = true;
        msg.L2.tell(msg, getSelf());
        this.MyLog = this.MyLog + " {CR "+msg.key+">"+msg.L2.path().name()+"}";
        try { Thread.sleep(rnd.nextInt(100)); }
        catch (InterruptedException e) { e.printStackTrace(); }
    }
    

    static public Props props(List<ActorRef> receiverActor, int id) {
        return Props.create(Client.class, () -> new Client(receiverActor, id));
    }
    private void receiveRead(Message.READ msg){
        System.out.println(" I got "+ msg.value);
        this.MyLog = this.MyLog + " {GR ("+msg.key+","+msg.value+")>"+msg.L2.path().name()+"}";
        this.sent = false;
        if(!this.continer.isEmpty()){this.nextMessage();}
    }
    private void receiveWrite(Message.WRITE msg){
        this.MyLog = this.MyLog + " {GW ("+msg.key+","+msg.value+")>"+msg.L2.path().name()+"}";
        this.sent = false;
        if(!this.continer.isEmpty()){this.nextMessage();}


    }
    private void ReadHandeler(Message.READ s){
        if(s.forward){
//            System.out.println(getSelf().path().name()+": I got R "+ s.key+ " and sent it");
            if(sent == false){
                sendReadMessage(s);
            }
            else {
                this.continer.add(s);
            }
        }else {
            receiveRead(s);
        }
    }

    private void WriteHandeler(Message.WRITE s){
        if(s.forward){
            if(sent == false){
                sendWriteMessage(s);
            }
            else {
                this.continer.add(s);
            }

        }else {
            receiveWrite(s);
        }
    }

    private void CReadHandeler(Message.CREAD s) {

        if(s.forward){
            if(sent == false){
                sendCReadMessage(s);
            }
            else {
                this.continer.add(s);
            }

        }else {
            receiveCRead(s);
        }

    }

    private void CWriteHandeler(Message.CWRITE s) {
    }

    private void receiveCRead(Message.CREAD msg){
        System.out.println(" I got "+ msg.value);
        this.MyLog = this.MyLog + " {GCR ("+msg.key+","+msg.value+")>"+msg.L2.path().name()+"}";
        this.sent = false;
        if(!this.continer.isEmpty()){this.nextMessage();}
    }

    private void printLog(){
        System.out.println(this.MyLog);
    }
    private void nextMessage(){
        Object msg = this.continer.get(0);
        continer.remove(0);
        if(msg.getClass() == Message.READ.class){
            ReadHandeler((Message.READ) msg);
        }else if (msg.getClass() == Message.WRITE.class){
            WriteHandeler((Message.WRITE) msg);
        } else if (msg.getClass() == Message.CREAD.class) {
            CReadHandeler((Message.CREAD) msg);
        }
    }

    public Receive createReceive() {
        return receiveBuilder()
                .match(Message.READ.class, s -> ReadHandeler(s))
                .match(Message.WRITE.class, s -> WriteHandeler(s))
                .match(Message.CREAD.class, s -> CReadHandeler(s))
                .match(Message.CWRITE.class, s -> CWriteHandeler(s))
                .match(Message.printLogs.class, s -> printLog())
                .match(String.class, s -> testing(s))
                .build();

    }

    private void testing(String s) {
        System.out.println(s + getSender().path().name());
        getSender().tell("boooo", getSelf());
        this.sent.notify();
    }



}
