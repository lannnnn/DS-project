package playground;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import scala.concurrent.duration.Duration;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class Client extends AbstractActor {

    private String MyLog;
    private Boolean sent;
    private List<ActorRef> L2Crefs;
    private final int id;
    private Random rnd = new Random();

    private List<Message> continer;

    private int waitingTime;
    private Object lastMessage;
    private boolean timeoutSend;

    public Client(List<ActorRef> receiverActors, int id) {
        this.L2Crefs = receiverActors;
        this.id = id;
        this.MyLog = getSelf().path().name() + ": ";
        this.continer = new ArrayList<>();
        this.sent = false;
        this.waitingTime = 2500;
        this.lastMessage = null;
        this.timeoutSend = false;

    }

    private void sendReadMessage(Message.READ msg){
        this.sent = true;
        msg.L2.tell(msg, getSelf());
        this.MyLog = this.MyLog + " {R "+msg.key+">"+msg.L2.path().name()+"}";
        this.lastMessage = msg;
        try { Thread.sleep(rnd.nextInt(10)); }
        catch (InterruptedException e) { e.printStackTrace(); }
    }

    private void sendWriteMessage(Message.WRITE msg){
        this.sent = true;
        msg.L2.tell(msg, getSelf());
        this.MyLog = this.MyLog + " {W ("+msg.key+","+msg.value+")>"+msg.L2.path().name()+"}";
        this.lastMessage = msg;
        try { Thread.sleep(rnd.nextInt(10)); }
        catch (InterruptedException e) { e.printStackTrace(); }
    }

    private void sendCReadMessage(Message.CREAD msg){
        this.sent = true;
        msg.L2.tell(msg, getSelf());
        this.MyLog = this.MyLog + " {CR "+msg.key+">"+msg.L2.path().name()+"}";
        this.lastMessage = msg;
        try { Thread.sleep(rnd.nextInt(10)); }
        catch (InterruptedException e) { e.printStackTrace(); }
    }
    private void sendCWriteMessage(Message.CWRITE msg){
        this.sent = true;
        msg.L2.tell(msg, getSelf());
        this.MyLog = this.MyLog + " {CW ("+msg.key+","+msg.value+")>"+msg.L2.path().name()+"}";
        this.lastMessage = msg;
        try { Thread.sleep(rnd.nextInt(10)); }
        catch (InterruptedException e) { e.printStackTrace(); }
    }
    

    static public Props props(List<ActorRef> receiverActor, int id) {
        return Props.create(Client.class, () -> new Client(receiverActor, id));
    }
    private void ReadHandeler(Message.READ msg){
        if(msg.forward){
            if(this.sent || this.timeoutSend){
                this.continer.add(msg);
            }
            else {
                msg.L2 = chooseL2();
                sendReadMessage(msg);
                setTimeout(this.waitingTime, msg);
            }
        }else {
            receiveRead(msg);
        }
    }

    private void WriteHandeler(Message.WRITE msg){
        if(msg.forward){
            if(this.sent || this.timeoutSend){
                this.continer.add(msg);
            }
            else {
                msg.L2 = chooseL2();
                sendWriteMessage(msg);
                setTimeout(this.waitingTime, msg);}
        }else {
            receiveWrite(msg);
        }
    }

    private void CReadHandeler(Message.CREAD msg) {
        if(msg.forward){
            if(this.sent || this.timeoutSend){
                this.continer.add(msg);
            }
            else {
                msg.L2 = chooseL2();
                sendCReadMessage(msg);
                setTimeout(this.waitingTime, msg);}
        }else {
            receiveCRead(msg);
        }
    }

    private void CWriteHandeler(Message.CWRITE msg) {
        if(msg.forward){
            if(this.sent || this.timeoutSend){
                this.continer.add(msg);
            }
            else {
                msg.L2 = chooseL2();
                sendCWriteMessage(msg);
                setTimeout(this.waitingTime, msg);}
        }else {
            receiveCWrite(msg);
        }
    }

    private void receiveRead(Message.READ msg){
        this.MyLog = this.MyLog + " {GR ("+msg.key+","+msg.value+")>"+msg.L2.path().name()+"}";
        this.sent = false;
//        if(!this.continer.isEmpty()){this.nextMessage();}
    }
    private void receiveWrite(Message.WRITE msg){
        this.MyLog = this.MyLog + " {GW ("+msg.key+","+msg.value+")>"+msg.L2.path().name()+"}";
        this.sent = false;
//        if(!this.continer.isEmpty()){this.nextMessage();}
    }
    private void receiveCRead(Message.CREAD msg){
        this.MyLog = this.MyLog + " {GCR ("+msg.key+","+msg.value+")>"+msg.L2.path().name()+"}";
        this.sent = false;
//        if(!this.continer.isEmpty()){this.nextMessage();}
    }
    private void receiveCWrite(Message.CWRITE msg) {
        this.MyLog = this.MyLog + " {GCW ("+msg.key+","+msg.value+")>"+msg.L2.path().name()+"}";
        this.sent = false;
//        if(!this.continer.isEmpty()){this.nextMessage();}
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
                .match(Message.Timeout.class, s -> timeOutCheck())
                .build();

    }

    private void timeOutCheck() {
        if(this.sent){
            System.out.println(getSelf().path().name()+" shit, someone crashed!!" );
            crashHandler();
        }else {
            this.timeoutSend = false;
            if(!this.continer.isEmpty()){this.nextMessage();}

        }
    }

    private void crashHandler() {
        this.MyLog = this.MyLog + " {Parent crashed} ";


        if (this.lastMessage.getClass().equals(Message.READ.class)){//Message.READ.class.equals(s.getClass())) {
            Message.READ msg;
            msg = (Message.READ) this.lastMessage;
            System.out.println(msg.key +" "+ msg.L2.path().name());
            msg.L2 = chooseNewL2(msg.L2);
            sendReadMessage(msg);
            setTimeout(this.waitingTime,msg);


        } else if (this.lastMessage.getClass().equals(Message.WRITE.class)){//Message.WRITE.class.equals(s.getClass())) {
            Message.WRITE msg;
            msg = (Message.WRITE) this.lastMessage;
            System.out.println(msg.key +" "+ msg.L2.path().name());
            msg.L2 = chooseNewL2(msg.L2);
            sendWriteMessage(msg);
            setTimeout(this.waitingTime,msg);

//            ((Message.WRITE) s).L2 = chooseNewL2(((Message.WRITE) s).L2);
//            sendWriteMessage((Message.WRITE) s);
//            setTimeout(this.waitingTime,s);

        } else if (this.lastMessage.getClass().equals(Message.CREAD.class)){//Message.CREAD.class.equals(s.getClass())) {
            Message.CREAD msg;
            msg = (Message.CREAD) this.lastMessage;
            System.out.println(msg.key +" "+ msg.L2.path().name());
            msg.L2 = chooseNewL2(msg.L2);
            sendCReadMessage(msg);
            setTimeout(this.waitingTime,msg);

//            ((Message.CREAD) s).L2 = chooseNewL2(((Message.CREAD) s).L2);
//            sendCReadMessage((Message.CREAD) s);
//            setTimeout(this.waitingTime,s);

        } else if (this.lastMessage.getClass().equals(Message.CWRITE.class)){//Message.CWRITE.class.equals(s.getClass())) {
            Message.CWRITE msg;
            msg = (Message.CWRITE) this.lastMessage;
            System.out.println(msg.key +" "+ msg.L2.path().name());
            msg.L2 = chooseNewL2(msg.L2);
            sendCWriteMessage(msg);
            setTimeout(this.waitingTime,msg);

//            ((Message.CWRITE) s).L2 = chooseNewL2(((Message.CWRITE) s).L2);
//            sendCWriteMessage((Message.CWRITE) s);
//            setTimeout(this.waitingTime, s);
        }
    }

    void setTimeout(int time, Object message) {
        this.timeoutSend = true;
        getContext().system().scheduler().scheduleOnce(
                Duration.create(time, TimeUnit.MILLISECONDS),
                getSelf(),
                new Message.Timeout(), // the message to send
                getContext().system().dispatcher(), getSelf()
        );
    }
    private ActorRef chooseL2(){
        int indx = ThreadLocalRandom.current().nextInt(this.L2Crefs.toArray().length);
        ActorRef L2 =  this.L2Crefs.get(indx);
        return L2;
    }
    private ActorRef chooseNewL2(ActorRef l2){
        int indx = ThreadLocalRandom.current().nextInt(this.L2Crefs.toArray().length);
        ActorRef newL2 = this.L2Crefs.get(indx);
        while(l2 == newL2){
            indx = ThreadLocalRandom.current().nextInt(this.L2Crefs.toArray().length);
            newL2 =  this.L2Crefs.get(indx);
        }
        return newL2;
    }

}
