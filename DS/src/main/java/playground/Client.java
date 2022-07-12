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

    private final int id;                           // permanant id for visit
    private List<ActorRef> L2Crefs;                 // parent list, one client will have several L2 parents
    private List<Message> continer;                 // queue for received messages
    private List<Boolean> sent;                     // processing state for sent requirements
    
    private Random rnd = new Random();
    private String myLog;
    private int waitingTime;
    private Object lastMessage;

    public Client(List<ActorRef> receiverActors, int id) {
        this.L2Crefs = receiverActors;
        this.id = id;
        this.myLog = getSelf().path().name() + ": \n";
        this.continer = new ArrayList<>();
        this.sent = new ArrayList<>();
        this.waitingTime = 2000;
        this.lastMessage = null;
    }

    private void sendReadMessage(Message.READ msg){
        this.sent.add(true);
        msg.L2.tell(msg, getSelf());
        this.myLog = this.myLog + " {READ VALUE OF KEY: " + msg.key + " FROM " + msg.L2.path().name()+"}\n";
        this.lastMessage = msg;
        try { Thread.sleep(rnd.nextInt(10)); }      // random sleep several seconds        
        catch (InterruptedException e) { e.printStackTrace(); }
    }

    private void sendWriteMessage(Message.WRITE msg){
        this.sent.add(true);
        msg.L2.tell(msg, getSelf());
        this.myLog = this.myLog + " {WRITE ("+msg.key+","+msg.value+") TO "+msg.L2.path().name()+"}\n";
        this.lastMessage = msg;
        try { Thread.sleep(rnd.nextInt(10)); }      // random sleep several seconds  
        catch (InterruptedException e) { e.printStackTrace(); }
    }

    private void sendCReadMessage(Message.CREAD msg){
        this.sent.add(true);
        msg.L2.tell(msg, getSelf());
        this.myLog = this.myLog + " {CRITICAL READ VALUE OF KEY: " + msg.key + " FROM " + msg.L2.path().name()+"}\n";
        this.lastMessage = msg;
        try { Thread.sleep(rnd.nextInt(10)); }      // random sleep several seconds 
        catch (InterruptedException e) { e.printStackTrace(); }
    }
    private void sendCWriteMessage(Message.CWRITE msg){
        this.sent.add(true);
        msg.L2.tell(msg, getSelf());
        this.myLog = this.myLog + " {CRITICAL WRITE ("+msg.key+","+msg.value+") TO "+ msg.L2.path().name()+"}\n";
        this.lastMessage = msg;
        try { Thread.sleep(rnd.nextInt(10)); }      // random sleep several seconds 
        catch (InterruptedException e) { e.printStackTrace(); }
    }
    
    static public Props props(List<ActorRef> receiverActor, int id) {
        return Props.create(Client.class, () -> new Client(receiverActor, id));
    }

    private void ReadHandeler(Message.READ msg){
        // check the direction of the msg(forward to db or backward to client)
        if(msg.forward){                            
            // if is sending message now, just add to container
            if(!this.sent.isEmpty() && this.sent.get(this.sent.toArray().length-1)){
                this.continer.add(msg);
            }   
            else {                                  
                msg.L2 = chooseL2();                // set the target parent of the message    
                sendReadMessage(msg);
                setTimeout(this.waitingTime, msg);
            }
        }else {
            receiveRead(msg);
        }
    }

    private void WriteHandeler(Message.WRITE msg){
        // check the direction of the msg(forward to db or backward to client)
        if(msg.forward){
            // if is sending message now, just add to container
            if(!this.sent.isEmpty() && this.sent.get(this.sent.toArray().length-1)){
                this.continer.add(msg);
            }
            else {
                msg.L2 = chooseL2();                // set the target parent of the message  
                sendWriteMessage(msg);
                setTimeout(this.waitingTime, msg);}
        }else {
            receiveWrite(msg);
        }
    }

    private void CReadHandeler(Message.CREAD msg) {
        // check the direction of the msg(forward to db or backward to client)
        if(msg.forward){
            // if is sending message now, just add to container
            if(!this.sent.isEmpty() && this.sent.get(this.sent.toArray().length-1)){
                this.continer.add(msg);
            }
            else {
                msg.L2 = chooseL2();                // set the target parent of the message 
                sendCReadMessage(msg);
                setTimeout(this.waitingTime, msg);}
        }else {
            receiveCRead(msg);
        }
    }

    private void CWriteHandeler(Message.CWRITE msg) {
        // check the direction of the msg(forward to db or backward to client)
        if(msg.forward){
            // if is sending message now, just add to container
            if(!this.sent.isEmpty() && this.sent.get(this.sent.toArray().length-1)){
                this.continer.add(msg);
            }
            else {
                msg.L2 = chooseL2();                // set the target parent of the message
                sendCWriteMessage(msg);
                setTimeout(this.waitingTime, msg);}
        }else {
            receiveCWrite(msg);
        }
    }

    private void receiveRead(Message.READ msg){
        this.myLog = this.myLog + " {GET READ RESULT ("+msg.key+","+msg.value+") FROM "+ msg.L2.path().name() + "}\n";
        // // change the first true to false
        // for(int i=0; i<this.sent.toArray().length; i++) {
        //     if(this.sent.get(i)){
        //         this.sent.set(i,false);
        //         break;
        //     }
        // }
        if(!this.sent.isEmpty()) this.sent.set(this.sent.toArray().length-1,false);
    }

    private void receiveWrite(Message.WRITE msg){
        this.myLog = this.myLog + " {GET WRITE CERTIFICATE ("+msg.key+","+msg.value+") FROM" + msg.L2.path().name() + "}\n";
        // // change the first true to false
        // for(int i=0; i<this.sent.toArray().length; i++) {
        //     if(this.sent.get(i)){
        //         this.sent.set(i,false);
        //         break;
        //     }
        // }
        if(!this.sent.isEmpty()) this.sent.set(this.sent.toArray().length-1,false);
    }

    private void receiveCRead(Message.CREAD msg){
        this.myLog = this.myLog + " {GET CRITICAL READ RESULT ("+msg.key+","+msg.value+") FROM "+msg.L2.path().name()+"}\n";
        // // change the first true to false
        // for(int i=0; i<this.sent.toArray().length; i++) {
        //     if(this.sent.get(i)){
        //         this.sent.set(i,false);
        //         break;
        //     }
        // }
        if(!this.sent.isEmpty()) this.sent.set(this.sent.toArray().length-1,false);
    }

    private void receiveCWrite(Message.CWRITE msg) {
        this.myLog = this.myLog + " {GET CRITICAL WRITE CERTIFICATE ("+msg.key+","+msg.value+") FROM"+msg.L2.path().name()+"}\n";
        // change the first true to false
        // for(int i=0; i<this.sent.toArray().length; i++) {
        //     if(this.sent.get(i)){
        //         this.sent.set(i,false);
        //         break;
        //     }
        // }
        if(!this.sent.isEmpty()) this.sent.set(this.sent.toArray().length-1,false);
    }

    private void printLog(){
        System.out.println(this.myLog);
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
        if(!this.sent.isEmpty() && this.sent.get(0)){
            crashHandler();
            this.sent.remove(0);
        }else {
            if(!this.sent.isEmpty() && !this.sent.get(0)) {
                this.sent.remove(0);
            }
            if(!this.continer.isEmpty()){this.nextMessage();}
        }
    }

    private void crashHandler() {
        if (this.lastMessage.getClass().equals(Message.READ.class)){
            Message.READ msg = (Message.READ) this.lastMessage;
            this.myLog = this.myLog + " [CRASH!] Parent crash detected from READ key "+ msg.key +" from "+ msg.L2.path().name();
            msg.L2 = chooseNewL2(msg.L2);
            this.myLog = this.myLog + ", redirect parent to " + msg.L2.path().name() + "\n";
            sendReadMessage(msg);
            setTimeout(this.waitingTime,msg);
        } else if (this.lastMessage.getClass().equals(Message.WRITE.class)){
            Message.WRITE msg = (Message.WRITE) this.lastMessage;
            this.myLog = this.myLog + " [CRASH!] Parent crash detected from WRITE key "+ msg.key +" to "+ msg.L2.path().name();
            msg.L2 = chooseNewL2(msg.L2);
            this.myLog = this.myLog + ", redirect parent to " + msg.L2.path().name() + "\n";
            sendWriteMessage(msg);
            setTimeout(this.waitingTime,msg);
        } else if (this.lastMessage.getClass().equals(Message.CREAD.class)){
            Message.CREAD msg = (Message.CREAD) this.lastMessage;
            this.myLog = this.myLog + " [CRASH!] Parent crash detected from CREAD key"+ msg.key +" from "+ msg.L2.path().name();
            msg.L2 = chooseNewL2(msg.L2);
            this.myLog = this.myLog + ", redirect parent to " + msg.L2.path().name() + "\n";
            sendCReadMessage(msg);
            setTimeout(this.waitingTime,msg);
        } else if (this.lastMessage.getClass().equals(Message.CWRITE.class)){
            Message.CWRITE msg = (Message.CWRITE) this.lastMessage;
            this.myLog = this.myLog + " [CRASH!] Parent crash detected from CWRITE key"+ msg.key +" to "+ msg.L2.path().name();
            msg.L2 = chooseNewL2(msg.L2);
            this.myLog = this.myLog + ", redirect parent to " + msg.L2.path().name() + "\n";
            sendCWriteMessage(msg);
            setTimeout(this.waitingTime,msg);
        }
    }

    void setTimeout(int time, Object message) {
        getContext().system().scheduler().scheduleOnce(
                Duration.create(time, TimeUnit.MILLISECONDS),
                getSelf(),
                new Message.Timeout(), // the message to send
                getContext().system().dispatcher(), getSelf()
        );
    }

    // randomly chose a L2 Cache from parent list
    private ActorRef chooseL2(){
        int indx = ThreadLocalRandom.current().nextInt(this.L2Crefs.toArray().length);
        ActorRef L2 = this.L2Crefs.get(indx);
        return L2;
    }

    // chose a new parent from valid parent list
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
