package playground;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import scala.concurrent.duration.Duration;

import java.security.cert.TrustAnchor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class L2C extends AbstractActor {

    private Random rnd = new Random();

    private List<ActorRef> L1s;
    private ActorRef L1;
    private ActorRef OldL1;
    private ActorRef databaseRef;
    private final int id;
    private HashMap<String, String> Ldata = new HashMap<String, String>();
    private boolean cw_waiting;
    private List<Message> continer;
    private boolean sent;
    private boolean timeoutSend;
    private String MyLog;
    private Boolean crash;
    private int waitingTime;
    private Object lastMessage;


    public L2C(List<ActorRef> L1s, ActorRef databaseRef,int id) {
        this.L1s = L1s;
        this.databaseRef = databaseRef;
        this.id = id;
        int indx = ThreadLocalRandom.current().nextInt(0, L1s.toArray().length);
        this.L1 = L1s.get(indx);
        this.OldL1 =  this.L1;
        System.out.println(getSelf().path().name() + " my parent is " + this.L1.path().name());
        this.tell_your_parent(this.L1);
        this.cw_waiting = false;
        this.MyLog = getSelf().path().name() + ": ";
        this.sent = false;
        this.timeoutSend = false;
        this.continer = new ArrayList<>();
        this.crash = false;
        this.waitingTime =  750;
        this.lastMessage = null;

    }

    private void tell_your_parent(ActorRef receiver){
        receiver.tell(getSelf(), getSelf());
    }
    void setTimeout(int time) {
        this.timeoutSend = true;
        getContext().system().scheduler().scheduleOnce(
                Duration.create(time, TimeUnit.MILLISECONDS),
                getSelf(),
                new Message.Timeout(), // the message to send
                getContext().system().dispatcher(), getSelf()
        );
    }

    private void read(Message.READ msg){
        if(!crash){
            if(msg.forward){
                if(this.sent || this.timeoutSend){
                    this.continer.add(msg);
                }else {
                    if(this.Ldata.containsKey(msg.key)) {
                        msg.value = this.Ldata.get(msg.key);
                        msg.forward = false;
                        this.MyLog = this.MyLog + " {"+ msg.c.path().name()+" LR "+msg.value+"}";
                        this.sendMessageR(msg,msg.c);

                    }else {
                        msg.L2 = getSelf();
                        this.sent = true;
                        this.MyLog = this.MyLog + " {SR "+ msg.c.path().name()+" "+ msg.key +"> "+this.L1.path().name()+"}";
                        this.lastMessage = msg;
                        this.sendMessageR((Message.READ) this.lastMessage,this.L1);
                        setTimeout(this.waitingTime);

                    }
                }

            }else {

                this.Ldata.put(msg.key, msg.value);
                this.sent = false;
                this.MyLog = this.MyLog + " {GR "+getSender().path().name()+" ("+ msg.key+","+ msg.value+")> "+msg.c.path().name()+"}";
                this.sendMessageR(msg,msg.c);
//                if(!this.continer.isEmpty()){this.nextMessage();}
            }
        }
    }

    private void write(Message.WRITE msg){
        if(!this.crash){
            if(msg.forward){
                if(this.sent || this.timeoutSend){
                    this.continer.add(msg);
                }else {
                    this.sent = true;
                    msg.L2 = getSelf();
                    this.MyLog = this.MyLog + " {SW ("+msg.key+","+msg.value+")>"+this.L1.path().name()+"}";
                    sendMessageW(msg, this.L1);
                    this.lastMessage = msg;
                    setTimeout(this.waitingTime);
                }
            } else {
                if(this.Ldata.containsKey(msg.key)) {
                    this.Ldata.put(msg.key, msg.value);
                    this.MyLog = this.MyLog + "{newData ("+msg.key+","+msg.value+")"+getSender().path().name()+" }";
                }
                if(msg.L2 == getSelf()){
                    this.sent = false;
                    this.MyLog = this.MyLog + " {GW "+getSender().path().name()+" ("+msg.key+","+msg.value+")>"+msg.c.path().name()+"}";
                    this.sendMessageW(msg,msg.c);
                }

            }
        }
    }

    private void cread(Message.CREAD msg){
        if(!this.crash){
            if(msg.forward){
                if(this.sent || this.timeoutSend){
                    this.continer.add(msg);
                }else {
                    msg.L2 = getSelf();
                    this.sent = true;
                    this.MyLog = this.MyLog + " {SCR "+ msg.c.path().name()+" "+ msg.key +"> "+this.L1.path().name()+"}";
                    this.sendMessageCR(msg,this.L1);
                    this.lastMessage = msg;
                    setTimeout(this.waitingTime);
                }
            }else {
                this.Ldata.put(msg.key, msg.value);
                this.sent = false;
                this.MyLog = this.MyLog + " {GCR "+getSender().path().name()+" ("+ msg.key+","+ msg.value+")> "+msg.c.path().name()+"}";
                this.sendMessageCR(msg,msg.c);

            }
        }
    }
    private void cwrite(Message.CWRITE s){
        System.out.println(getSelf().path().name()+": shit!!!");
        this.sent = true;

    }


    private void nextMessage(){
        Object msg = this.continer.get(0);
        continer.remove(0);
        if(msg.getClass() == Message.READ.class){
            this.read((Message.READ) msg);
        }else if (msg.getClass() == Message.WRITE.class){
            this.write((Message.WRITE) msg);
        } else if (msg.getClass() == Message.CREAD.class) {
            this.cread((Message.CREAD) msg);
        }
    }


///// Sending
    private void sendMessageR(Message.READ message, ActorRef reciver){
        reciver.tell(message, getSelf());
        try { Thread.sleep(rnd.nextInt(10)); }
        catch (InterruptedException e) { e.printStackTrace(); }
    }
    private void sendMessageW(Message.WRITE message, ActorRef reciver){
        reciver.tell(message, getSelf());
        try { Thread.sleep(rnd.nextInt(10)); }
        catch (InterruptedException e) { e.printStackTrace(); }
    }
    private void sendMessageCR(Message.CREAD message, ActorRef reciver){
        reciver.tell(message, getSelf());
        try { Thread.sleep(rnd.nextInt(10)); }
        catch (InterruptedException e) { e.printStackTrace(); }
    }
    private void sendMessageCW(Message.CWRITE message, ActorRef reciver){
        reciver.tell(message, getSelf());
        try { Thread.sleep(rnd.nextInt(10)); }
        catch (InterruptedException e) { e.printStackTrace(); }
    }

    private void checking(Message.CW_check s) {
    }

    static public Props props(List<ActorRef> L1s, ActorRef databaseRef, int id) {
        return Props.create(L2C.class, () -> new L2C(L1s, databaseRef, id));
    }

    private void printLog(){
        System.out.println(this.MyLog);
        System.out.println(getSelf().path().name()+": "+this.Ldata);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Message.READ.class, s -> read(s))
                .match(Message.WRITE.class, s -> write(s))
                .match(Message.CREAD.class, s -> cread(s))
                .match(Message.CWRITE.class, s -> cwrite(s))
                .match(Message.CW_check.class, s -> checking(s))
                .match(Message.printLogs.class, s -> printLog())
                .match(Message.CRASH.class, s -> onCrash())
                .match(Message.ImBack.class, s -> setParent(s)) //I'm back for parent
                .match(Message.Timeout.class, s -> timeOutCheck())
                .build();
    }
    private void doNothing(){}

    private void setParent(Message.ImBack msg) {
        this.L1 = msg.L1;
        msg.L2 = getSelf();
        this.L1.tell(msg, getSelf());
        System.out.println(getSelf().path().name()+" it back!! "+ msg.L1.path().name());
        try { Thread.sleep(rnd.nextInt(10)); }
        catch (InterruptedException e) { e.printStackTrace(); }

    }

    private void timeOutCheck() {
        if(this.sent){
            System.out.println(getSelf().path().name()+"shit, someone crashed!!");
            crashHandler();
        }else {
            this.timeoutSend = false;
            if(!this.continer.isEmpty()){this.nextMessage();}
        }
    }

    private void crashHandler() {
        this.L1 = this.databaseRef;
        System.out.println(this.L1.path().name());
        MyLog += " {Parent crashed}";
        Object msg = this.lastMessage;
        if (Message.READ.class.equals(msg.getClass())) {
            ((Message.READ) msg).L1 = this.L1;
            this.MyLog = this.MyLog + " {SR "+ ((Message.READ) msg).c.path().name()+" "+ ((Message.READ) msg).key +"> "+this.L1.path().name()+"}";
            sendMessageR((Message.READ) msg, ((Message.READ) msg).L1);
            setTimeout(this.waitingTime);

        } else if (Message.WRITE.class.equals(msg.getClass())) {
            this.MyLog = this.MyLog + " {SW ("+ ((Message.WRITE) msg).key+","+ ((Message.WRITE) msg).value+")>"+this.L1.path().name()+"}";
            ((Message.WRITE) msg).L1 = this.L1;
            sendMessageW((Message.WRITE) msg, ((Message.WRITE) msg).L1);
            setTimeout(this.waitingTime);

        } else if (Message.CREAD.class.equals(msg.getClass())) {
            this.MyLog = this.MyLog + " {SCR "+ ((Message.CREAD) msg).c.path().name()+" "+ ((Message.CREAD) msg).key +"> "+this.L1.path().name()+"}";
            ((Message.CREAD) msg).L1 = this.L1;
            sendMessageCR((Message.CREAD) msg, ((Message.CREAD) msg).L1);
            setTimeout(this.waitingTime);

        } else if (Message.CWRITE.class.equals(msg.getClass())) {
            sendMessageCW((Message.CWRITE) msg, this.L1);
            setTimeout(this.waitingTime);
        }
    }


    private void onCrash() {
        if(!this.crash){
            this.crash = true;
            this.MyLog += " {CRASH}";
            System.out.println(getSelf().path().name() +" Crash");
        }else {
            this.L1 = this.OldL1;
            this.crash = false;
            this.sent = false;
            this.Ldata.clear();
            this.continer.clear();
            try { Thread.sleep(rnd.nextInt(200)+300); }
            catch (InterruptedException e) { e.printStackTrace(); }
            this.MyLog += " {BACK}";
        }
    }


}
