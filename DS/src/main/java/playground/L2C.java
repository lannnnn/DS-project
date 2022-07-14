package playground;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
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

    private final int id;                                                  // permanant id for visit
    private ActorRef parent;                                               // current parent, L1 or database
    private ActorRef L1;                                                   // original assigned L1
    private ActorRef databaseRef;

    private HashMap<String, String> Ldata = new HashMap<String, String>(); // cache data table
    private List<Message> continer;                                        // message queue
    private int waitingTime;
    private int deletingTime;
    private boolean cw_waiting;
    private Boolean crash;
    private List<Boolean> sent;                                            // processing state for sent requirements
    private Object lastMessage;
    private boolean timeoutSend;
    private String MyLog;
    private Random rnd = new Random();
    private Cancellable timer;
    private Cancellable CWTimer;
    private int lastMassegeId;
    private Boolean Send;
    private String ignoreKey;
    private int AbortwaitingTime;

    public L2C(List<ActorRef> L1s, ActorRef databaseRef,int id) {
        this.id = id;
        this.databaseRef = databaseRef;
        int indx = ThreadLocalRandom.current().nextInt(0, L1s.toArray().length);
        this.parent = L1s.get(indx);
        this.L1 = this.parent;
        System.out.println(getSelf().path().name() + ": assigned parent : " + this.parent.path().name());
        this.tell_your_parent(this.parent);
        this.cw_waiting = false;
        this.MyLog = getSelf().path().name() + ":\n";
        this.sent = new ArrayList<>();
        this.timeoutSend = false;
        this.continer = new ArrayList<>();
        this.crash = false;
        this.waitingTime = 500;
        this.AbortwaitingTime = 300;
        this.deletingTime = 5000;
        this.lastMessage = null;
        this.Send = false;
        this.ignoreKey = "-1";
        setTimetoDeleteCache(this.deletingTime);

    }

    private void tell_your_parent(ActorRef receiver){
        receiver.tell(getSelf(), getSelf());
    }

    void setTimeout(int time) {
        timer = getContext().system().scheduler().scheduleOnce(
                Duration.create(time, TimeUnit.MILLISECONDS),
                getSelf(),
                new Message.Timeout(), // the message to send
                getContext().system().dispatcher(), getSelf()
        );
    }

    private void read(Message.READ msg){
        // check state, if crashed, do nothing
        if(!this.crash){
            // check the direction of the msg(forward to db or backward to client)
            if(msg.forward){
                // if is sending message now, just add to container
                if(this.Send){//if(!this.sent.isEmpty() && this.sent.get(this.sent.toArray().length-1)){
                    this.continer.add(msg);
                }else {
                    // if have the key, return the value, else forward to parent
                    if(this.Ldata.containsKey(msg.key) && msg.key != this.ignoreKey) {
                        msg.value = this.Ldata.get(msg.key);
                        msg.forward = false;
                        this.MyLog = this.MyLog + " {READ EQURE FROM "+ msg.c.path().name()+" FINISHED WITH VALUE ("+msg.key+","+msg.value+")}\n";
                        this.sendMessageR(msg,msg.c);
                    }else {
                        this.lastMassegeId = msg.id;
                        this.Send = true;
                        msg.L2 = getSelf();
                        this.MyLog = this.MyLog + " {FORWARD READ REQ "+ msg.key +" FROM "+ msg.c.path().name()+" TO "+this.parent.path().name()+"}\n";
                        this.lastMessage = msg;
                        this.sent.add(true);
                        this.Send = true;
                        this.sendMessageR((Message.READ) this.lastMessage,this.parent);
                        setTimeout(this.waitingTime);
                    }
                }
            }else {
                this.Ldata.put(msg.key, msg.value);    // update the data table
                this.MyLog = this.MyLog + " {UPDATE DATA ("+msg.key+","+msg.value+")}\n";
                // change the first true(in design, always the last element)  to false

                this.MyLog = this.MyLog + " {BACKWARD READ REQ FROM "+getSender().path().name()+" ("+ msg.key+","+ msg.value+") TO "+msg.c.path().name()+"}\n";
                this.sendMessageR(msg,msg.c);

                if(this.lastMassegeId == msg.id){
                    this.Send = false;
                    timer.cancel();
                    if(!this.continer.isEmpty()){ nextMessage();}
                }
            }
        }
    }

    private void write(Message.WRITE msg){
        // check state, if crashed, do nothing
        if(!this.crash){
            // check the direction of the msg(forward to db or backward to client)
            if(msg.forward){
                // if is sending message now, just add to container
                if(this.Send){//if(!this.sent.isEmpty() && this.sent.get(this.sent.toArray().length-1)){
                    this.continer.add(msg);
                }else {
                    this.lastMassegeId = msg.id;
                    this.Send = true;

                    msg.L2 = getSelf();
                    this.MyLog = this.MyLog + " {SEND WRITE REQ("+msg.key+","+msg.value+") FROM " +getSender().path().name() + " TO "+ this.parent.path().name() +"}\n";
                    this.sent.add(true);
                    sendMessageW(msg, this.parent);
                    this.lastMessage = msg;
                    setTimeout(this.waitingTime);
                }
            } else {
                if(this.Ldata.containsKey(msg.key)) {       // only the cache contain the data updated
                    this.Ldata.put(msg.key, msg.value);     // update the data table
                    this.MyLog = this.MyLog + " {UPDATE DATA ("+msg.key+","+msg.value+") FROM "+getSender().path().name()+"}\n";
                }
                if(msg.L2 == getSelf()){
                    // change the first true(in design, always the last element) to false
                    this.MyLog = this.MyLog + " {BACKWARD WRITE CERTIFICATION ("+msg.key+","+msg.value+") FROM "+getSender().path().name()+" TO "+msg.c.path().name()+"}\n";
                    this.sendMessageW(msg,msg.c);
                    if(this.lastMassegeId == msg.id){
                        this.Send = false;
                        timer.cancel();
                        if(!this.continer.isEmpty()){ nextMessage();}
                    }
                }

            }
        }
    }

    private void cread(Message.CREAD msg){
        // check state, if crashed, do nothing
        if(!this.crash){
            // check the direction of the msg(forward to db or backward to client)
            // goto database anyway
            if(msg.forward){
                // if is sending message now, just add to container
                if(this.Send){
                    this.continer.add(msg);
                }else {
                    this.lastMassegeId = msg.id;
                    this.Send = true;
                    msg.L2 = getSelf();
                    this.sent.add(true);
                    this.MyLog = this.MyLog + " {FORWARD CRITICAL READ REQ "+ msg.key +" FROM "+ msg.c.path().name()+" TO "+this.parent.path().name()+"}\n";                    this.sendMessageCR(msg,this.parent);
                    this.lastMessage = msg;
                    setTimeout(this.waitingTime);
                }
            }else {
                this.Ldata.put(msg.key, msg.value);  // update the data table
                // change the first true to false
                // change the first true(in design, always the last element)  to false
                this.MyLog = this.MyLog + " {BACKWORD CRITICAL READ REQ FROM "+getSender().path().name()+" ("+ msg.key+","+ msg.value+") TO "+msg.c.path().name()+"}\n";
                this.sendMessageCR(msg,msg.c);
                if(this.lastMassegeId == msg.id){
                    this.Send = false;
                    timer.cancel();
                    if(!this.continer.isEmpty()){ nextMessage();}

                }

            }
        }
    }

    private void cwrite(Message.CWRITE msg){
        // check state, if crashed, do nothing
        if(!this.crash){
            // check the direction of the msg(forward to db or backward to client)
            if(msg.forward){
                // if is sending message now, just add to container
                if(this.Send){
                    this.continer.add(msg);
                }else {
                    this.lastMassegeId = msg.id;
                    this.Send = true;
                    msg.L2 = getSelf();
                    this.MyLog = this.MyLog + " {SEND CWRITE REQ("+msg.key+","+msg.value+") FROM " +getSender().path().name() + " TO "+ this.parent.path().name() +"}\n";
                    this.sent.add(true);
                    sendMessageCW(msg, this.parent);
                    this.lastMessage = msg;
                    setTimeout(this.waitingTime);
                }
            } else {
                if(msg.L2 == getSelf()){
                    // change the first true to false
                    this.MyLog = this.MyLog + " {BACKWORD CWRITE CERTIFICATION ("+msg.key+","+msg.value+", "+msg.done +" ) FROM "+getSender().path().name()+" TO "+msg.c.path().name()+"}\n";
                    this.sendMessageCW(msg,msg.c);
                    if(this.lastMassegeId == msg.id){
                        this.Send = false;
                        timer.cancel();
                        if(!this.continer.isEmpty()){ nextMessage();}
                    }
                }

            }
        }
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
        } else if (msg.getClass() == Message.CWRITE.class) {
            this.cwrite((Message.CWRITE) msg);
        }
    }

    // Sender
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
                .match(Message.printLogs.class, s -> printLog())
                .match(Message.CRASH.class, s -> onCrash())
                .match(Message.ImBack.class, s -> recover(s)) //I'm back for parent
                .match(Message.Timeout.class, s -> crashHandler())
                .match(Message.DeleteCache.class, s-> deteleCache())
                .match(Message.CW_check.class, s-> checking(s))
                .match(Message.WriteCW.class, s-> onWriteCW(s))
                .match(Message.Abort.class, s -> onAbort())
                .match(Message.CWTimeOut.class, s -> CWTimeOut())
                .build();
    }

    private void CWTimeOut() {
//        System.out.println(getSelf().path().name() + "time out CW "+this.crash);
        if(!this.crash){
            if(this.Ldata.containsKey(this.ignoreKey)){
                this.Ldata.remove(this.ignoreKey);
            }
            this.ignoreKey = "-1";
        }
//        System.out.println(this.Ldata);
    }

    private void onAbort() {
        if(!this.crash){
            this.ignoreKey = "-1" ;
            if(CWTimer != null){
                CWTimer.cancel();
            }

        }
    }

    private void checking(Message.CW_check msg) {
        if(!this.crash){
            this.ignoreKey = msg.key;
//            System.out.println(getSelf().path().name() + " ignore key  => "+ this.ignoreKey);
            setCWTimeout(this.AbortwaitingTime);
        }
    }

    private void onWriteCW(Message.WriteCW msg) {

//        System.out.println(getSelf().path().name() + " I have to write it! "+ msg.key+ this.crash);

        if(!this.crash){
//            System.out.println(msg.key+"   "+ this.Ldata);
            if(this.Ldata.containsKey(msg.key)){
                this.Ldata.put(msg.key,msg.value);
//                System.out.println(getSelf().path().name() + " Update ++++++++++   "+ msg.key + " "+ msg.value);
//                System.out.println(this.Ldata);
            }
            this.ignoreKey = "-1";
            CWTimer.cancel();
        }
    }

    void setCWTimeout(int time) {
        CWTimer = getContext().system().scheduler().scheduleOnce(
                Duration.create(time, TimeUnit.MILLISECONDS),
                getSelf(),
                new Message.CWTimeOut(), // the message to send
                getContext().system().dispatcher(), getSelf()
        );
    }

    private void deteleCache() {
        // deleting the oldest data
        if(!Ldata.isEmpty()){
            Ldata.remove(Ldata.values().toArray()[0]);
        }
        setTimetoDeleteCache(this.deletingTime);
    }

    private void setTimetoDeleteCache(int time) {
        getContext().system().scheduler().scheduleOnce(
                Duration.create(time, TimeUnit.MILLISECONDS),
                getSelf(),
                new Message.DeleteCache(), // the message to send
                getContext().system().dispatcher(), getSelf()
        );
    }

    private void recover(Message.ImBack msg) {
        if(!this.crash){
            this.parent = msg.L1;
            Message.ImBack backmsg = new Message.ImBack(this.L1, getSelf());
            this.parent.tell(backmsg, getSelf());
//            System.out.println(backmsg.L1.path().name() + " recovered, back to be the parent of " + getSelf().path().name());
            try { Thread.sleep(rnd.nextInt(10)); }
            catch (InterruptedException e) { e.printStackTrace(); }
        }
    }

    private void crashHandler() {
        if(!this.crash) {
            MyLog += " [CRASH!] " + this.parent.path().name() + " crash/timeout detected, resend the request\n";
            this.parent = this.databaseRef;
            Object msg = this.lastMessage;
            if (Message.READ.class.equals(msg.getClass())) {
                ((Message.READ) msg).L1 = this.databaseRef;//this.parent;
                this.MyLog = this.MyLog + " {RE-FORWARD READ REQ "+ ((Message.READ) msg).key +" FROM "+ ((Message.READ) msg).c.path().name()+" TO "+this.parent.path().name()+"}\n";
                sendMessageR((Message.READ) msg, ((Message.READ) msg).L1);
            } else if (Message.WRITE.class.equals(msg.getClass())) {
                this.MyLog = this.MyLog + " {RE-SEND WRITE REQ("+((Message.WRITE) msg).key+","+((Message.WRITE) msg).value+") TO "+ this.parent.path().name() +"}\n";
                ((Message.WRITE) msg).L1 = this.databaseRef;//this.parent;
                sendMessageW((Message.WRITE) msg, ((Message.WRITE) msg).L1);
            } else if (Message.CREAD.class.equals(msg.getClass())) {
                this.MyLog = this.MyLog + " {RE-FORWARD CRITICAL READ REQ "+ ((Message.CREAD) msg).key +"FROM "+((Message.CREAD) msg).c.path().name()+" TO "+this.parent.path().name()+"}\n";
                ((Message.CREAD) msg).L1 = this.databaseRef;//this.parent;
                sendMessageCR((Message.CREAD) msg, ((Message.CREAD) msg).L1);
            } else if (Message.CWRITE.class.equals(msg.getClass())) {
                ((Message.CWRITE) msg).L1 = this.databaseRef;//this.parent;
                sendMessageCW((Message.CWRITE) msg, this.parent);
            }
            setTimeout(this.waitingTime);
        }

    }

    private void onCrash() {
        // first receive: crash, second receive: recover
        if(!this.crash){
            this.crash = true;
            this.MyLog += " {SELF CRASH}\n";
            this.parent = this.L1;
            this.Send = false;
            this.ignoreKey = "-1";
            this.Ldata.clear();
            this.continer.clear();

        }else {

            try { Thread.sleep(rnd.nextInt(200)+300); }
            catch (InterruptedException e) { e.printStackTrace(); }
            this.MyLog += " {RECOVER} \n";
            this.timeoutSend = false;
            this.crash = false;
        }
    }
}