package playground;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import scala.concurrent.duration.Duration;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class L1C extends AbstractActor {
    private final int id;                                                  // permanant id for visit
    private ActorRef parent;                                               // parent, database 
    private List<ActorRef> L2s;                      // child nodes
    private List<ActorRef> childrenDontKnowImBack;
    private HashMap<String, String> Ldata = new HashMap<String, String>();
    private List<Message> continer;
    private int waitingTime;
    private int deletingTime;
    private boolean sent;                                                  // state whether is forward msg
    private String MyLog;
    private Boolean crash;
    private int counter;                                                   // when come back, count the number of attempt to send msg to child(try 3 times here)
    private int child_counter;                                             // count the certificate number from child node for critical write
    private Random rnd = new Random();
    private String ignorKey;                                               // black list for critical write
    private Cancellable timer;

    public L1C(ActorRef receiverActor, int id) {
        this.id = id;
        this.parent = receiverActor;
        this.tell_your_parent(this.parent);
        this.MyLog = getSelf().path().name() + ":\n";
        this.sent = false;
        this.continer = new ArrayList<>();
        this.crash = false;
        this.childrenDontKnowImBack = new ArrayList<>();
        this.waitingTime = 250;
        this.counter = 0;
        this.deletingTime = 5000;
        setTimetoDeleteCache(this.deletingTime);
        this.ignorKey = "-1";       
        this.L2s = new ArrayList<ActorRef>();
    }

    private void tell_your_parent(ActorRef receiver){
        receiver.tell(getSelf(), getSelf());
    }

    private void read(Message.READ msg){
        // check state, if crashed, do nothing
        if(!this.crash){
            // check the direction of the msg(forward to db or backward to client)
            if(msg.forward){
                // if is sending message now, just add to container
                if(this.sent){
                    this.continer.add(msg);
                }else {
                    // if have the key(and not in black list), return the value, else forward to parent
                    if(this.Ldata.containsKey(msg.key) && msg.key != this.ignorKey) {
                        msg.value = this.Ldata.get(msg.key);
                        msg.forward = false;
                        this.MyLog = this.MyLog + " {READ EQURE FROM "+ msg.L2.path().name()+" FINISHED WITH VALUE ("+msg.key+","+msg.value+")}\n";
                        this.sendMessageR(msg,msg.L2);

                    }else {
                        msg.L1 = getSelf();
                        this.sent = true;
                        this.MyLog = this.MyLog + " {FORWARD READ REQ "+ msg.key +" FROM "+ msg.L2.path().name()+" TO "+this.parent.path().name()+"}\n";
                        this.sendMessageR(msg,this.parent);
                    }
                }
            }else {
                this.Ldata.put(msg.key, msg.value);  // update the data table
                this.MyLog = this.MyLog + " {INSERT DATA ("+msg.key+","+msg.value+")}\n";
                this.sent = false;
                this.MyLog = this.MyLog + " {BACKWARD READ REQ FROM "+getSender().path().name()+" ("+ msg.key+","+ msg.value+") TO "+msg.L2.path().name()+"}";
                this.sendMessageR(msg,msg.L2);
                if(!this.continer.isEmpty()){this.nextMessage();}
            }
        }
    }

    private void write(Message.WRITE msg){
        // check state, if crashed, do nothing
        if(!this.crash){
            // check the direction of the msg(forward to db or backward to client)
            if(msg.forward){
                // if is sending message now or timeout, just add to container
                if(this.sent){
                    this.continer.add(msg);
                }else {
                    this.sent = true;
                    msg.L1 = getSelf();
                    this.MyLog = this.MyLog + " {SEND WRITE REQ("+msg.key+","+msg.value+") FROM " +getSender().path().name() + " TO "+ this.parent.path().name() +"}\n";
                    sendMessageW(msg, this.parent);
                }
            } else {
                if(this.Ldata.containsKey(msg.key)) {         // only the cache contain the data updated
                    this.Ldata.put(msg.key, msg.value);       // update the data table
                    this.MyLog = this.MyLog + " {UPDATE DATA ("+msg.key+","+msg.value+") FROM "+getSender().path().name()+"}\n";
                }

                List<ActorRef> childrenKnowImBack = new ArrayList<>(findDifference(this.L2s, this.childrenDontKnowImBack));
                for(int i = 0; i < childrenKnowImBack.toArray().length; i++){
                    childrenKnowImBack.get(i).tell(msg,getSelf());
                }
                if(msg.L1 == getSelf()){
                    this.sent = false;
                    this.MyLog = this.MyLog + " {BACKWARD WRITE CERTIFICATION ("+msg.key+","+msg.value+") FROM "+getSender().path().name()+" TO "+msg.L2.path().name()+"}\n";
                    if(!this.continer.isEmpty()){this.nextMessage();}
                }
            }
        }
    }

    private void cread(Message.CREAD msg){
        // check state, if crashed, do nothing
        if(!this.crash){
            // check the direction of the msg(forward to db or backward to client)
            if(msg.forward){
                // if is sending message now or timeout, just add to container
                if(this.sent){
                    this.continer.add(msg);
                }else {
                    msg.L1 = getSelf();
                    this.sent = true;
                    this.MyLog = this.MyLog + " {FORWARD CRITICAL READ REQ "+ msg.key +"FROM "+ msg.L2.path().name()+" TO "+this.parent.path().name()+"}\n";
                    this.sendMessageCR(msg,this.parent);
                }
            }else {
                this.Ldata.put(msg.key, msg.value);     // update the data table
                this.sent = false;
                this.MyLog = this.MyLog + " {BACKWARD CRITICAL READ REQ FROM "+getSender().path().name()+" ("+ msg.key+","+ msg.value+") TO "+msg.L2.path().name()+"}\n";
                this.sendMessageCR(msg,msg.L2);
                if(!this.continer.isEmpty()){this.nextMessage();}
            }
        }
    }

    private void cwrite(Message.CWRITE msg){
        // check state, if crashed, do nothing
        if(!this.crash){
            // check the direction of the msg(forward to db or backward to client)
            if(msg.forward){
                // if is sending message now or timeout, just add to container
                if(this.sent){
                    this.continer.add(msg);
                }else {

                    this.sent = true;
                    msg.L1 = getSelf();
                    this.MyLog = this.MyLog + " {SEND CWRITE REQ("+msg.key+","+msg.value+") FROM " +getSender().path().name() + " TO "+ this.parent.path().name() +"}\n";
                    sendMessageCW(msg, this.parent);
                }
            } else {
                this.sent = false;
                this.MyLog = this.MyLog + " {BACKWARD CWRITE CERTIFICATION ("+msg.key+","+msg.value+", "+msg.done +") FROM "+getSender().path().name()+" TO "+msg.L2.path().name()+"}\n";
                this.sendMessageCW(msg,msg.L2);
                if(!this.continer.isEmpty()){this.nextMessage();}
            }
        }
    }


    private void nextMessage(){
        Object msg = this.continer.get(0);
        this.continer.remove(0);
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


    private void sendMessageR(Message.READ message, ActorRef messageReciver){
        messageReciver.tell(message, getSelf());
        try { Thread.sleep(rnd.nextInt(10)); }
        catch (InterruptedException e) { e.printStackTrace(); }
    }

    private void sendMessageW(Message.WRITE message, ActorRef messageReciver){
        messageReciver.tell(message, getSelf());
        try { Thread.sleep(rnd.nextInt(10)); }
        catch (InterruptedException e) { e.printStackTrace(); }
    }

    private void sendMessageCR(Message.CREAD message, ActorRef messageReciver){
        messageReciver.tell(message, getSelf());
        try { Thread.sleep(rnd.nextInt(10)); }
        catch (InterruptedException e) { e.printStackTrace(); }
    }
    private void sendMessageCW(Message.CWRITE message, ActorRef messageReciver){
        messageReciver.tell(message, getSelf());
        try { Thread.sleep(rnd.nextInt(10)); }
        catch (InterruptedException e) { e.printStackTrace(); }
    }


    private void checking(Message.CW_check msg) {
       if(!this.crash){
           // ignore the key,
           this.ignorKey = msg.key;
           // System.out.println(getSelf().path().name()+ " Check children num "+ this.L2s.toArray().length + " " + this.childrenDontKnowImBack.toArray().length);
           
           // send the message to all children
           for (int i = 0; i < L2s.toArray().length; i++){
               L2s.get(i).tell(msg, getSelf());
           }
           // System.out.println(getSelf().path().name() + "goftam check konan "+ ignorKey);
           try { Thread.sleep(rnd.nextInt(10)); }
           catch (InterruptedException e) { e.printStackTrace(); }

           this.parent.tell(msg, getSelf());

           try { Thread.sleep(rnd.nextInt(10)); }
           catch (InterruptedException e) { e.printStackTrace(); }
       }
    }
    
    private void onWriteCW(Message.WriteCW msg){
        if(!this.crash){
            // System.out.println(getSelf().path().name()+ " children num "+ this.L2s.toArray().length);
            for (int i = 0; i < this.L2s.toArray().length; i++){
                this.L2s.get(i).tell(msg, getSelf());
                // System.out.println(getSelf().path().name()+ " send write> "+L2s.get(i).path().name());
            }

            if(this.Ldata.containsKey(msg.key)){
                this.Ldata.put(msg.key,msg.value);
                this.MyLog +=  " {UPDATE DATA ("+msg.key+","+msg.value+") CW }\n";
            }
            this.ignorKey = "-1";
        }
    }

    private void onAbort(){
        if(!this.crash){
            Message.Abort msg = new Message.Abort();
            for (int i = 0; i < L2s.toArray().length; i++){
                L2s.get(i).tell(msg, getSelf());
            }
            this.ignorKey = "-1";
        }
    }

    public void addingChild(ActorRef receiver){
        this.L2s.add(receiver);
        printL();
    }

    static public Props props(ActorRef receiverActor, int id) {
        return Props.create(L1C.class, () -> new L1C(receiverActor, id));
    }
    private void printLog(){
        System.out.println(this.MyLog);
        System.out.println(getSelf().path().name()+" data table: "+this.Ldata);
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
                .match(ActorRef.class, s -> addingChild(s))
                .match(Message.Timeout.class, s -> tellChildren())
                .match(Message.ImBack.class, s -> updateLostChildren(s))
                .match(Message.DeleteCache.class, s-> deteleCache())
                .match(Message.CW_check.class, s-> checking(s))
                .match(Message.WriteCW.class, s-> onWriteCW(s))
                .match(Message.Abort.class, s -> onAbort())
                .build();

    }

    private void updateLostChildren(Message.ImBack msg){
        this.childrenDontKnowImBack.remove(this.childrenDontKnowImBack.indexOf(msg.L2));
        // System.out.println(getSelf().path().name()+" YOU back ==> "+ msg.L2.path().name() + " || they Dont know =>"+ printL());
        if (this.childrenDontKnowImBack.isEmpty()) {
            this.timer.cancel();
            this.MyLog += " {Children know!}\n";
            // System.out.println(getSelf().path().name()+" DoNE!!!!!");
        }
    }
    private void deteleCache() {
        // deleting the first the oldest data
        if(!Ldata.isEmpty()){
            Ldata.remove(Ldata.values().toArray()[0]);
        }
        setTimetoDeleteCache(this.deletingTime);
    }

    private void onCrash() {
        if(!this.crash){
            this.crash = true;
            this.MyLog += " {SELF CRASH}\n";
            this.Ldata.clear();
            this.continer.clear();
            this.ignorKey = "-1";
        }else {
            this.crash = false;
            try { Thread.sleep(rnd.nextInt(100)+250); }
            catch (InterruptedException e) { e.printStackTrace(); }
            this.MyLog += " {RECOVER}\n";
            // tell your childs that you are alive!!
            this.childrenDontKnowImBack = this.L2s;
            if(!this.L2s.isEmpty()){
                tellChildren();
            }

        }
    }

    private void tellChildren() {
        // printL();
        // System.out.println(getSelf().path().name()+" they dont know => " + printL());
        for(int i = 0; i < this.childrenDontKnowImBack.toArray().length; i++){
            Message.ImBack msg = new Message.ImBack(getSelf(), null);
            this.childrenDontKnowImBack.get(i).tell(msg, getSelf());
            // System.out.println(getSelf().path().name()+" Im back ==> "+ this.childrenDontKnowImBack.get(i).path().name());
        }
        setTimeout(this.waitingTime);
        try { Thread.sleep(rnd.nextInt(10)); }
        catch (InterruptedException e) { e.printStackTrace(); }
    }

    void setTimeout(int time) {
        this.timer = getContext().system().scheduler().scheduleOnce(
                Duration.create(time, TimeUnit.MILLISECONDS),
                getSelf(),
                new Message.Timeout(), // the message to send
                getContext().system().dispatcher(), getSelf()
        );
    }


    private void setTimetoDeleteCache(int time) {
        getContext().system().scheduler().scheduleOnce(
                Duration.create(time, TimeUnit.MILLISECONDS),
                getSelf(),
                new Message.DeleteCache(), // the message to send
                getContext().system().dispatcher(), getSelf()
        );
    }


    private static <T> Set<T> findDifference(List<T> first, List<T> second)
    {
        Set<T> diff = new HashSet<>(first);
        diff.removeAll(second);
        return diff;
    }
    private String printL(){
        String s = getSelf().path().name();
        for (int i = 0;i < this.childrenDontKnowImBack.toArray().length ;i++){
            s += " "+this.childrenDontKnowImBack.get(i).path().name();
        }
        return s;
    }
}
