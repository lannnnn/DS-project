package playground;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import scala.concurrent.duration.Duration;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class L1C extends AbstractActor {
    private final int id;                                                  // permanant id for visit
    private ActorRef parent;                                               // parent, database 
    private List<ActorRef> L2s = new ArrayList<>();                        // child nodes    
    private List<ActorRef> childrenDontKnowImBack;              

    private HashMap<String, String> Ldata = new HashMap<String, String>();
    private List<Message> continer;
    private boolean cw_waiting;
    private int waitingTime;
    private boolean sent;
    private String MyLog;
    private String check_state;
    private Boolean state;                                                 // state used for critical write
    private Boolean crash;
    private int counter;                                                   // when come back, count the number of attempt to send msg to child(try 3 times here)
    private int child_counter;                                             // count the certificate number from child node for critical write
    private Random rnd = new Random();

    public L1C(ActorRef receiverActor, int id) {
        this.id = id;
        this.parent = receiverActor;
        this.tell_your_parent(this.parent);
        this.cw_waiting = false;
        this.MyLog = getSelf().path().name() + ":\n";
        this.sent = false;
        this.continer = new ArrayList<>();
        this.crash = false;
        this.childrenDontKnowImBack = new ArrayList<>();
        this.waitingTime = 250;
        this.counter = 0;
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
                    // if have the key, return the value, else forward to parent
                    if(this.Ldata.containsKey(msg.key)) {
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
                this.MyLog = this.MyLog + " {BACKWORD READ REQ FROM "+getSender().path().name()+" ("+ msg.key+","+ msg.value+") TO "+msg.L2.path().name()+"}";
                this.sendMessageR(msg,msg.L2);
                if(!this.continer.isEmpty()){this.nextMessage();}
            }
        }else {
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
                // System.out.println(findDifference(this.L2s,this.childrenDontKnowImBack).toArray().length + " ___________ "+ this.L2s.toArray().length);
                List<ActorRef> childrenKnowImBack = new ArrayList<>(findDifference(this.L2s, this.childrenDontKnowImBack));

                for(int i = 0; i < childrenKnowImBack.toArray().length; i++){
                    childrenKnowImBack.get(i).tell(msg,getSelf());
                }
                if(msg.L1 == getSelf()){
                    this.sent = false;
                    this.MyLog = this.MyLog + " {BACKWORD WRITE CERTIFICATION ("+msg.key+","+msg.value+") FROM "+getSender().path().name()+" TO "+msg.L2.path().name()+"}\n";
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
                this.MyLog = this.MyLog + " {BACKWORD CRITICAL READ REQ FROM "+getSender().path().name()+" ("+ msg.key+","+ msg.value+") TO "+msg.L2.path().name()+"}\n";
                this.sendMessageCR(msg,msg.L2);
//                if(!this.continer.isEmpty()){this.nextMessage();}
            }
        }
    }

    private void cwrite(Message.CWRITE s){
        this.cw_waiting = true;

        // checking

        System.out.println(getSelf().path().name()+": shit!!!");
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

    private void checking(Message.CW_check s) {
        // pause evrything (is it really needed?)
        // if !forward
        if(!s.forward){
            if(s.A){
                //un pause! :))
            }

            child_counter = 0;
            for(int i = 0; i < L2s.toArray().length; i++){
                L2s.get(i).tell(s,getSelf());
            }
            //if forward
        }else {
        // ask children to clear the data 
        // add a function to receive the clear certification from child
        // everytime receive the certification, check the count
        // if count(certificate + crash == all child) call another function to change the state
            child_counter++;
            if (s.A){
                this.parent.tell(s,getSelf());
            }
        }
    }

    public void addingChild(ActorRef receiver){
        this.L2s.add(receiver);
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
                .match(Message.CW_check.class, s-> checking(s))
                .match(Message.printLogs.class, s -> printLog())
                .match(Message.CRASH.class, s -> onCrash())
                .match(ActorRef.class, s -> addingChild(s))
                .match(Message.Timeout.class, s -> timeOutCheck())
                .match(Message.ImBack.class, s -> {this.childrenDontKnowImBack.remove(s.L2);})
                .build();

    }

    private void onCrash() {
        if(!this.crash){
            this.crash = true;
            this.MyLog += " {SELF CRASH}\n";
        }else {
            this.crash = false;
            this.Ldata.clear();
            this.continer.clear();
            try { Thread.sleep(rnd.nextInt(100)+250); }
            catch (InterruptedException e) { e.printStackTrace(); }
            this.MyLog += " {RECOVER}\n";
            // tell your childs that you are alive!!
            this.childrenDontKnowImBack = this.L2s;
            tellChildren();
        }
    }

    private void tellChildren() {
        for(int i = 0; i < this.childrenDontKnowImBack.toArray().length; i++){
            Message.ImBack msg = new Message.ImBack(getSelf(), null);
            childrenDontKnowImBack.get(i).tell(msg, getSelf());
            try { Thread.sleep(rnd.nextInt(10)); }
            catch (InterruptedException e) { e.printStackTrace(); }
        }
        setTimeout(this.waitingTime);
    }

    void setTimeout(int time) {
        getContext().system().scheduler().scheduleOnce(
                Duration.create(time, TimeUnit.MILLISECONDS),
                getSelf(),
                new Message.Timeout(), // the message to send
                getContext().system().dispatcher(), getSelf()
        );
    }
    private void timeOutCheck() {
        if(!this.childrenDontKnowImBack.isEmpty() || this.counter > 3){
            tellChildren();
            this.counter++;
        }
        this.MyLog += " {Almost children know!}\n";
    }

    private static <T> Set<T> findDifference(List<T> first, List<T> second)
    {
        Set<T> diff = new HashSet<>(first);
        diff.removeAll(second);
        return diff;
    }
}
