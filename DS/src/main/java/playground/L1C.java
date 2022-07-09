package playground;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import scala.concurrent.duration.Duration;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class L1C extends AbstractActor {
    private Random rnd = new Random();
    private List<ActorRef> L2s = new ArrayList<>();

    private ActorRef parent;
    private final int id;
    private HashMap<String, String> Ldata = new HashMap<String, String>();
    private boolean cw_waiting;
    private List<Message> continer;
    private boolean sent;
    private String MyLog;
    private int child_counter;
    private String check_state;
    private Boolean state;
    private Boolean crash;
    private List<ActorRef> childrenDontKnowImBack;
    private int waitingTime;
    private int counter;


    public L1C(ActorRef receiverActor, int id) {
        this.parent = receiverActor;
        this.id = id;
        this.tell_your_parent(this.parent);
        this.cw_waiting = false;
        this.MyLog = getSelf().path().name() + ": ";
        this.sent = false;
        this.continer = new ArrayList<>();
        this.crash = false;
        this.childrenDontKnowImBack = new ArrayList<>();
        this.waitingTime =  250;
        this.counter = 0;

    }

    private void tell_your_parent(ActorRef receiver){
        receiver.tell(getSelf(), getSelf());
    }

    private void read(Message.READ msg){
        if(!this.crash){
            if(msg.forward){
                if(this.sent){
                    this.continer.add(msg);
                }else {
                    if(this.Ldata.containsKey(msg.key)) {
                        msg.value = this.Ldata.get(msg.key);
                        msg.forward = false;
                        this.MyLog = this.MyLog + " {"+ msg.L2.path().name()+" LR "+msg.value+"}";
                        this.sendMessageR(msg,msg.L2);

                    }else {
                        msg.L1 = getSelf();
                        this.sent = true;
                        this.MyLog = this.MyLog + " {SR "+ msg.L2.path().name()+" "+ msg.key +"> "+this.parent.path().name()+"}";
                        this.sendMessageR(msg,this.parent);
                    }
                }

            }else {

                this.Ldata.put(msg.key, msg.value);
                this.sent = false;
                this.MyLog = this.MyLog + " {GR "+getSender().path().name()+" ("+ msg.key+","+ msg.value+")> "+msg.L2.path().name()+"}";
                this.sendMessageR(msg,msg.L2);
                if(!this.continer.isEmpty()){this.nextMessage();}
            }
        }else {
        }
    }

    private void write(Message.WRITE msg){
        if(!this.crash){
            if(msg.forward){
                if(this.sent){
                    this.continer.add(msg);
                }else {
                    this.sent = true;
                    msg.L1 = getSelf();
                    this.MyLog = this.MyLog + " {SW " + getSender().path().name()+ "(" +msg.key+","+msg.value+")>"+this.parent.path().name()+"}";
                    sendMessageW(msg, this.parent);
                }

            } else {
                if(this.Ldata.containsKey(msg.key)) {
                    this.Ldata.put(msg.key, msg.value);
                    this.MyLog = this.MyLog + "{newData ("+msg.key+","+msg.value+")"+getSender().path().name()+" }";

                }
//                System.out.println(findDifference(this.L2s,this.childrenDontKnowImBack).toArray().length + " ___________ "+ this.L2s.toArray().length);
                List<ActorRef> childrenKnowImBack = new ArrayList<>(findDifference(this.L2s, this.childrenDontKnowImBack));

                for(int i = 0; i < childrenKnowImBack.toArray().length; i++){
                    childrenKnowImBack.get(i).tell(msg,getSelf());
                }
                if(msg.L1 == getSelf()){
                    this.sent = false;
                    this.MyLog = this.MyLog + " {GW "+ getSender().path().name()+" ("+msg.key+","+msg.value+")>"+msg.L2.path().name()+"}";
                    if(!this.continer.isEmpty()){this.nextMessage();}
                }
            }
        }else {

        }
    }

    private void cread(Message.CREAD msg){
        if(!this.crash){
            if(msg.forward){
                if(this.sent){
                    this.continer.add(msg);

                }else {
                    msg.L1 = getSelf();
                    this.sent = true;
                    this.MyLog = this.MyLog + " {SCR "+ msg.L2.path().name()+" "+ msg.key +"> "+this.parent.path().name()+"}";
                    this.sendMessageCR(msg,this.parent);
                }
            }else {
                this.Ldata.put(msg.key, msg.value);
                this.sent = false;
                this.MyLog = this.MyLog + " {GCR "+getSender().path().name()+" ("+ msg.key+","+ msg.value+")> "+msg.L2.path().name()+"}";
                this.sendMessageCR(msg,msg.L2);
//                if(!this.continer.isEmpty()){this.nextMessage();}
            }
        }
    }

    private void cwrite(Message.CWRITE s){
        this.cw_waiting = true;
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
        // pause evrything
        // if !forward
        if(!s.forward){
            if(s.A){
                //un pause! :))
            }
            // send to childs
            child_counter = 0;
            for(int i = 0; i < L2s.toArray().length; i++){
                L2s.get(i).tell(s,getSelf());
            }
            //if forward
        }else {
            child_counter++;
            if (s.A){
                this.parent.tell(s,getSelf());
            }

        }

        //if counting is same as childs
        //set state
        //change the P,R C?
        // send to parent
    }

    public void addingChild(ActorRef receiver){
        this.L2s.add(receiver);
    }

    static public Props props(ActorRef receiverActor, int id) {
        return Props.create(L1C.class, () -> new L1C(receiverActor, id));
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
            this.MyLog += " {CRASH}";
        }else {
            this.crash = false;
            this.Ldata.clear();
            this.continer.clear();
            try { Thread.sleep(rnd.nextInt(100)+250); }
            catch (InterruptedException e) { e.printStackTrace(); }
            this.MyLog += " {BACK}";
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
        this.MyLog += " {Almost children know!}";
    }

    private static <T> Set<T> findDifference(List<T> first, List<T> second)
    {
        Set<T> diff = new HashSet<>(first);
        diff.removeAll(second);
        return diff;
    }




}
