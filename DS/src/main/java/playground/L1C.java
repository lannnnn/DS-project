package playground;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

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


    public L1C(ActorRef receiverActor, int id) {
        this.parent = receiverActor;
        this.id = id;
        this.tell_your_parent(this.parent);
        this.cw_waiting = false;
        this.MyLog = getSelf().path().name() + ": ";
        this.sent = false;
        this.continer = new ArrayList<>();
    }

    private void tell_your_parent(ActorRef receiver){
        receiver.tell(getSelf(), getSelf());
    }

    private void read(Message.READ s){
        if(s.forward){
            if(this.sent){
                this.continer.add(s);
            }else {
                if(this.Ldata.containsKey(s.key)) {
                    s.value = this.Ldata.get(s.key);
                    s.forward = false;
                    System.out.println(getSelf().path().name()+" yes, I had it!  "+s.value);
                    this.MyLog = this.MyLog + " {"+ s.L2.path().name()+" LR "+s.value+"}";
                    this.sendMessageR(s,s.L2);

                }else {
                    s.L1 = getSelf();
                    this.sent = true;
                    this.MyLog = this.MyLog + " {"+ s.L2.path().name()+" R ("+ s.key+") "+this.parent.path().name()+"}";
                    this.sendMessageR(s,this.parent);
                }
            }

        }else {
            this.Ldata.put(s.key, s.value);
            this.sent = false;
            this.MyLog = this.MyLog + " {"+ getSender().path().name()+" GR ("+ s.key+","+s.value+") "+s.L2.path().name()+"}";
            this.sendMessageR(s,s.L2);
            if(!this.continer.isEmpty()){this.nextMessage();}

        }
    }

    private void write(Message.WRITE s){
        if(s.forward){
            if(this.sent){
                this.continer.add(s);
            }else {
                this.sent = true;
                s.L1 = getSelf();
                sendMessageW(s, this.parent);
            }

        } else {
            if(this.Ldata.containsKey(s.key)) {
                this.Ldata.put(s.key, s.value);
            }
            for(int i = 0; i < L2s.toArray().length; i++){
                L2s.get(i).tell(s,getSelf());
            }
            if(s.L1 == getSelf()){
                this.sent = false;
                if(!this.continer.isEmpty()){this.nextMessage();}
            }

        }
    }

    private void cread(Message.CREAD s){
        if(s.forward){
            if(this.sent){
                this.continer.add(s);

            }else {
                s.L1 = getSelf();
                this.sent = true;
                this.sendMessageCR(s,this.parent);
            }
        }else {
            this.Ldata.put(s.key, s.value);
            this.sent = false;
            this.sendMessageCR(s,s.L2);
            if(!this.continer.isEmpty()){this.nextMessage();}

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

    public void gg(ActorRef receiver){
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
                .match(ActorRef.class, s -> gg(s))
                .build();

    }



}
