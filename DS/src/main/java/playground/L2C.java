package playground;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class L2C extends AbstractActor {

    private Random rnd = new Random();

    private List<ActorRef> L1s;
    private ActorRef parent;
    private final int id;
    private HashMap<String, String> Ldata = new HashMap<String, String>();
    private boolean cw_waiting;
    private List<Message> continer;
    private boolean sent;
    private String MyLog;

    public L2C(List<ActorRef> L1s, int id) {
        this.L1s = L1s;
        this.id = id;
        int indx = ThreadLocalRandom.current().nextInt(0, L1s.toArray().length);
        this.parent = L1s.get(indx);
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
                    this.MyLog = this.MyLog + " {"+ s.c.path().name()+" LR "+s.value+"}";
                    this.sendMessageR(s,s.c);

                }else {
                    s.L2 = getSelf();
                    this.sent = true;
                    this.MyLog = this.MyLog + " {"+ s.c.path().name()+" R ("+ s.key+") "+this.parent.path().name()+"}";
                    this.sendMessageR(s,this.parent);
                }
            }

        }else {
            this.Ldata.put(s.key, s.value);
            this.sent = false;
            this.MyLog = this.MyLog + " {"+ getSender().path().name()+" GR ("+ s.key+","+s.value+") "+s.c.path().name()+"}";
            this.sendMessageR(s,s.c);
            if(!this.continer.isEmpty()){this.nextMessage();}
        }
    }

    private void write(Message.WRITE s){
        if(s.forward){
            if(this.sent){
                this.continer.add(s);
            }else {
                this.sent = true;
                s.L2 = getSelf();
                sendMessageW(s, this.parent);
            }

        } else {
            if(this.Ldata.containsKey(s.key)) {
                this.Ldata.put(s.key, s.value);
            }
            if(s.L2 == getSelf()){
                this.sent = false;
                this.sendMessageW(s,s.c);
                if(!this.continer.isEmpty()){this.nextMessage();}

            }
        }
    }

    private void cread(Message.CREAD s){
        if(s.forward){
            if(this.sent){
                this.continer.add(s);
            }else {
                s.L2 = getSelf();
                this.sent = true;
                this.sendMessageCR(s,this.parent);
            }
        }else {
            this.Ldata.put(s.key, s.value);
            this.sent = false;
            this.sendMessageCR(s,s.c);
            if(!this.continer.isEmpty()){this.nextMessage();}

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
    }

    static public Props props(List<ActorRef> L1s, int id) {
        return Props.create(L2C.class, () -> new L2C(L1s, id));
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
                .build();

    }

}
