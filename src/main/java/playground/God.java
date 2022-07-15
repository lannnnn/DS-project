package playground;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.util.Timeout;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static akka.pattern.Patterns.ask;


public class God extends AbstractActor {

    public God(List<ActorRef> Clients, List<ActorRef> L2cs, List<ActorRef> L1cs, ActorRef database, int id){
        // HI lets play a game
        System.out.println("Hi, I'm God!");

        // making task

        List<Object> msg = this.generate_tasks(5,5, 5,5);

        System.out.println("God: generate "+ msg.toArray().length+ " tasks!");
        // setTimeout(20,L1cs.get(0));
        // setTimeout(25,L1cs.get(0));

        Message.CWRITE cwmsg = new Message.CWRITE("5", "-50", Clients.get(0),null,null,true,0);
        Message.CWRITE cwmsg1 = new Message.CWRITE("5", "-60", Clients.get(0),null,null,true,1);
        Message.READ rmsg = new Message.READ("5", null,Clients.get(1), null,null,true,2);
        Message.READ rmsg1 = new Message.READ("5", null,Clients.get(1), null,null,true,3);

        // rmsg.c.tell(rmsg,getSelf());
        // rmsg.c.tell(rmsg1,getSelf());
        // rmsg.c.tell(rmsg,getSelf());
        // rmsg.c.tell(rmsg1,getSelf());
        // rmsg.c.tell(rmsg,getSelf());
        // rmsg.c.tell(rmsg1,getSelf());
        // rmsg.c.tell(rmsg,getSelf());
        // rmsg.c.tell(rmsg1,getSelf());
        // rmsg.c.tell(rmsg,getSelf());
        // rmsg.c.tell(rmsg1,getSelf());
        // rmsg.c.tell(rmsg,getSelf());
        // rmsg.c.tell(rmsg1,getSelf());

        // cwmsg.c.tell(cwmsg,getSelf());
        // rmsg.c.tell(rmsg1,getSelf());
        // cwmsg.c.tell(cwmsg1,getSelf());
        // L1cs.get(0).tell(new Message.CRASH(), getSelf())git ;
        // rmsg.c.tell(rmsg,getSelf());

        // sending tasks to clienst
        // System.out.println(L1cs.get(0).path().name());
        // setTimeout(20,L1cs.get(0));
        // setTimeout(3000,L1cs.get(0));
        // setTimeout(450,L1cs.get(1));
        // setTimeout(1500,L1cs.get(1));
        // setTimeout(1,L2cs.get(1));
        // setTimeout(400,L2cs.get(1));

        // setTimeout(350,L2cs.get(0));
        // setTimeout(1500,L2cs.get(0));
        // L2cs.get(0).tell(new Message.CRASH(),getSelf());
        this.doing_tasks(msg, Clients);
        //ending ask every one to print their log
        try {
            TimeUnit.SECONDS.sleep(12);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        Message.printLogs print_log = new Message.printLogs();
        // print client peration logs
        for(int i = 0; i < Clients.toArray().length; i++){
            Clients.get(i).tell(print_log, getSelf());
        }
        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // print L2 operation logs
        for(int i = 0; i < L2cs.toArray().length; i++){
            L2cs.get(i).tell(print_log, getSelf());
        }
        System.out.println("");
        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println("");
        // print L1 operation logs
        for(int i = 0; i < L1cs.toArray().length; i++){
            L1cs.get(i).tell(print_log, getSelf());
        }
        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println();
        // print DB operation logs
        database.tell(print_log, getSelf());
    }

    static public Props props(List<ActorRef> Clients, List<ActorRef> L2cs, List<ActorRef> L1cs, ActorRef database, int id) {
        return Props.create(God.class, () -> new God(Clients, L2cs,L1cs, database , id));
    }

    private void doing_tasks(List<Object> tasks, List<ActorRef> clients){
        for(int i=0; i < tasks.toArray().length; i++){
            Object msg = tasks.get(i);
            int clients_index = ThreadLocalRandom.current().nextInt(0, clients.toArray().length);
            if(Message.READ.class == msg.getClass()){
                ((Message.READ) msg).c = clients.get(clients_index);
                ((Message.READ) msg).c.tell(((Message.READ) msg),getSelf());
            } else if (Message.WRITE.class == msg.getClass()) {
                ((Message.WRITE) msg).c = clients.get(clients_index);
                ((Message.WRITE) msg).c.tell(((Message.WRITE) msg), getSelf());
            } else if (Message.CREAD.class == msg.getClass()) {
                ((Message.CREAD) msg).c = clients.get(clients_index);
                ((Message.CREAD) msg).c.tell(((Message.CREAD) msg), getSelf());
            } else if (Message.CWRITE.class == msg.getClass()) {
                ((Message.CWRITE) msg).c = clients.get(clients_index);
                ((Message.CWRITE) msg).c.tell(((Message.CWRITE) msg), getSelf());
            }
        }
    }

    private static List<Object> generate_tasks(int n_reads, int n_writes, int n_creads, int n_cwrites){
        List<Object> messageList = new ArrayList<Object>();
        int id = 1000;
        // generate n read operations
        for(int n_r = 0; n_r < n_reads; n_r++){
            int key_to_ask = ThreadLocalRandom.current().nextInt(0, 10 + 1);
            Message.READ MSG = new Message.READ(String.valueOf(key_to_ask),
                    null,
                    null,
                    null,
                    null,
                    true,
                    id++);
            messageList.add(MSG);
        }
        // generate n write operations
        for(int n_w = 0; n_w < n_writes; n_w++){
            int key_to_ask = ThreadLocalRandom.current().nextInt(0, 10 + 1);
            int value_to_write = key_to_ask*key_to_ask-1;
            Message.WRITE MSG = new Message.WRITE(String.valueOf(key_to_ask),
                    String.valueOf(value_to_write),
                    null,
                    null,
                    null,
                    true,
                    id++);
            messageList.add(MSG);
        }
        // generate n critical read operations
        for(int n_cr = 0; n_cr < n_creads; n_cr++){
            int key_to_ask = ThreadLocalRandom.current().nextInt(0, 10 + 1);
            Message.CREAD MSG = new Message.CREAD(String.valueOf(key_to_ask),
                    null,
                    null,
                    null,
                    null,
                    true,
                    id++);
            messageList.add(MSG);
        }
        // generate n critical write operations
        for(int n_cw = 0; n_cw < n_cwrites; n_cw++){
            int key_to_ask = ThreadLocalRandom.current().nextInt(0, 10 + 1);
            int value_to_write = -key_to_ask*key_to_ask*10;
            Message.CWRITE MSG = new Message.CWRITE(String.valueOf(key_to_ask),
                    String.valueOf(value_to_write),
                    null,
                    null,
                    null,
                    true,
                    id++);
            messageList.add(MSG);
        }
        Collections.shuffle(messageList);
        return messageList;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder().match(String.class, s -> System.out.println(s)).build();
    }

    void setTimeout(int time, ActorRef reciver) {
        getContext().system().scheduler().scheduleOnce(
                Duration.create(time, TimeUnit.MILLISECONDS),
                reciver,
                new Message.CRASH(), // the message to send
                getContext().system().dispatcher(), getSelf()
        );
    }
}
