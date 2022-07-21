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

    public God(List<ActorRef> Clients, List<ActorRef> L2cs, List<ActorRef> L1cs, ActorRef database, String scenario){
        // HI lets play a game
        System.out.println("Hi, I'm God!");

        if (scenario == "write_L1_crash_tasks") {
            write_L1_crash_tasks(Clients,L1cs);
        } else if (scenario == "cwrite_L1_crash_tasks") {
            cwrite_L1_crash_tasks(Clients,L1cs);
        } else {
            List<Object> msg = this.generate_tasks(5,5, 5,5);
            System.out.println("God: generate "+ msg.toArray().length+ " tasks!");
            this.doing_tasks(msg, Clients);
        }
        //ending ask every one to print their log
        try {
            TimeUnit.SECONDS.sleep(10);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        Message.printLogs print_log = new Message.printLogs();
        // print client peration logs
        for(int i = 0; i < Clients.toArray().length; i++){
            Clients.get(i).tell(print_log, getSelf());
        }
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // print L2 operation logs
        for(int i = 0; i < L2cs.toArray().length; i++){
            L2cs.get(i).tell(print_log, getSelf());
        }
        System.out.println("");
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println("");
        // print L1 operation logs
        for(int i = 0; i < L1cs.toArray().length; i++){
            L1cs.get(i).tell(print_log, getSelf());
        }
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println();
        // print DB operation logs
        database.tell(print_log, getSelf());
    }

    static public Props props(List<ActorRef> Clients, List<ActorRef> L2cs, List<ActorRef> L1cs, ActorRef database, String scenario) {
        return Props.create(God.class, () -> new God(Clients, L2cs,L1cs, database , scenario));
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
            Message.READ MSG = new Message.READ(String.valueOf(key_to_ask), null, null, null, null, true, id++);
            messageList.add(MSG);
        }
        // generate n write operations
        for(int n_w = 0; n_w < n_writes; n_w++){
            int key_to_ask = ThreadLocalRandom.current().nextInt(0, 10 + 1);
            int value_to_write = key_to_ask*key_to_ask-1;
            Message.WRITE MSG = new Message.WRITE(String.valueOf(key_to_ask), String.valueOf(value_to_write),
                    null, null, null, true, id++);
            messageList.add(MSG);
        }
        // generate n critical read operations
        for(int n_cr = 0; n_cr < n_creads; n_cr++){
            int key_to_ask = ThreadLocalRandom.current().nextInt(0, 10 + 1);
            Message.CREAD MSG = new Message.CREAD(String.valueOf(key_to_ask), null, null, null, null, true, id++);
            messageList.add(MSG);
        }
        // generate n critical write operations
        for(int n_cw = 0; n_cw < n_cwrites; n_cw++){
            int key_to_ask = ThreadLocalRandom.current().nextInt(0, 10 + 1);
            int value_to_write = -key_to_ask*key_to_ask*10;
            Message.CWRITE MSG = new Message.CWRITE(String.valueOf(key_to_ask), String.valueOf(value_to_write),
                    null, null, null,  true, id++);
            messageList.add(MSG);
        }
        Collections.shuffle(messageList);
        return messageList;
    }

    private void write_L1_crash_tasks(List<ActorRef> Clients, List<ActorRef> L1cs){
        int id = 1000;
        // generate n read operations all read the same data
        int key_to_ask = ThreadLocalRandom.current().nextInt(0, 10 + 1);
        for(int n_r = 0; n_r < Clients.toArray().length; n_r++){
            Message.READ MSG = new Message.READ(String.valueOf(key_to_ask), null, Clients.get(n_r), null, null, true, id++);
            this.SendMessage(50, Clients.get(n_r), MSG);
        }
        // first L1 crash msg
        this.SendMessage(100, L1cs.get(0), new Message.CRASH());
        // a client write a new value
        this.SendMessage(110, Clients.get(0), new Message.WRITE(String.valueOf(key_to_ask), "1234", Clients.get(0), null, null, true, id++));

        // generate n read operations all read the same data
        for(int n_r = 0; n_r < Clients.toArray().length; n_r++){
            Message.READ MSG = new Message.READ(String.valueOf(key_to_ask), null, Clients.get(n_r), null, null, true, id++);
            this.SendMessage(130, Clients.get(n_r), MSG);
        }

        // generate n cread operations all read the same data
        for(int n_r = 0; n_r < Clients.toArray().length; n_r++){
            Message.CREAD MSG = new Message.CREAD(String.valueOf(key_to_ask), null, Clients.get(n_r), null, null, true, id++);
            this.SendMessage(150, Clients.get(n_r), MSG);
        }
    }

    private void cwrite_L1_crash_tasks(List<ActorRef> Clients, List<ActorRef> L1cs){
        int id = 1000;
        // generate n read operations all read the same data
        int key_to_ask = ThreadLocalRandom.current().nextInt(0, 10 + 1);
        for(int n_r = 0; n_r < Clients.toArray().length; n_r++){
            Message.READ MSG = new Message.READ(String.valueOf(key_to_ask), null, Clients.get(n_r), null, null, true, id++);
            this.SendMessage(50, Clients.get(n_r), MSG);
        }
        // first L1 crash msg
        this.SendMessage(100, L1cs.get(0), new Message.CRASH());
        // a client critically write a new value
        this.SendMessage(110, Clients.get(0), new Message.CWRITE(String.valueOf(key_to_ask), "1234", Clients.get(0), null, null, true, id++));

        // generate n read operations all read the same data
        for(int n_r = 0; n_r < Clients.toArray().length; n_r++){
            Message.READ MSG = new Message.READ(String.valueOf(key_to_ask), null, Clients.get(n_r), null, null, true, id++);
            this.SendMessage(130, Clients.get(n_r), MSG);
        }

        // generate n cread operations all read the same data
        for(int n_r = 0; n_r < Clients.toArray().length; n_r++){
            Message.CREAD MSG = new Message.CREAD(String.valueOf(key_to_ask), null, Clients.get(n_r), null, null, true, id++);
            this.SendMessage(150, Clients.get(n_r), MSG);
        }
    }

    private void SendMessage(int time, ActorRef reciver, Object msg) {
        getContext().system().scheduler().scheduleOnce(
                Duration.create(time, TimeUnit.MILLISECONDS),
                reciver,
                msg, // the message to send
                getContext().system().dispatcher(), getSelf()
        );

    }

    @Override
    public Receive createReceive() {
        return receiveBuilder().match(String.class, s -> System.out.println(s)).build();
    }

    void setTimeout(int time, ActorRef reciver, Object msg) {
        getContext().system().scheduler().scheduleOnce(
                Duration.create(time, TimeUnit.MILLISECONDS),
                reciver,
                new Message.CRASH(), // the message to send
                getContext().system().dispatcher(), getSelf()
        );
    }

}