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

        List<Object> msg = this.generate_tasks(10,3, 5,L2cs);
        System.out.println("God: generate "+ msg.toArray().length+ " tasks!");

        // sending tasks to clienst
//        System.out.println(L1cs.get(0).path().name());
        setTimeout(1500,L1cs.get(0));
        setTimeout(3000,L1cs.get(0));
        setTimeout(450,L1cs.get(1));
        setTimeout(1500,L1cs.get(1));
        setTimeout(250,L2cs.get(1));
        setTimeout(700,L2cs.get(1));

        setTimeout(350,L2cs.get(0));
        // setTimeout(1500,L2cs.get(0));

        this.doing_tasks(msg, Clients);

        //ending ask every one to print their log

        try {
            TimeUnit.SECONDS.sleep(12);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        Message.printLogs print_log = new Message.printLogs();
        for(int i = 0; i < Clients.toArray().length; i++){

            Clients.get(i).tell(print_log, getSelf());
        }
        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

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
        for(int i = 0; i < L1cs.toArray().length; i++){

            L1cs.get(i).tell(print_log, getSelf());
        }

        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        System.out.println();
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
            }

        }

    }


    private static List<Object> generate_tasks(int n_reads, int n_writes, int n_creads, List<ActorRef> Lc2ref){
        List<Object> messageList = new ArrayList<Object>();
        for(int n_r = 0; n_r < n_reads; n_r++){

            int key_to_ask = ThreadLocalRandom.current().nextInt(0, 10 + 1);
            int indx = ThreadLocalRandom.current().nextInt(0, Lc2ref.toArray().length);

            Message.READ MSG = new Message.READ(String.valueOf(key_to_ask),
                    null,
                    null,
                    null,
                    null,
                    true);
            messageList.add(MSG);
        }
        for(int n_w = 0; n_w < n_writes; n_w++){

            int key_to_ask = ThreadLocalRandom.current().nextInt(0, 10 + 1);
            int value_to_write = key_to_ask*key_to_ask-1;

            int indx = ThreadLocalRandom.current().nextInt(0, Lc2ref.toArray().length);
            Message.WRITE MSG = new Message.WRITE(String.valueOf(key_to_ask),
                    String.valueOf(value_to_write),
                    null,
                    null,
                    null,
                    true);
            messageList.add(MSG);
        }
        for(int n_r = 0; n_r < n_creads; n_r++){

            int key_to_ask = ThreadLocalRandom.current().nextInt(0, 10 + 1);

            int indx = ThreadLocalRandom.current().nextInt(0, Lc2ref.toArray().length);
            Message.CREAD MSG = new Message.CREAD(String.valueOf(key_to_ask),
                    null,
                    null,
                    null,
                    null,
                    true);
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
