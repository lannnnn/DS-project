package playground;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class Message {
//    public final int ID;
//
//    public Message(int ID) {
//        this.ID = ID;
//    }



    public Message() {

    }

    public static class READ extends Message implements Serializable {
        public final String key;
        public String value;
        public ActorRef c;
        public ActorRef L2;
        public ActorRef L1;
        public Boolean forward;

        public READ(String key, String value, ActorRef c, ActorRef L2, ActorRef L1, Boolean forward) {

            this.key = key;
            this.value = value;
            this.c = c;
            this.L1 = L1;
            this.L2 = L2;
            this.forward = forward;
        }
    }

    public static class WRITE extends Message implements Serializable {
        public final String key;
        public String value;
        public ActorRef c;
        public ActorRef L2;
        public ActorRef L1;
        public boolean forward;
        public boolean done;

        public WRITE(String key, String value, ActorRef c, ActorRef L2, ActorRef L1, boolean forward) {

            this.key = key;
            this.value = value;
            this.c = c;
            this.L1 = L1;
            this.L2 = L2;
            this.forward = forward;
            this.done = false;
        }
    }


//    public static class WRITE implements Serializable {
//        public final String msg1;
//        public final String msg2;
//
//        public WRITE(String msg1, String msg2) {
//            this.msg2 = msg2;
//            this.msg1 = msg1;
//        }
//    }
public static class CREAD extends Message implements Serializable {
    public final String key;
    public String value;
    public ActorRef c;
    public ActorRef L2;
    public ActorRef L1;
    public Boolean forward;

    public CREAD(String key, String value, ActorRef c, ActorRef L2, ActorRef L1, Boolean forward) {

        this.key = key;
        this.value = value;
        this.c = c;
        this.L1 = L1;
        this.L2 = L2;
        this.forward = forward;
    }
}
    public static class CWRITE extends Message implements Serializable {
        public final String key;
        public String value;
        public ActorRef c;
        public ActorRef L2;
        public ActorRef L1;
        public Boolean forward;
        public Boolean done;

        public CWRITE(String key, String value, ActorRef c, ActorRef L2, ActorRef L1, Boolean forward) {

            this.key = key;
            this.value = value;
            this.c = c;
            this.L1 = L1;
            this.L2 = L2;
            this.forward = forward;
            this.done = false;

        }
    }
    public static class CW_check implements Serializable{
        public boolean R;
        public boolean P;
        public boolean A;
        public CWRITE cwrite;
        public boolean forward;

        public CW_check(CWRITE cwrite){
            this.R = false;
            this.P = false;
            this.A = false;
            this.cwrite = cwrite;
            this.forward = false;
        }

    }
    public static class printLogs implements Serializable {
        public printLogs(){

        }
    }

    public static class TEST implements Serializable {
        public String s;
        public ActorRef sender;
        public TEST(String s, ActorRef sender){
            this.s = s;
            this.sender = sender;
        }
    }
//    public static class Timeout implements Serializable {
//        public Object message;
//        public String type;
//        public Timeout(Object message, String type){
//            this.message = message;
//            this.type = type;
//        }
//    }
    public static class Timeout implements Serializable {

        public Timeout(){

        }
    }

    public static class CRASH implements Serializable {
    }
    public static class ImBack implements Serializable {
        ActorRef L1;
        ActorRef L2;
        public ImBack(ActorRef L1, ActorRef L2){
            this.L1 = L1;
            this.L2 = L2;
        }
    }


//    public static class






}
