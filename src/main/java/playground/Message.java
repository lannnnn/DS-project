package playground;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class Message {

    public Message() {}

    public static class READ extends Message implements Serializable {
        public final int id;
        public final String key;
        public String value;
        public ActorRef c;
        public ActorRef L2;
        public ActorRef L1;
        public Boolean forward;

        public READ(String key, String value, ActorRef c, ActorRef L2, ActorRef L1, Boolean forward, int id) {
            this.key = key;
            this.value = value;
            this.c = c;
            this.L1 = L1;
            this.L2 = L2;
            this.forward = forward;
            this.id = id;
        }
    }

    public static class WRITE extends Message implements Serializable {
        public final int id;
        public final String key;
        public String value;
        public ActorRef c;
        public ActorRef L2;
        public ActorRef L1;
        public boolean forward;
        public boolean done;

        public WRITE(String key, String value, ActorRef c, ActorRef L2, ActorRef L1, boolean forward, int id) {
            this.key = key;
            this.value = value;
            this.c = c;
            this.L1 = L1;
            this.L2 = L2;
            this.forward = forward;
            this.done = false;
            this.id = id;
        }
    }

    public static class CREAD extends Message implements Serializable {
        public final int id;
        public final String key;
        public String value;
        public ActorRef c;
        public ActorRef L2;
        public ActorRef L1;
        public Boolean forward;

        public CREAD(String key, String value, ActorRef c, ActorRef L2, ActorRef L1, Boolean forward, int id) {

            this.key = key;
            this.value = value;
            this.c = c;
            this.L1 = L1;
            this.L2 = L2;
            this.forward = forward;
            this.id = id;
        }
    }

    public static class CWRITE extends Message implements Serializable {
        public final int id;
        public final String key;
        public String value;
        public ActorRef c;
        public ActorRef L2;
        public ActorRef L1;
        public Boolean forward;

        public String done;

        public CWRITE(String key, String value, ActorRef c, ActorRef L2, ActorRef L1, Boolean forward, int id) {

            this.key = key;
            this.value = value;
            this.c = c;
            this.L1 = L1;
            this.L2 = L2;
            this.forward = forward;
            this.done = "";
            this.id = id;
        }
    }

    public static class CW_check implements Serializable{
        public String key;

        public CW_check(String key){
            this.key = key;
        }
    }
    public static class WriteCW implements Serializable{
        public String key;
        public String value;

        public WriteCW(String key, String value){
            this.key = key;
            this.value = value;
        }
    }

    public static class Abort implements Serializable {}
    public static class CWTimeOut implements Serializable {}



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

    public static class Timeout implements Serializable {
        public Timeout(){

        }
    }
    public static class DeleteCache implements Serializable {
        public DeleteCache(){

        }
    }

    public static class CRASH implements Serializable {
        public ActorRef target;
        public CRASH() {

        }
    }

    public static class ImBack implements Serializable {
        ActorRef L1;
        ActorRef L2;
        public ImBack(ActorRef L1, ActorRef L2){
            this.L1 = L1;
            this.L2 = L2;
        }
    }
    public static class CW_ch implements Serializable {}



//    public static class






}
