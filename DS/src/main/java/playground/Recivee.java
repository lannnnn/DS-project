package playground;
import akka.actor.Props;
import akka.actor.AbstractActor;


import java.io.Serializable;


public class Recivee extends AbstractActor {

    public static class Hello implements Serializable {
        public final String msg;
        public final int k;
        public Hello(String msg, int k) {
            this.msg = msg;
            this.k = k;
        }
    }

    static public Props props() {
        return Props.create(Recivee.class, () -> new Recivee());
    }

    private void m(Hello s){
        if (s.msg == "hello"){
            System.out.println("Nice, Hello!"+ s.k);
        } else if (s.msg == "boo") {
            getSender().tell(s.k, getSelf());
            System.out.println("not Nice Man!!!" + s.k);
        }
    }


    @Override
    public Receive createReceive() {
        return receiveBuilder().match(Hello.class, s -> m(s)).build();

    }
}
