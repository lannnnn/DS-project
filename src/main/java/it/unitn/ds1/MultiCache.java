package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.AbstractActor;
import akka.actor.ActorSystem;
import akka.actor.Props;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.lang.Thread;
import java.util.Collections;

import java.io.IOException;
import it.unitn.ds1.actors.CacheActor;
import it.unitn.ds1.actors.ClientActor;
import it.unitn.ds1.actors.DatabaseActor;

public class MultiCache {
  // data define
  // final static int ;
  final static int N_CACHE_1 = 4;  // number of L1 cache
  final static int N_CACHE_2 = 12;  // number of L2 cache

  // function define
  public static void main(String[] args) {
    // Create a database
    ActorRef database = system.actorOf(DatabaseActor.props(), "database");
    // Create cahce1 and put them to a list
    List<ActorRef> cache_1 = new ArrayList<>();
      for (int i=0; i<N_CACHE_1; i++) {
        group.add(system.actorOf(CacheActor.props( ), "cache1_" + i));
    }
    // Create cahce2 and put them to a list
    List<ActorRef> cache_2 = new ArrayList<>();
      for (int i=0; i<N_CACHE_2; i++) {
        group.add(system.actorOf(CacheActor.props( ), "cache2_" + i));
    }
  }
}
  