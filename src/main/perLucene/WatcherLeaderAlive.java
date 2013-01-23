package perLucene;


import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;

class WatcherLeaderAlive implements Watcher
{

    protected ZooTiger tiger;

      WatcherLeaderAlive (ZooTiger tiger)
    {
        this.tiger = tiger;

    }

    public void process (WatchedEvent e)
    {
//there are remnant watchers on old leaders
        if (e.getPath ().equals ("/tiger/servers/" +
                                 Integer.toString (tiger.interval) + "/" +
                                 Integer.toString (tiger.leader) + "/up")) {
            if (e.getType ().equals (Watcher.Event.EventType.NodeDeleted)) {

                assert (!tiger.isLeader ());

                tiger.stoppedWorking ();

                if (tiger.isSynced ()) {
                    tiger.becomeLeader ();

                }
            }

            if (e.getType ().equals (Watcher.Event.EventType.NodeCreated)) {

                tiger.leaderExists (true);

                assert (!tiger.isLeader ());

                if (tiger.isSynced () == false) {

                    tiger.sync ();

                }


                tiger.connect ();


            }



        }

    }
}
