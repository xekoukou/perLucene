package perLucene;


import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;


class WatcherLeader implements Watcher
{

    protected ZooTiger tiger;

      WatcherLeader (ZooTiger tiger)
    {
        this.tiger = tiger;

    }

    public void process (WatchedEvent e)
    {

        if (e.getType ().equals (Watcher.Event.EventType.NodeDataChanged)) {

            if (tiger.isLeader ()) {

                System.out.println
                    ("The Leader status changed while I was online");
                System.exit (-1);

            }

            if (tiger.getLeader ()) {

                if (!tiger.isLeader ()) {

                    if (tiger.isSynced () == false) {

                        tiger.sync ();

                    }


                    tiger.connect ();




                }
                else {

                    tiger.bind ();
                }
            }
        }
    }
}
