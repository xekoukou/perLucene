package perLucene;


import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;


class WatcherReplicas implements Watcher
{

    protected ZooTiger tiger;

      WatcherReplicas (ZooTiger tiger)
    {
        this.tiger = tiger;

    }
    public void process (WatchedEvent e)
    {
        tiger.findReplicas ();



    }
}
