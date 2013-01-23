package perLucene;


/*
    Copyright contributors as noted in the AUTHORS file.
                
    This file is part of PLATANOS.

    PLATANOS is free software; you can redistribute it and/or modify it under
    the terms of the GNU Affero General Public License as published by
    the Free Software Foundation; either version 3 of the License, or
    (at your option) any later version.
            
    PLATANOS is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.
        
    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.document.TextField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongDocValuesField;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;


import java.util.HashMap;
import java.io.IOException;
import java.util.HashSet;

import org.jeromq.ZMQ;
import org.jeromq.ZContext;
import org.jeromq.ZFrame;
import org.jeromq.ZMsg;
import org.jeromq.ZMQ.Msg;
import org.jeromq.ZMQ.PollItem;
import org.jeromq.ZMQ.Socket;


import java.nio.ByteBuffer;


//Each index replica sends the last id<->gdid that it has indexed
//when it commits


class IndexerThread implements Runnable
{

    protected IndexWriter w;
    protected HashMap < String, Analyzer > ha;
    protected ZContext ctx;
    protected Socket sindex;
    protected Socket scommands;
    protected Socket ssync;
    protected Socket sbroker;

    protected String location;
    protected int replica;      //used to set the id of the dealer socket

    protected HashSet hIds;     //the ids of the replicas used by the router
//when this replica is the leader

    protected boolean isLeader;

      IndexerThread (IndexWriter w, HashMap < String, Analyzer > hAnalyzer,
                     String location, int replica)
    {
        this.w = w;
        this.ha = ha;
        this.location = location;
    }

//Add spatial search support
    private void addDoc (String language,
                         String summary, String text,
                         long uid, long date) throws IOException
    {
        Analyzer analyzer = ha.get (language);

        Document doc = new Document ();
          doc.add (new TextField ("summary", summary, Field.Store.NO));
          doc.add (new TextField ("text", text, Field.Store.NO));
          doc.add (new LongDocValuesField ("uid", uid));
          doc.add (new StringField ("language", language, Field.Store.NO));
          doc.add (new LongField ("date", date, Field.Store.NO));
          w.addDocument (doc, analyzer);
    }


    private void connect (String location)
    {

//free the hashset in case you were the leader
//this is probably unnecessary
        hIds = null;

        isLeader = false;

        if (sindex == null) {
            sindex.close ();
        }
        sindex = ctx.createSocket (ZMQ.DEALER);

        sindex.setIdentity (ByteBuffer.allocate (4).putInt (replica).array ());

        String address = "tcp://" + location + ":49000";


        sindex.connect (address);



        if (scommands == null) {
            scommands.close ();
        }
        scommands = ctx.createSocket (ZMQ.DEALER);

        scommands.setIdentity (ByteBuffer.allocate (4).putInt (replica).array ());

        address = "tcp://" + location + ":49003";


        scommands.connect (address);


    }


    private void updateHIds ()
    {


    }


    private void softSync ()
    {


    }

    private void Sync ()
    {


    }


    private void bind (String location)
    {

        isLeader = true;

        if (sindex == null) {
            sindex.close ();
        }
        sindex = ctx.createSocket (ZMQ.ROUTER);

        String address = "tcp://" + location + ":49000";


        sindex.bind (address);


        if (scommands == null) {
            scommands.close ();
        }

        scommands = ctx.createSocket (ZMQ.ROUTER);

        address = "tcp://" + location + ":49003";


        scommands.bind (address);






    }

    private void init ()
    {
        sbroker = ctx.createSocket (ZMQ.DEALER);

        String address = "tcp://127.0.0.1:49002";
        sbroker.bind (address);



    }




    private void commit ()
    {

        try {
            w.commit ();
        }
        catch (IOException io) {
            System.out.println ("Error,while trying to commit..");
            try {
                if (w != null) {
                    w.close ();
                }
            }
            catch (IOException e) {
            }

            finally {
                System.exit (-1);
            }
        }



//send responces to the graph document database


    }


    @Override public void run ()
    {
        ctx = new ZContext ();
        init ();


    }



}
