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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.index.IndexWriter;

import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;

import org.apache.lucene.util.Version;


import java.util.HashMap;
import java.io.IOException;
import org.apache.lucene.queryparser.classic.ParseException;


import org.jeromq.ZMQ;
import org.jeromq.ZContext;
import org.jeromq.ZFrame;
import org.jeromq.ZMsg;
import org.jeromq.ZMQ.Msg;
import org.jeromq.ZMQ.PollItem;
import org.jeromq.ZMQ.Socket;


class SearcherThread implements Runnable
{
    protected SearcherManager sm;
    protected HashMap < String, Analyzer > ha;
    protected String location;
    protected Socket ssearch;
    protected Socket ssres;
    protected ZContext ctx;



      SearcherThread (SearcherManager sm, HashMap < String, Analyzer > ha,
                      String location)
    {
        this.sm = sm;
        this.ha = ha;
        this.location = location;

    }

    private void search (int numHits, String language, String defField,
                         String search, VarInt intersection) throws IOException,
        ParseException
    {

        Analyzer analyzer = ha.get (language);
        QueryParser parser =
            new QueryParser (Version.LUCENE_40, defField, analyzer);

        JoinedDocCollector collector =
            new JoinedDocCollector (numHits, intersection, true);

        IndexSearcher s = sm.acquire ();

        Query query = parser.parse (search);

          s.search (query, collector);


    }

    private void init ()
    {

//bindings for search

        ssearch = ctx.createSocket (ZMQ.DEALER);

        String address = "tcp://" + location + ":49000";
        ssearch.bind (address);

        ssres = ctx.createSocket (ZMQ.ROUTER);

        address = "tcp://" + location + ":49001";
        ssres.bind (address);



    }



    @Override public void run ()
    {
        ctx = new ZContext ();
        init ();


    }



}
