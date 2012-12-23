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


public class SearcherThread implements Runnable
{
private SearcherManager sm;
private HashMap < String, Analyzer > ha;


SearcherThread(SearcherManager sm,HashMap < String, Analyzer > ha){
this.sm=sm;
this.ha=ha;
}

private void search(int numHits,String language,String defField, String search,VarInt intersection) throws IOException,ParseException{

Analyzer analyzer=ha.get(language);
QueryParser parser = new QueryParser(Version.LUCENE_40,defField, analyzer);

JoinedDocCollector collector=new JoinedDocCollector(numHits,intersection,true);

IndexSearcher s=sm.acquire();

Query query=parser.parse(search);

s.search(query,collector);


}


     @Override public void run ()
    {


    }



}
