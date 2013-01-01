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


import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocValues.Source;
import org.apache.lucene.search.*;

class JoinedDocCollector extends Collector
{

    // Assumes docs are scored in order.

    @Override public void collect (int doc) throws IOException
    {
        float score = scorer.score ();

        // This collector cannot handle these scores:
        assert score != Float.NEGATIVE_INFINITY;
          assert ! Float.isNaN (score);

        if (score <= pqTop.score)
        {
            return;
        }
        //obtain the uid of the document
        int pointer = intersection.bsearch (uidValues.getInt (doc));
        if (pointer == -1) {
//the doc didnt match
            return;
        }
        totalHits++;
        pqTop.pointer = pointer;
        pqTop.score = score;
        pqTop = pq.updateTop ();

    }

    @Override public boolean acceptsDocsOutOfOrder () {
        return false;
    }

    private JoinedDoc pqTop;
    private int docBase = 0;
    private Scorer scorer;
    private JoinedQueue pq;
    private VarInt intersection;
    private int totalHits = 0;
    private Source uidValues;


    public JoinedDocCollector (int numHits, VarInt intersection,
                               boolean docsScoredInOrder)
    {

        if (numHits <= 0) {
            throw new
                IllegalArgumentException
                ("numHits must be > 0; please use TotalHitCountCollector if you just need the total hit count");
        }
        if (!docsScoredInOrder) {
            throw new
                IllegalArgumentException
                ("Only queries that score in sorted order are supported");

        }
        this.intersection = intersection;

        pq = new JoinedQueue (numHits, true);
        // JoinedQueue implements getSentinelObject to return a ScoreDoc, so we know
        // that at this point top() is already initialized.
        pqTop = pq.top ();



    }

    @Override
        public void setNextReader (AtomicReaderContext context) throws
        IOException
    {
        uidValues = context.reader ().docValues ("uid").getSource ();
    }

    @Override public void setScorer (Scorer scorer) throws IOException
    {
        this.scorer = scorer;
    }
}
