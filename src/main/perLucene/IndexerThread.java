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

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.document.DerefBytesDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongDocValuesField;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;

import java.util.HashMap;
import java.util.List;
import java.io.IOException;
import java.util.HashSet;

class IndexerThread implements Runnable {

	protected IndexWriter w;
	protected HashMap<String, Analyzer> ha;

	protected Italk italk;

	protected String location;
	protected int replica;

	public long id; // ids are incremented in big-endian unsigned integer
	protected Spatial sp;
	protected SearcherManager sm;

	// order

	IndexerThread(IndexWriter w, SearcherManager sm,
			HashMap<String, Analyzer> ha, String location, int replica, long id) {
		this.w = w;
		this.ha = ha;
		this.location = location;
		this.replica = replica;
		this.id = id;
		this.sm = sm;
		sp = new Spatial();
	}

	// ids are incremented in big-endian unsigned integer order
	// used only by the leader
	public long incrementId() {
		Long i = id;
		long mask = 1;

		while ((i & mask) != 0) {
			i &= ~mask;
			mask <<= 1;
		}
		i |= mask;

		id = i;
		return id;
	}

	// Add spatial search support
	public boolean addDoc(String language, String summary, String text,
			long date, String wkt, byte[] gdid, long id) {
		Analyzer analyzer = ha.get(language);

		Document doc = new Document();
		doc.add(new TextField("summary", summary, Field.Store.NO));
		doc.add(new TextField("text", text, Field.Store.NO));
		doc.add(new LongDocValuesField("uid", id));
		doc
				.add(new DerefBytesDocValuesField("language", new BytesRef(
						language)));
		doc.add(new LongField("date", date, Field.Store.NO));
		doc.add(new StoredField("gdid", gdid));
		doc.add(new LongField("id", id, Field.Store.NO));
		// ?? for
		// deletions we
		// need
		// LongField to
		// do search on
		// id
		if (sp.addFields(doc, wkt)) {
			try {
				w.addDocument(doc, analyzer);
			} catch (Exception e) {
				System.out.println("Couldnt add Doc");
				System.out.println("Stacktrace " + e.toString());

				try {
					if (w != null) {
						w.close();
					}
				} catch (IOException e1) {
				}

				System.exit(-1);
			}

			return true;
		} else {
			return false;
		}
	}

	public void deleteDoc(long id) {
		try {
			sm.maybeRefreshBlocking();
			IndexSearcher s = sm.acquire();

			Query q = NumericRangeQuery.newLongRange("id", 1, id, id, true,
					true);

			// TODO remove assertion
			TopDocs td = s.search(q, 1);
			assert (td.totalHits <= 1);

			w.deleteDocuments(q);

			sm.release(s);

		} catch (Exception e) {
			System.out.println("Couldnt delete Doc");
			System.out.println("Stacktrace " + e.toString());

			try {
				if (w != null) {
					w.close();
				}
			} catch (IOException e1) {
			}

			System.exit(-1);
		}

	}

	public void deleteDocs(long start, long end) {
		try {
			sm.maybeRefreshBlocking();
			IndexSearcher s = sm.acquire();

			Query q = NumericRangeQuery.newLongRange("id", 1, start, end, true,
					true);

			w.deleteDocuments(q);

			sm.release(s);

		} catch (Exception e) {
			System.out.println("Couldnt delete Docs");
			System.out.println("Stacktrace " + e.toString());

			try {
				if (w != null) {
					w.close();
				}
			} catch (IOException e1) {
			}

			System.exit(-1);
		}

	}

	private void softSync() {

	}

	private void Sync() {

	}

	private void commit() {

		try {
			w.commit();
		} catch (IOException io) {
			System.out.println("Error,while trying to commit..");
			try {
				if (w != null) {
					w.close();
				}
			} catch (IOException e) {
			}

			finally {
				System.exit(-1);
			}
		}
	}

	@Override
	public void run() {
		italk = new Italk(this, location, replica);
		italk.poll();

	}

}
