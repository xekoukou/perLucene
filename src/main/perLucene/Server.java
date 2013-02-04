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

import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;

import org.apache.lucene.analysis.el.GreekAnalyzer;
import org.apache.lucene.analysis.ar.ArabicAnalyzer;
import org.apache.lucene.analysis.bg.BulgarianAnalyzer;
import org.apache.lucene.analysis.br.BrazilianAnalyzer;
import org.apache.lucene.analysis.ca.CatalanAnalyzer;
import org.apache.lucene.analysis.cz.CzechAnalyzer;
import org.apache.lucene.analysis.da.DanishAnalyzer;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.apache.lucene.analysis.eu.BasqueAnalyzer;
import org.apache.lucene.analysis.fa.PersianAnalyzer;
import org.apache.lucene.analysis.fi.FinnishAnalyzer;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.ga.IrishAnalyzer;
import org.apache.lucene.analysis.gl.GalicianAnalyzer;
import org.apache.lucene.analysis.hi.HindiAnalyzer;
import org.apache.lucene.analysis.hu.HungarianAnalyzer;
import org.apache.lucene.analysis.hy.ArmenianAnalyzer;
import org.apache.lucene.analysis.id.IndonesianAnalyzer;
import org.apache.lucene.analysis.it.ItalianAnalyzer;
import org.apache.lucene.analysis.lv.LatvianAnalyzer;
import org.apache.lucene.analysis.nl.DutchAnalyzer;
import org.apache.lucene.analysis.no.NorwegianAnalyzer;
import org.apache.lucene.analysis.pt.PortugueseAnalyzer;
import org.apache.lucene.analysis.ro.RomanianAnalyzer;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.apache.lucene.analysis.sv.SwedishAnalyzer;
import org.apache.lucene.analysis.th.ThaiAnalyzer;
import org.apache.lucene.analysis.tr.TurkishAnalyzer;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.SearcherFactory;
import org.jeromq.ZLoop;
import org.jeromq.ZMQ;
import org.jeromq.ZMsg;
import org.jeromq.ZMQ.PollItem;

import java.io.IOException;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.lang.System;

//TODO should I call this class abstract since it should not be initialized
public class Server {

	private static SearcherManager sm;
	private static IndexWriter w;
	// conatins all the analyzers that are to be used by IndexWriter or the
	// query
	// this is READ-ONLY
	private static HashMap<String, Analyzer> ha;

	private static ZooTiger tiger;

	public static void main(String[] args) {
		boolean becomeLeader = false;
		System.out.println("Starting perLucene Server...");

		if (args.length == 1) {
			if (args[0].equals("becomeLeader")) {
				becomeLeader = true;

			} else {
				System.out.println("only option : becomeLeader");

				System.exit(0);

			}

		}

		try {
			initAnalyzers();
			initIndexWriter();
			initSearcherManager();

			long id = getId();

			String[] line = Config.readLocalConfig();
			int nSearchThreads = Integer.parseInt(line[4]);

			// Init the Zookeeper client
			tiger = new ZooTiger();

			String location = tiger.getConfig();

			// Init the threads
			for (int i = 0; i < nSearchThreads; i++) {
				(new Thread(new SearcherThread(sm, ha, location))).start();
			}
			(new Thread(new IndexerThread(w, sm, ha, location, Integer
					.parseInt(line[3]), id))).start();

			tiger.goOnline(becomeLeader);

			while (true) {
				ZMQ.poll(null, 10000);
				updateSearcherManager();
				notifyZoo();
			}

		} catch (Exception e) {
			System.out.println("there has been an error");
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

	private static void initAnalyzers() {

		ha = new HashMap<String, Analyzer>();

		ha.put("ar", new ArabicAnalyzer(Version.LUCENE_41));
		ha.put("el", new GreekAnalyzer(Version.LUCENE_41));
		ha.put("bg", new BulgarianAnalyzer(Version.LUCENE_41));
		ha.put("br", new BrazilianAnalyzer(Version.LUCENE_41));
		ha.put("ca", new CatalanAnalyzer(Version.LUCENE_41));
		ha.put("cz", new CzechAnalyzer(Version.LUCENE_41));
		ha.put("da", new DanishAnalyzer(Version.LUCENE_41));
		ha.put("de", new GermanAnalyzer(Version.LUCENE_41));
		ha.put("en", new EnglishAnalyzer(Version.LUCENE_41));
		ha.put("es", new SpanishAnalyzer(Version.LUCENE_41));
		ha.put("eu", new BasqueAnalyzer(Version.LUCENE_41));
		ha.put("fa", new PersianAnalyzer(Version.LUCENE_41));
		ha.put("fi", new FinnishAnalyzer(Version.LUCENE_41));
		ha.put("fr", new FrenchAnalyzer(Version.LUCENE_41));
		ha.put("ga", new IrishAnalyzer(Version.LUCENE_41));
		ha.put("gl", new GalicianAnalyzer(Version.LUCENE_41));
		ha.put("hi", new HindiAnalyzer(Version.LUCENE_41));
		ha.put("hu", new HungarianAnalyzer(Version.LUCENE_41));
		ha.put("hy", new ArmenianAnalyzer(Version.LUCENE_41));
		ha.put("id", new IndonesianAnalyzer(Version.LUCENE_41));
		ha.put("it", new ItalianAnalyzer(Version.LUCENE_41));
		ha.put("lv", new LatvianAnalyzer(Version.LUCENE_41));
		ha.put("nl", new DutchAnalyzer(Version.LUCENE_41));
		ha.put("no", new NorwegianAnalyzer(Version.LUCENE_41));
		ha.put("pt", new PortugueseAnalyzer(Version.LUCENE_41));
		ha.put("ro", new RomanianAnalyzer(Version.LUCENE_41));
		ha.put("ru", new RussianAnalyzer(Version.LUCENE_41));
		ha.put("sv", new SwedishAnalyzer(Version.LUCENE_41));
		ha.put("th", new ThaiAnalyzer(Version.LUCENE_41));
		ha.put("tr", new TurkishAnalyzer(Version.LUCENE_41));
		ha.put("cn", new SmartChineseAnalyzer(Version.LUCENE_41));

	}

	private static void initIndexWriter() {

		try {
			Directory dir = new MMapDirectory(new File("/mnt/perLucene"));

			// FSDirectory.setMaxMergeWriteMBPerSec.
			// use this if merges interfere with the search

			// use a unix native directory to tell the os not to cache merges

			NRTCachingDirectory index = new NRTCachingDirectory(dir, 500, 1000);

			IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_41,
					null);
			// used to reduce latency of obtaining a reader after a big merge
			// config.setMergedSegmentWarmer(new
			// IndexWriter.IndexReaderWarmer());

			config.setMergePolicy(new LogByteSizeMergePolicy());

			w = new IndexWriter(index, config);
		} catch (IOException e) {
			System.out.println("Stacktrace " + e.toString());
		}

	}

	// must be called only if the writer is initialized
	private static void initSearcherManager() {
		System.out.println("initSearcherManager");
		try {
			sm = new SearcherManager(w, true, new SearcherFactory());
		} catch (IOException e) {
			System.out.println("Stacktrace " + e.toString());
		}

	}

	/**
	 * Use at the beginning to obtain the last id that this replica has indexed
	 */
	protected static long getId() {

		// not necessary if used at the beginning
		try {
			sm.maybeRefreshBlocking();
			IndexSearcher s = sm.acquire();
			List<AtomicReaderContext> leaves = s.getIndexReader().leaves();
			int size = leaves.size();
			AtomicReader r = leaves.get(size - 1).reader();
			int lastDoc = r.maxDoc() - 1;

			long id = r.docValues("uid").getDirectSource().getInt(lastDoc);

			sm.release(s);

			return id;

		} catch (Exception e) {
			System.out.println("Couldnt get last Id(refresh)");
			System.exit(-1);
		}
		// should never happen
		return -1;
	}

	// these 2 must me done every few minutes

	private static void updateSearcherManager() {
		try {

			sm.maybeRefresh();
		} catch (IOException e) {
			System.out.println("Stacktrace " + e.toString());

		}

	}

	private static void notifyZoo() {

		tiger.notifyZoo();

	}

}
