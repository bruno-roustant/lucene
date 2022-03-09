/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.index;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.apache.lucene.tests.store.MockDirectoryWrapper;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class TestIndexWriterForceMerge extends LuceneTestCase {
  public void testPartialMerge() throws IOException {

    Directory dir = newDirectory();

    final Document doc = new Document();
    doc.add(newStringField("content", "aaa", Field.Store.NO));
    final int incrMin = TEST_NIGHTLY ? 15 : 40;
    for (int numDocs = 10;
        numDocs < 500;
        numDocs += TestUtil.nextInt(random(), incrMin, 5 * incrMin)) {
      LogDocMergePolicy ldmp = new LogDocMergePolicy();
      ldmp.setMinMergeDocs(1);
      ldmp.setMergeFactor(5);
      IndexWriter writer =
          new IndexWriter(
              dir,
              newIndexWriterConfig(new MockAnalyzer(random()))
                  .setOpenMode(OpenMode.CREATE)
                  .setMaxBufferedDocs(2)
                  .setMergePolicy(ldmp));
      for (int j = 0; j < numDocs; j++) writer.addDocument(doc);
      writer.close();

      SegmentInfos sis = SegmentInfos.readLatestCommit(dir);
      final int segCount = sis.size();

      ldmp = new LogDocMergePolicy();
      ldmp.setMergeFactor(5);
      writer =
          new IndexWriter(
              dir, newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(ldmp));
      writer.forceMerge(3);
      writer.close();

      sis = SegmentInfos.readLatestCommit(dir);
      final int optSegCount = sis.size();

      if (segCount < 3) assertEquals(segCount, optSegCount);
      else assertEquals(3, optSegCount);
    }
    dir.close();
  }

  public void testMaxNumSegments2() throws IOException {
    Directory dir = newDirectory();

    final Document doc = new Document();
    doc.add(newStringField("content", "aaa", Field.Store.NO));

    LogDocMergePolicy ldmp = new LogDocMergePolicy();
    ldmp.setMinMergeDocs(1);
    ldmp.setMergeFactor(4);
    IndexWriter writer =
        new IndexWriter(
            dir,
            newIndexWriterConfig(new MockAnalyzer(random()))
                .setMaxBufferedDocs(2)
                .setMergePolicy(ldmp)
                .setMergeScheduler(new ConcurrentMergeScheduler()));

    for (int iter = 0; iter < 10; iter++) {
      for (int i = 0; i < 19; i++) writer.addDocument(doc);

      writer.commit();
      writer.waitForMerges();
      writer.commit();

      SegmentInfos sis = SegmentInfos.readLatestCommit(dir);

      final int segCount = sis.size();
      writer.forceMerge(7);
      writer.commit();
      writer.waitForMerges();

      sis = SegmentInfos.readLatestCommit(dir);
      final int optSegCount = sis.size();

      if (segCount < 7) assertEquals(segCount, optSegCount);
      else assertEquals("seg: " + segCount, 7, optSegCount);
    }
    writer.close();
    dir.close();
  }

  /**
   * Make sure forceMerge doesn't use any more than 1X starting index size as its temporary free
   * space required.
   */
  public void testForceMergeTempSpaceUsage() throws IOException {

    final MockDirectoryWrapper dir = newMockDirectory();
    // don't use MockAnalyzer, variable length payloads can cause merge to make things bigger,
    // since things are optimized for fixed length case. this is a problem for MemoryPF's encoding.
    // (it might have other problems too)
    Analyzer analyzer =
        new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            return new TokenStreamComponents(new MockTokenizer(MockTokenizer.WHITESPACE, true));
          }
        };
    IndexWriter writer =
        new IndexWriter(
            dir,
            newIndexWriterConfig(analyzer)
                .setMaxBufferedDocs(10)
                .setMergePolicy(newLogMergePolicy()));

    if (VERBOSE) {
      System.out.println("TEST: config1=" + writer.getConfig());
    }

    for (int j = 0; j < 500; j++) {
      TestIndexWriter.addDocWithIndex(writer, j);
    }
    // force one extra segment w/ different doc store so
    // we see the doc stores get merged
    writer.commit();
    TestIndexWriter.addDocWithIndex(writer, 500);
    writer.close();

    long startDiskUsage = 0;
    for (String f : dir.listAll()) {
      startDiskUsage += dir.fileLength(f);
      if (VERBOSE) {
        System.out.println(f + ": " + dir.fileLength(f));
      }
    }
    if (VERBOSE) {
      System.out.println("TEST: start disk usage = " + startDiskUsage);
    }
    String startListing = listFiles(dir);

    dir.resetMaxUsedSizeInBytes();
    dir.setTrackDiskUsage(true);

    writer =
        new IndexWriter(
            dir,
            newIndexWriterConfig(new MockAnalyzer(random()))
                .setOpenMode(OpenMode.APPEND)
                .setMergePolicy(newLogMergePolicy()));

    if (VERBOSE) {
      System.out.println("TEST: config2=" + writer.getConfig());
    }

    writer.forceMerge(1);
    writer.close();

    long finalDiskUsage = 0;
    for (String f : dir.listAll()) {
      finalDiskUsage += dir.fileLength(f);
      if (VERBOSE) {
        System.out.println(f + ": " + dir.fileLength(f));
      }
    }
    if (VERBOSE) {
      System.out.println("TEST: final disk usage = " + finalDiskUsage);
    }

    // The result of the merged index is often smaller, but sometimes it could
    // be bigger (compression slightly changes, Codec changes etc.). Therefore
    // we compare the temp space used to the max of the initial and final index
    // size
    long maxStartFinalDiskUsage = Math.max(startDiskUsage, finalDiskUsage);
    long maxDiskUsage = dir.getMaxUsedSizeInBytes();
    assertTrue(
        "forceMerge used too much temporary space: starting usage was "
            + startDiskUsage
            + " bytes; final usage was "
            + finalDiskUsage
            + " bytes; max temp usage was "
            + maxDiskUsage
            + " but should have been at most "
            + (4 * maxStartFinalDiskUsage)
            + " (= 4X starting usage), BEFORE="
            + startListing
            + "AFTER="
            + listFiles(dir),
        maxDiskUsage <= 4 * maxStartFinalDiskUsage);
    dir.close();
  }

  // print out listing of files and sizes, but recurse into CFS to debug nested files there.
  private String listFiles(Directory dir) throws IOException {
    SegmentInfos infos = SegmentInfos.readLatestCommit(dir);
    StringBuilder sb = new StringBuilder();
    sb.append(System.lineSeparator());
    for (SegmentCommitInfo info : infos) {
      for (String file : info.files()) {
        sb.append(String.format(Locale.ROOT, "%-20s%d%n", file, dir.fileLength(file)));
      }
      if (info.info.getUseCompoundFile()) {
        try (Directory cfs =
            info.info
                .getCodec()
                .compoundFormat()
                .getCompoundReader(dir, info.info, IOContext.DEFAULT)) {
          for (String file : cfs.listAll()) {
            sb.append(
                String.format(
                    Locale.ROOT,
                    " |- (inside compound file) %-20s%d%n",
                    file,
                    cfs.fileLength(file)));
          }
        }
      }
    }
    sb.append(System.lineSeparator());
    return sb.toString();
  }

  // Test calling forceMerge(1, false) whereby forceMerge is kicked
  // off but we don't wait for it to finish (but
  // writer.close()) does wait
  public void testBackgroundForceMerge() throws IOException {

    Directory dir = newDirectory();
    for (int pass = 0; pass < 2; pass++) {
      IndexWriter writer =
          new IndexWriter(
              dir,
              newIndexWriterConfig(new MockAnalyzer(random()))
                  .setOpenMode(OpenMode.CREATE)
                  .setMaxBufferedDocs(2)
                  .setMergePolicy(newLogMergePolicy(51)));
      Document doc = new Document();
      doc.add(newStringField("field", "aaa", Field.Store.NO));
      for (int i = 0; i < 100; i++) {
        writer.addDocument(doc);
      }
      writer.forceMerge(1, false);

      if (0 == pass) {
        writer.close();
        DirectoryReader reader = DirectoryReader.open(dir);
        assertEquals(1, reader.leaves().size());
        reader.close();
      } else {
        // Get another segment to flush so we can verify it is
        // NOT included in the merging
        writer.addDocument(doc);
        writer.addDocument(doc);
        writer.close();

        DirectoryReader reader = DirectoryReader.open(dir);
        assertTrue(reader.leaves().size() > 1);
        reader.close();

        SegmentInfos infos = SegmentInfos.readLatestCommit(dir);
        assertEquals(2, infos.size());
      }
    }

    dir.close();
  }

  public void testForceMergeWithConcurrentCommits() throws Exception {
    boolean debug = false;
    ExecutorService executorService = Executors.newFixedThreadPool(2);
    try (Directory dir = newDirectory()) {
      try (IndexWriter writer = new IndexWriter(
        dir,
        newIndexWriterConfig(new MockAnalyzer(random()))
          .setOpenMode(OpenMode.CREATE)
          .setMaxBufferedDocs(2)
          .setMergePolicy(newLogMergePolicy(51)))) {
        AtomicInteger docIndex = new AtomicInteger();
        List<Callable<Void>> concurrentCommits = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
          // Create and commit two segments.
          addDoc(docIndex.incrementAndGet(), writer);
          writer.commit();
          addDoc(docIndex.incrementAndGet(), writer);
          writer.commit();
          SegmentInfos infos = SegmentInfos.readLatestCommit(dir);
          assertTrue(i == 0 ? infos.size() == 2 : infos.size() >= 3 && infos.size() <= 4);
          if (debug) {
            printSegments("\n\nBefore concurrent commits", -1, dir);
          }

          concurrentCommits.clear();
          concurrentCommits.add(() -> {
            writer.forceMerge(1);
            long seq = writer.commit();
            if (debug) {
              printSegments("Force merge commit", seq, dir);
            }
            return null;
          });
          concurrentCommits.add(() -> {
            Thread.sleep(random().nextInt(10));
            addDoc(docIndex.incrementAndGet(), writer);
            long seq = writer.commit();
            if (debug) {
              printSegments("Concurrent indexing commit", seq, dir);
            }
            return null;
          });

          executorService.invokeAll(concurrentCommits);

          infos = SegmentInfos.readLatestCommit(dir);
          assertTrue(infos.size() >= 1 && infos.size() <= 2);
          if (debug) {
            printSegments("After concurrent commits", -1, dir);
          }
        }
      }
    } finally {
      executorService.shutdown();
    }
  }

  private void addDoc(int docIndex, IndexWriter writer) throws IOException {
    Document doc = new Document();
    doc.add(newStringField("field", "value" + docIndex, Field.Store.NO));
    writer.addDocument(doc);
  }

  private void printSegments(String message, long seq, Directory dir) throws IOException {
    StringBuilder s = new StringBuilder();
    if (seq != -1) {
      s.append("(").append(seq).append(") ");
    }
    s.append(message).append(":");
    for (SegmentCommitInfo segment : SegmentInfos.readLatestCommit(dir)) {
      s.append("\n  ").append(segment.info.name);
    }
    System.out.println(s);
  }
}
