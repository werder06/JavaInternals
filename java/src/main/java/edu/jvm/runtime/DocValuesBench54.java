package edu.jvm.runtime;

import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.search.BitDocSet;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgs = {
//        "-XX:+UnlockDiagnosticVMOptions",
//        "-XX:+PrintInlining",
//        "-XX:+DebugNonSafepoints",
//        "-XX:+UnlockCommercialFeatures",
//        "-XX:+FlightRecorder",
//        "-XX:StartFlightRecording=duration=60s,settings=profile,filename=/tmp/myrecording.jfr"
})
@State(Scope.Benchmark)
public class DocValuesBench54 {

  private static final String INDEX_PATH = "/Users/akudryavtsev/Downloads/indexes/products/restore.20170817052816863";

  private DirectoryReader reader;

  @Param({"873", "374", "44", "45"})
  private int store;

  private FixedBitSet bitSet;
  private IndexSearcher searcher;
  private int maxDoc;
  private Random random;

  @Setup
  public void init() throws IOException {
    random = new Random(0xDEAD_BEEF);
    FSDirectory directory = FSDirectory.open(new File(INDEX_PATH).toPath());
    reader = DirectoryReader.open(directory);
    List<LeafReaderContext> leaves = reader.leaves();
    LeafReaderContext leafReaderContext = leaves.get(0);
    maxDoc = reader.numDocs();
    searcher = new IndexSearcher(reader);

    //this bitset is used as baseline for all measurements
    bitSet = new FixedBitSet(maxDoc);
    Query query = DocValuesRangeQuery.newLongRange(getStore(), 1L, 1L, true, true);
    query = query.rewrite(reader);
    Weight weight = query.createWeight(searcher, false);
    Scorer scorer = weight.scorer(leafReaderContext);
    DocIdSetIterator docs = scorer.iterator();
    int doc;
    while ((doc = docs.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      bitSet.set(doc);
    }
  }

  private String getStore() {
    return "store_" + store;
  }

  @Benchmark
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int iterateAllRangeQuery() throws IOException {
    int result = 0;
    Query query = DocValuesRangeQuery.newLongRange(getStore(), 1L, 1L, true, true);
    query = query.rewrite(reader);
    Weight weight = query.createWeight(searcher, false);
    for (LeafReaderContext context : reader.getContext().leaves()) {
      Scorer scorer = weight.scorer(context);
      DocIdSetIterator docs = scorer.iterator();
      while (docs.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
        result++;
      }
    }
    return result;
  }


  @Benchmark
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int iterateAllDocValuesFull() throws IOException {
    int result = 0;
    NumericDocValues numericDocValues = MultiDocValues.getNumericValues(reader, getStore());
    if(numericDocValues == null) {
      return 0;
    }
    FixedBitSet bitSet = new FixedBitSet(maxDoc);
    for (int j = 0; j < maxDoc; j++) {
      long value = numericDocValues.get(j);
      if (value == 1) {
        bitSet.set(j);
      }
    }
    BitDocSet bitDocSet = new BitDocSet(bitSet);
    Query query = bitDocSet.getTopFilter();
    query = query.rewrite(reader);
    Weight weight = query.createWeight(searcher, false);
    for (LeafReaderContext context : reader.getContext().leaves()) {
      Scorer scorer = weight.scorer(context);
      DocIdSetIterator docs = scorer.iterator();
      while (docs.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
        result++;
      }
    }
    return result;
  }

  @Benchmark
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int iterateAllDocValuesWithoutQuery() throws IOException {
    int result = 0;
    String store = getStore();
    for (LeafReaderContext context : reader.getContext().leaves()) {
      NumericDocValues numericDocValues = DocValues.getNumeric(context.reader(), store);
      FixedBitSet bitSet = new FixedBitSet(context.reader().maxDoc());
      for (int j = 0; j < context.reader().maxDoc(); j++) {
        long value = numericDocValues.get(j);
        if (value == 1) {
          bitSet.set(j);
        }
      }
      if(bitSet.cardinality() == 0) {
        continue;
      }
      BitDocSet bitDocSet = new BitDocSet(bitSet);
      Query query = bitDocSet.getTopFilter();
      Weight weight = query.createWeight(searcher, false);
      Scorer scorer = weight.scorer(context);
      DocIdSetIterator docs = scorer.iterator();
      while (docs.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
        result++;
      }
    }
    return result;
  }


  @Benchmark
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int customDocValuesQuery() throws IOException {
    int result = 0;
    Query query = new DocValuesNumberQuery(getStore(), 1);
    query = query.rewrite(reader);
    Weight weight = query.createWeight(searcher, false);
    for (LeafReaderContext context : reader.getContext().leaves()) {
      Scorer scorer = weight.scorer(context);
      DocIdSetIterator docs = scorer.iterator();
      while (docs.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
        result++;
      }
    }
    return result;
  }


  public static void main(String[] args) throws Exception {

    Options options = new OptionsBuilder()
        .include(DocValuesBench54.class.getName())
//                .addProfiler(LinuxPerfAsmProfiler.class)
//                .addProfiler(StackProfiler.class)
//                .addProfiler(LinuxPerfProfiler.class)
//                .addProfiler(GCProfiler.class)
        .verbosity(VerboseMode.NORMAL)
        .build();
    new Runner(options).run();
  }

  private class DocValuesNumberQuery extends Query {

    private final String field;
    private final long number;

    public DocValuesNumberQuery(String field, long number) {
      this.field = Objects.requireNonNull(field);
      this.number = number;
    }

    @Override
    public boolean equals(Object obj) {
      if (!super.equals(obj)) {
        return false;
      }
      DocValuesNumberQuery that = (DocValuesNumberQuery) obj;
      return field.equals(that.field) && number == that.number;
    }

    @Override
    public int hashCode() {
      return 31 * super.hashCode() + Objects.hash(field, number);
    }

    @Override
    public String toString(String defaultField) {
      return field + ":" + number;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
      return new RandomAccessWeight(this) {

        @Override
        protected Bits getMatchingDocs(final LeafReaderContext context) throws IOException {
          final NumericDocValues values = DocValues.getNumeric(context.reader(), field);
          return new Bits() {

            @Override
            public boolean get(int doc) {
              return values.get(doc) == number;
            }

            @Override
            public int length() {
              return context.reader().maxDoc();
            }
          };
        }
      };
    }
  }
}