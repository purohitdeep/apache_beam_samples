package com.deep.sample.pcollection;

import static org.junit.Assert.assertEquals;

import com.deep.sample.BeamFunctions;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.commons.io.FileUtils;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PCollectionTests {
  private static final String FILE_1 = "/resources/beam/1";
  private static final String FILE_2 = "/resources/beam/2";

  @BeforeClass
  public static void writeFiles() throws IOException {
    FileUtils.writeStringToFile(new File(FILE_1), "1\n2\n3\n4", "UTF-8");
    FileUtils.writeStringToFile(new File(FILE_2), "5\n6\n7\n8", "UTF-8");
  }

  @AfterClass
  public static void deleteFiles() {
    FileUtils.deleteQuietly(new File(FILE_1));
    FileUtils.deleteQuietly(new File(FILE_2));
  }

  @Test
  public void should_construct_pcollection_from_memory_objects() {
    List<String> letters = Arrays.asList("a", "b", "c", "d");
    Pipeline p = BeamFunctions.createPipeline("creating from memory");
    PCollection<String> apply = p.apply(Create.of(letters));
    PAssert.that(apply).containsInAnyOrder(letters);
    p.run().waitUntilFinish();
  }

  @Test
  public void should_construct_pcollection_without_applying_transform() {
    Pipeline p = BeamFunctions.createPipeline("Create Pcollection from text file");
    TextIO.Read reader = TextIO.read().from("src/test/resources/beam/pcollection/numbers.txt");
    PCollection<String> readNumbers = p.apply(reader);
    PAssert.that(readNumbers).containsInAnyOrder("1", "2", "3", "4", "5", "6", "7", "8");
    p.run().waitUntilFinish();
  }

  @Test
  public void should_not_modify_input_pcollection_after_transformation() {
    Pipeline p = BeamFunctions.createPipeline("Pcollection should be immutable");
    List<String> letters = Arrays.asList("a", "b", "c", "d");
    PCollection<String> lettersCollection = p.apply(Create.of(letters));
    PCollection<String> filterCollection = lettersCollection.apply(Filter.equal("a"));
    PAssert.that(lettersCollection).containsInAnyOrder(letters);
    PAssert.that(filterCollection).containsInAnyOrder("a");
  }

  @Test
  public void should_use_one_pcollection_as_input_to_different_transformation() {
    Pipeline p =
        BeamFunctions.createPipeline(
            "A single Pcollection is used as input for different transformation ");
    List<String> letters = Arrays.asList("a", "b", "c", "d");
    PCollection<String> lettersCollection = p.apply(Create.of(letters));
    PCollection<String> filterCollection = lettersCollection.apply(Filter.equal("a"));
    PCollection<String> notALetter = lettersCollection.apply(Filter.greaterThan("a"));

    PAssert.that(lettersCollection).containsInAnyOrder(letters);
    PAssert.that(filterCollection).containsInAnyOrder("a");
    PAssert.that(notALetter).containsInAnyOrder("b", "c", "d");
  }

  @Test
  public void should_create_explicitly_timestamped_batch_pcollection_with_custom_windows() {
    Pipeline p = BeamFunctions.createPipeline("PCollection with different windows");
    PCollection<Integer> timestampedNumbers =
        p.apply(
            Create.timestamped(
                TimestampedValue.of(1, new Instant(1)),
                TimestampedValue.of(2, new Instant(1)),
                TimestampedValue.of(3, new Instant(3)),
                TimestampedValue.of(4, new Instant(4))));

    PCollection<String> mappedResults =
        timestampedNumbers
            .apply(Window.into(FixedWindows.of(new Duration(1))))
            .apply(
                ParDo.of(
                    new DoFn<Integer, String>() {
                      @ProcessElement
                      public void processElement(ProcessContext context, BoundedWindow window) {
                        context.output(
                            context.element()
                                + "("
                                + context.timestamp()
                                + ")"
                                + " window="
                                + window);
                      }
                    }));

    PAssert.that(mappedResults)
        .containsInAnyOrder(
            "1(1970-01-01T00:00:00.001Z) window=[1970-01-01T00:00:00.001Z..1970-01-01T00:00:00.002Z)",
            "2(1970-01-01T00:00:00.001Z) window=[1970-01-01T00:00:00.001Z..1970-01-01T00:00:00.002Z)",
            "3(1970-01-01T00:00:00.003Z) window=[1970-01-01T00:00:00.003Z..1970-01-01T00:00:00.004Z)",
            "4(1970-01-01T00:00:00.004Z) window=[1970-01-01T00:00:00.004Z..1970-01-01T00:00:00.005Z)");
  }

  @Test
  public void should_get_pcollection_coder() {
    List<String> letters = Arrays.asList("1", "b", "c", "d");
    Pipeline pipeline = BeamFunctions.createPipeline("Pcollection coder");

    PCollection<String> lettersCollection = pipeline.apply(Create.of(letters));
    pipeline.run().waitUntilFinish();

    Coder<String> lettersCoder = lettersCollection.getCoder();
    assertEquals(lettersCoder.getClass(), StringUtf8Coder.class);
  }

  @Test
  public void should_get_pcollection_metadata() {
    List<String> letters = Arrays.asList("a", "b", "c", "d");
    Pipeline pipeline = BeamFunctions.createPipeline("Pcollection metadata check");

    PCollection<String> lettersCollection = pipeline.apply("A-B-C-D letters", Create.of(letters));
    pipeline.run().waitUntilFinish();

    assertEquals(lettersCollection.isBounded(), PCollection.IsBounded.BOUNDED);
    WindowingStrategy windowingStrategy = lettersCollection.getWindowingStrategy();
    assertEquals(windowingStrategy.getWindowFn().getClass(), GlobalWindows.class);
    assertEquals(lettersCollection.getName(), "A-B-C-D letters/Read(CreateSource).out");
  }

  @Test
  public void should_create_pcollection_list() {
    Pipeline pipeline = BeamFunctions.createPipeline("Pcollection list");
    PCollection<String> letters1 = pipeline.apply(Create.of(Arrays.asList("a", "b", "c")));
    PCollection<String> letters2 = pipeline.apply(Create.of(Arrays.asList("d", "e", "f")));
    PCollection<String> letters3 = pipeline.apply(Create.of(Arrays.asList("g", "h", "i")));

    PCollectionList<String> allLetters = PCollectionList.of(letters1).and(letters2).and(letters3);
    List<PCollection<String>> lettersCollection = allLetters.getAll();

    pipeline.run().waitUntilFinish();

    PAssert.that(lettersCollection.get(0)).containsInAnyOrder("a", "b", "c");
    PAssert.that(lettersCollection.get(1)).containsInAnyOrder("d", "e", "f");
    PAssert.that(lettersCollection.get(2)).containsInAnyOrder("g", "h", "i");
  }

  @Test
  public void should_create_pcollection_tuple() {
    Pipeline pipeline = BeamFunctions.createPipeline("Pcollection tuple");
    PCollection<String> letters = pipeline.apply(Create.of(Arrays.asList("a", "b", "c")));
    PCollection<Integer> numbers = pipeline.apply(Create.of(Arrays.asList(1, 2, 3)));
    PCollection<Boolean> flags = pipeline.apply(Create.of(Arrays.asList(true, false, true)));

    TupleTag<String> lettersTag = new TupleTag<>();
    TupleTag<Integer> numbersTag = new TupleTag<>();
    TupleTag<Boolean> flagsTag = new TupleTag<>();

    PCollectionTuple mixedDataTuple =
        PCollectionTuple.of(lettersTag, letters).and(numbersTag, numbers).and(flagsTag, flags);
    Map<TupleTag<?>, PCollection<?>> allData = mixedDataTuple.getAll();

    PAssert.that((PCollection<String>) allData.get(lettersTag)).containsInAnyOrder("a", "b", "c");
    PAssert.that((PCollection<Integer>) allData.get(numbersTag)).containsInAnyOrder(1, 2, 3);
    PAssert.that((PCollection<Boolean>) allData.get(flagsTag))
        .containsInAnyOrder(true, false, true);
    pipeline.run().waitUntilFinish();
  }
}
