package com.deep.sample.transformations;

import static org.assertj.core.api.Assertions.assertThat;

import com.deep.sample.BeamFunctions;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.transforms.Regex;
import org.apache.beam.sdk.transforms.Sample;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Top;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.common.collect.Iterables;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TransformTest implements Serializable {

  @Test
  public void should_filter_empty_words() {
    Pipeline pipeline = BeamFunctions.createPipeline("Empty words filter");
    PCollection<String> dataCollection =
        pipeline.apply(Create.of(Arrays.asList("", "a", "", "", "ab", "ab", "abc")));
    PCollection<String> nonEmptyWord = dataCollection.apply(Filter.by(Filters.NON_EMPTY));
    PAssert.that(nonEmptyWord).containsInAnyOrder(Arrays.asList("a", "ab", "ab", "abc"));
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void should_take_only_a_letter() {
    Pipeline pipeline = BeamFunctions.createPipeline("Should take only a letter");
    PCollection<String> dataCollection =
        pipeline.apply(Create.of(Arrays.asList("a", "b", "c", "a")));
    PCollection<String> aCollection = dataCollection.apply(Filter.equal("a"));
    PAssert.that(aCollection).containsInAnyOrder("a", "a");
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void should_only_keep_numbers_greater_than_equal_to_2() {
    Pipeline pipeline = BeamFunctions.createPipeline("greater than or equal to - Filter");
    PCollection<Integer> numberCollection =
        pipeline.apply(Create.of(Arrays.asList(1, 2, 3, 4, 5, -1, -3)));
    PCollection<Integer> filteredCollection = numberCollection.apply(Filter.greaterThanEq(2));
    PAssert.that(filteredCollection).containsInAnyOrder(2, 3, 4, 5);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void should_map_words_into_their_length() {
    Pipeline pipeline = BeamFunctions.createPipeline("Mapping transform");
    PCollection<String> dataCollection =
        pipeline.apply(Create.of(Arrays.asList("", "a", "", "", "ab", "ab", "abc")));
    PCollection<Integer> lengthCollection =
        dataCollection.apply(
            MapElements.into(TypeDescriptors.integers()).via(words -> words.length()));
    pipeline.run().waitUntilFinish();
    PAssert.that(lengthCollection).containsInAnyOrder(0, 1, 0, 0, 2, 2, 3);
  }

  @Test
  public void should_partition_numbers_to_10_equal_partitions() {
    Pipeline pipeline = BeamFunctions.createPipeline("Partitioning transformation");
    PCollection<Integer> numbersCollection =
        pipeline.apply(
            Create.of(IntStream.rangeClosed(1, 20).boxed().collect(Collectors.toList())));
    PCollectionList<Integer> repartitionedNumbers =
        numbersCollection.apply(
            Partition.of(
                4, (Partition.PartitionFn<Integer>) (elem, numPartitions) -> elem % numPartitions));

    PAssert.that(repartitionedNumbers.get(0)).containsInAnyOrder(4, 8, 12, 16, 20);
    PAssert.that(repartitionedNumbers.get(1)).containsInAnyOrder(1, 5, 9, 13, 17);
    PAssert.that(repartitionedNumbers.get(2)).containsInAnyOrder(2, 6, 10, 14, 18);
    PAssert.that(repartitionedNumbers.get(3)).containsInAnyOrder(3, 7, 11, 15, 19);

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void should_count_number_of_elements_in_collection() {
    Pipeline pipeline = BeamFunctions.createPipeline("Count Transformation");
    PCollection<Integer> integerPCollection = pipeline.apply(Create.of(Arrays.asList(1, 2, 3)));
    PCollection<Long> countCollection = integerPCollection.apply(Count.globally());
    PAssert.that(countCollection).containsInAnyOrder(3L);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void should_count_occurances_per_element_in_key_value_collection() {
    Pipeline pipeline = BeamFunctions.createPipeline("Count per element transformation");
    PCollection<String> dataCollection =
        pipeline.apply(Create.of(Arrays.asList("", "a", "", "", "ab", "ab", "abc")));

    PCollection<KV<String, Long>> elementCountCollection = dataCollection.apply(Count.perElement());

    PAssert.that(elementCountCollection)
        .containsInAnyOrder(KV.of("", 3L), KV.of("a", 1L), KV.of("ab", 2L), KV.of("abc", 1L));
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void should_get_distinct_words() {
    Pipeline pipeline = BeamFunctions.createPipeline("Distinct transform");
    PCollection<String> dataCollection =
        pipeline.apply(Create.of(Arrays.asList("", "a", "", "", "ab", "ab", "abc")));

    PCollection<String> stringPCollection = dataCollection.apply(Distinct.create());
    pipeline.run().waitUntilFinish();
    PAssert.that(stringPCollection).containsInAnyOrder("", "a", "ab", "abc");
  }

  @Test
  public void should_keep_only_one_pair() {
    Pipeline pipeline =
        BeamFunctions.createPipeline("Distinct with representative values for key-value pairs");
    PCollection<KV<Integer, String>> dataCollection =
        pipeline.apply(
            Create.of(Arrays.asList(KV.of(1, "a"), KV.of(2, "b"), KV.of(1, "a"), KV.of(10, "a"))));

    PCollection<KV<Integer, String>> distinctPairs =
        dataCollection.apply(
            Distinct.withRepresentativeValueFn(
                new SerializableFunction<KV<Integer, String>, String>() {
                  @Override
                  public String apply(KV<Integer, String> input) {
                    return input.getValue();
                  }
                }));

    PAssert.that(distinctPairs).containsInAnyOrder(KV.of(1, "a"), KV.of(2, "b"));
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void should_get_minimum_value() {
    Pipeline pipeline = BeamFunctions.createPipeline("minimum value transform");
    PCollection<Integer> integerPCollection = pipeline.apply(Create.of(Arrays.asList(1, 2, 3)));
    PCollection<Integer> MinCollection = integerPCollection.apply(Min.globally());
    PAssert.that(MinCollection).containsInAnyOrder(1);
    pipeline.run();
  }

  @Test
  public void should_get_minimum_value_customer_key() {
    Pipeline pipeline = BeamFunctions.createPipeline("minimum value key transform");
    PCollection<KV<String, Integer>> customerOrders =
        pipeline.apply(
            Create.of(
                Arrays.asList(
                    KV.of("C#1", 100),
                    KV.of("C#2", 108),
                    KV.of("C#3", 120),
                    KV.of("C#1", 209),
                    KV.of("C#1", 210),
                    KV.of("C#1", 200),
                    KV.of("C#2", 450))));
    PCollection<KV<String, Integer>> minValueCollection = customerOrders.apply(Min.perKey());
    PAssert.that(minValueCollection)
        .containsInAnyOrder(KV.of("C#1", 100), KV.of("C#2", 108), KV.of("C#3", 120));
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void should_get_min_value_with_custom_operator() {
    Pipeline pipeline = BeamFunctions.createPipeline("Min value transformation");
    PCollection<Integer> numbers = pipeline.apply(Create.of(Arrays.asList(1, 2, 3, -4, 5, 6)));

    PCollection<Integer> lowerAndEven = numbers.apply(Min.globally(Comparators.LOWER_AND_EVEN));
    PAssert.that(lowerAndEven).containsInAnyOrder(-4);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void should_get_mean_value_of_numeric_values() {
    Pipeline pipeline = BeamFunctions.createPipeline("Mean value of numeric collection");
    PCollection<Integer> numericCollection =
        pipeline.apply(
            Create.of(IntStream.rangeClosed(1, 20).boxed().collect(Collectors.toList())));
    PCollection<Double> meanCollection = numericCollection.apply(Mean.globally());
    PAssert.that(meanCollection).containsInAnyOrder(10.5d);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void should_get_first_2_elements() {
    Pipeline pipeline = BeamFunctions.createPipeline("Top 2 Elements");
    PCollection<Integer> numericCollection =
        pipeline.apply(
            Create.of(IntStream.rangeClosed(1, 20).boxed().collect(Collectors.toList())));
    PCollection<List<Integer>> top2ValuesCollection = numericCollection.apply(Top.largest(2));
    PAssert.that(top2ValuesCollection).containsInAnyOrder(Arrays.asList(20, 19));
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void should_get_2_first_values_per_key() {
    Pipeline pipeline = BeamFunctions.createPipeline("Top 2 per key tranformations");
    PCollection<KV<String, Integer>> customerOrders =
        pipeline.apply(
            Create.of(
                Arrays.asList(
                    KV.of("C#1", 100),
                    KV.of("C#2", 108),
                    KV.of("C#3", 120),
                    KV.of("C#1", 209),
                    KV.of("C#1", 210),
                    KV.of("C#1", 200),
                    KV.of("C#2", 450))));
    PCollection<KV<String, List<Integer>>> top2Orders = customerOrders.apply(Top.largestPerKey(2));
    PAssert.that(top2Orders)
        .containsInAnyOrder(
            KV.of("C#1", Arrays.asList(210, 209)),
            KV.of("C#2", Arrays.asList(450, 108)),
            KV.of("C#3", Collections.singletonList(120)));
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void should_get_first_value_with_custom_comparator() {
    Pipeline pipeline = BeamFunctions.createPipeline("Top 1 Custom comparator tranformations");
    PCollection<KV<String, Integer>> customerOrders =
        pipeline.apply(
            Create.of(
                Arrays.asList(
                    KV.of("C#1", 100),
                    KV.of("C#2", 108),
                    KV.of("C#3", 120),
                    KV.of("C#1", 209),
                    KV.of("C#1", 210),
                    KV.of("C#1", 200),
                    KV.of("C#2", 450))));

    PCollection<KV<String, List<Integer>>> topEvenCollection =
        customerOrders.apply(Top.perKey(1, Comparators.BIGGER_AND_EVEN));

    PAssert.that(topEvenCollection)
        .containsInAnyOrder(
            KV.of("C#1", Arrays.asList(210)),
            KV.of("C#2", Arrays.asList(450)),
            KV.of("C#3", Collections.singletonList(120)));
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void should_retrieve_values_and_keys_of_key_value_pair() {
    Pipeline pipeline = BeamFunctions.createPipeline("Amount Value retrieval");
    PCollection<KV<String, Integer>> customerOrders =
        pipeline.apply(
            Create.of(
                Arrays.asList(
                    KV.of("C#1", 100),
                    KV.of("C#2", 108),
                    KV.of("C#3", 120),
                    KV.of("C#1", 209),
                    KV.of("C#1", 210),
                    KV.of("C#1", 200),
                    KV.of("C#2", 450))));
    PCollection<Integer> amounts = customerOrders.apply(Values.create());
    PCollection<String> customer = customerOrders.apply(Keys.create());

    PAssert.that(amounts).containsInAnyOrder(100, 108, 120, 209, 210, 200, 450);
    PAssert.that(customer).containsInAnyOrder("C#1", "C#1", "C#1", "C#1", "C#2", "C#2", "C#3");
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void should_sample_3_numbers() {
    Pipeline pipeline = BeamFunctions.createPipeline("Sample 3 numbers");
    PCollection<Integer> numbersCollection =
        pipeline.apply(
            Create.of(IntStream.rangeClosed(1, 20).boxed().collect(Collectors.toList())));
    PCollection<Iterable<Integer>> sampledCollection =
        numbersCollection.apply(Sample.fixedSizeGlobally(3));

    PAssert.that(sampledCollection)
        .satisfies(
            input -> {
              Set<Integer> distinctNumbers = new HashSet<>();
              for (Iterable<Integer> ints : input) {
                for (int number : ints) {
                  distinctNumbers.add(number);
                }
              }
              assert distinctNumbers.size() == 3;
              return null;
            });
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void should_group_by_customer() {
    Pipeline pipeline = BeamFunctions.createPipeline("Group by key transformation");
    PCollection<KV<String, Integer>> customerOrders =
        pipeline.apply(
            Create.of(
                Arrays.asList(
                    KV.of("C#1", 100),
                    KV.of("C#2", 108),
                    KV.of("C#3", 120),
                    KV.of("C#1", 209),
                    KV.of("C#1", 210),
                    KV.of("C#1", 200),
                    KV.of("C#2", 450))));

    PCollection<KV<String, Iterable<Integer>>> groupByKeyCollection =
        customerOrders.apply(GroupByKey.create());

    PAssert.that(groupByKeyCollection)
        .satisfies(
            input -> {
              Map<String, List<Integer>> expected = new HashMap<>();
              expected.put("C#1", Arrays.asList(210, 200, 209, 100));
              expected.put("C#2", Arrays.asList(108, 450));
              expected.put("C#3", Arrays.asList(120));

              for (KV<String, Iterable<Integer>> keyValues : input) {
                List<Integer> expectedOrderAmounts = expected.get(keyValues.getKey());
                Iterable<Integer> value = keyValues.getValue();
                assertThat(keyValues.getValue()).containsOnlyElementsOf(expectedOrderAmounts);
              }
              return null;
            });
  }

  @Test
  public void should_join_the_elements_of_2_collection() {
    Pipeline pipeline = BeamFunctions.createPipeline("Group by key transformation");
    PCollection<KV<String, Integer>> elements1 =
        pipeline.apply(
            Create.of(KV.of("A", 1), KV.of("B", 10), KV.of("A", 5), KV.of("A", 3), KV.of("B", 11)));
    PCollection<KV<String, Integer>> elements2 =
        pipeline.apply(
            Create.of(KV.of("A", 6), KV.of("B", 12), KV.of("A", 4), KV.of("A", 2), KV.of("C", 20)));

    TupleTag<Integer> tupleTag1 = new TupleTag<>();
    TupleTag<Integer> tupleTag2 = new TupleTag<>();

    KeyedPCollectionTuple<String> tupleCollection =
        KeyedPCollectionTuple.of(tupleTag1, elements1).and(tupleTag2, elements2);
    PCollection<KV<String, CoGbkResult>> groupByKeyCollection =
        tupleCollection.apply(CoGroupByKey.create());

    PAssert.that(groupByKeyCollection)
        .satisfies(
            input -> {
              Map<String, List<Integer>> expected = new HashMap<>();
              expected.put("A", Arrays.asList(1, 2, 3, 4, 5, 6));
              expected.put("B", Arrays.asList(10, 11, 12));
              expected.put("C", Arrays.asList(20));
              for (KV<String, CoGbkResult> result : input) {
                Iterable<Integer> allFrom1 = result.getValue().getAll(tupleTag1);
                Iterable<Integer> allFrom2 = result.getValue().getAll(tupleTag2);
                Iterable<Integer> groupedValues = Iterables.concat(allFrom1, allFrom2);
                assertThat(groupedValues).containsOnlyElementsOf(expected.get(result.getKey()));
              }
              return null;
            });
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void should_flat_map_numbers() {
    Pipeline pipeline = BeamFunctions.createPipeline("FlatMap transform");
    PCollection<Integer> integerPCollection = pipeline.apply(Create.of(1, 10, 100));
    PCollection<Integer> flattenNumbers =
        integerPCollection.apply(
            FlatMapElements.into(TypeDescriptors.integers()).via(new Multiplicator(2)));
    PAssert.that(flattenNumbers).containsInAnyOrder(1, 2, 10, 20, 100, 200);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void should_flatten_numbers() {
    Pipeline pipeline = BeamFunctions.createPipeline("Flatten transform");
    PCollection<List<Integer>> numbersFromList =
        pipeline.apply(
            Create.of(
                Arrays.asList(1, 2, 3, 4),
                Arrays.asList(10, 11, 12, 13),
                Arrays.asList(20, 21, 22, 23)));
    PCollection<Integer> flattenNumbers = numbersFromList.apply(Flatten.iterables());

    PAssert.that(flattenNumbers).containsInAnyOrder(1, 2, 3, 4, 10, 11, 12, 13, 20, 21, 22, 23);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void should_flatten_numbers_from_pcollection() {
    Pipeline pipeline = BeamFunctions.createPipeline("Flatten transformation");
    PCollection<Integer> numbers1 = pipeline.apply(Create.of(1));
    PCollection<Integer> numbers2 = pipeline.apply(Create.of(10, 11, 12, 13));
    PCollection<Integer> numbers3 = pipeline.apply(Create.of(20, 21, 22, 23));
    PCollectionList<Integer> numbersList = PCollectionList.of(numbers1).and(numbers2).and(numbers3);
    PCollection<Integer> flattenCollection = numbersList.apply(Flatten.pCollections());
    PAssert.that(flattenCollection).containsInAnyOrder(1, 10, 11, 12, 13, 20, 21, 22, 23);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void should_find_1_occurence_of_regex_expression() {
    Pipeline pipeline = BeamFunctions.createPipeline("Regex find tranform");
    PCollection<String> dataCollection =
        pipeline.apply(Create.of(Arrays.asList("", "a", "", "", "ab", "ab", "abc")));
    PCollection<String> stringPCollection = dataCollection.apply(Regex.find("ab"));
    PAssert.that(stringPCollection).containsInAnyOrder("ab", "ab", "ab");
    pipeline.run();
  }

  @Test
  public void should_find_1_occurence_of_regex_expression_per_group() {
    Pipeline pipeline = BeamFunctions.createPipeline("Regex find in group transform");
    PCollection<String> dataCollection =
        pipeline.apply(Create.of("aa ab c", "ab bb c", "ab cc d", "dada"));

    PCollection<KV<String, String>> foundWords =
        dataCollection.apply(Regex.findKV("(ab) (c)", 1, 2));
    PAssert.that(foundWords).containsInAnyOrder(KV.of("ab", "c"), KV.of("ab", "c"));
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void should_replace_should_split_using_regex() {
    Pipeline pipeline = BeamFunctions.createPipeline("Regex replace and split transformation");
    PCollection<String> stringPCollection =
        pipeline.apply(Create.of(Arrays.asList("aa", "aba", "baba")));

    PCollection<String> replaceCollection = stringPCollection.apply(Regex.replaceAll("a", "1"));

    PCollection<String> firstReplaceCollection =
        stringPCollection.apply(Regex.replaceFirst("a", "1"));

    PCollection<String> splitCollection = stringPCollection.apply(Regex.split("a"));

    pipeline.run().waitUntilFinish();

    PAssert.that(replaceCollection).containsInAnyOrder("11", "1b1", "b1b1");
    PAssert.that(firstReplaceCollection).containsInAnyOrder("1a", "1ba", "b1ba");
    PAssert.that(splitCollection).containsInAnyOrder("a", "a", "a", "ba", "ba", "ba");
  }

  @Test
  public void should_concat_all_words() {
    Pipeline pipeline = BeamFunctions.createPipeline("Combine transformation");
    PCollection<String> dataCollection =
        pipeline.apply(Create.of(Arrays.asList("This", "is", "interesting")));
    PCollection<String> concatenatedWords =
        dataCollection.apply(
            Combine.globally(
                words -> {
                  String concat = "";
                  for (String word : words) {
                    concat += word;
                  }
                  return concat;
                }));
    pipeline.run().waitUntilFinish();
    PAssert.that(concatenatedWords).containsInAnyOrder("Thisisinteresting");
  }

  @Test
  public void should_combine_all_orders_by_customer() {
    Pipeline pipeline = BeamFunctions.createPipeline("Combine per key transformations");
    PCollection<KV<String, Integer>> customerOrders =
        pipeline.apply(
            Create.of(
                Arrays.asList(
                    KV.of("C#1", 100),
                    KV.of("C#2", 108),
                    KV.of("C#3", 120),
                    KV.of("C#1", 209),
                    KV.of("C#1", 210),
                    KV.of("C#1", 200),
                    KV.of("C#2", 450))));

    PCollection<KV<String, Integer>> orderSumPerCustomer =
        customerOrders.apply(
            Combine.perKey(
                amounts -> {
                  int amount = 0;
                  for (int a : amounts) {
                    amount += a;
                  }
                  return amount;
                }));
    pipeline.run().waitUntilFinish();
    PAssert.that(orderSumPerCustomer)
        .containsInAnyOrder(KV.of("C#1", 719), KV.of("C#2", 558), KV.of("C#3", 120));
  }

/*
  @Test
  public void should_apply_timestamp_to_input_elements(){
    Pipeline pipeline = BeamFunctions
        .createPipeline("Apply timestamp transformation");
    PCollection<String> stringPCollection = pipeline.apply(Create.of(Arrays.asList("a", "b")));
    Instant timestampToApply = Instant.now().minus(Duration.ofMinutes(2));
    SerializableFunction<String, Instant> backInTimeFn = input -> new Instant(Long.valueOf(input)).minus(Duration.millis(1000L));
    PCollection<String> itemsWithNewTimestamp = stringPCollection
        .apply(WithTimestamps.of(input -> new Instant(Long.parseLong(input))));


  }
*/

}
