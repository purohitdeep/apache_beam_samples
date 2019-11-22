package com.deep.sample.transformations;

import java.util.Arrays;
import java.util.Collection;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class Multiplicator implements SerializableFunction<Integer, Collection<Integer>> {

  private int factor;

  public Multiplicator(int factor) {
    this.factor = factor;
  }

  @Override
  public Collection<Integer> apply(Integer input) {
    return Arrays.asList(input, input * factor);
  }
}
