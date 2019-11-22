package com.deep.sample.transformations;

import org.apache.beam.sdk.transforms.SerializableFunction;

public enum Filters implements SerializableFunction<String, Boolean> {
  NON_EMPTY {
    @Override
    public Boolean apply(String input) {
      return !input.isEmpty();
    }
  }
}
