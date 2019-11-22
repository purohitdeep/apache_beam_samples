package com.deep.sample.transformations;

import java.util.Comparator;

public enum Comparators implements Comparator<Integer> {
  LOWER_AND_EVEN {
    @Override
    public int compare(Integer comparedNumber, Integer toCompare) {
      boolean isEvenCompared = comparedNumber % 2 == 0;
      boolean isEvenToCompare = toCompare % 2 == 0;
      if (isEvenCompared && !isEvenToCompare) {
        return -1;
      } else if (!isEvenCompared) {
        return 1;
      } else {
        return comparedNumber > toCompare ? 1 : -1;
      }
    }
  },
  BIGGER_AND_EVEN {
    @Override
    public int compare(Integer comparedNumber, Integer toCompare) {
      boolean isEvenCompared = comparedNumber%2 == 0;
      boolean isEvenToCompare = toCompare%2 == 0;
      if (isEvenCompared && !isEvenToCompare) {
        return 1;
      } else if (!isEvenCompared) {
        return -1;
      } else {
        return comparedNumber > toCompare ? 1 : -1;
      }
    }
  }
}
