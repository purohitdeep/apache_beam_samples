# apache_beam_samples
Code demonstrating various features of apache beam

**PCollection**
1. Create From Memory
2. Create Pcollection from text file
3. Pcollection should be immutable
4. A single Pcollection is used as input for different transformation 
5. PCollection with different windows
6. Pcollection coder
7. Pcollection metadata check
8. Pcollection list
9. Pcollection tuple

**Transformations**
1. Filter [Empty, character, greaterThan]
2. Mapping (words into their length)
3. Partition (Numbers into different group/partition)
4. Count (Global, Per Element)
5. Distinct (normal, key-value pair)
6. Minimum (Global, Per key, Global with custom rule)
7. Mean (Global)
8. Top (Largest, per key, per key with custom condition)
9. Keys and Values (Fetch keys or the values from element)
10. Sample (Fetches the sample from the dataset)
11. FlatMap
12. Flatten (Pcollection and PCollectionList)
13. Regex (Find, replace, Split)
14. Combine (Globally, Per Key)
