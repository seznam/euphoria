#BEAM-IO
  EuphoriaIO is read connector to Beam, where DataSource is transformed to PCollection.
  It's useful for smoother transition between Euphoria and Beamphoria
  
  E.g.:
```java
 PCollection<KV<Text, ImmutableBytesWritable>> input =
   pipeline.apply(EuphoriaIO.read(dataSource, kvCoderTextBytes));
```