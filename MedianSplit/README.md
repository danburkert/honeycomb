Add the MedianSplit logic to HBase:

on HBase 0.92 or CDH 4, add following to `hbase-site.xml`:
```XML
<property>
  <name>hbase.coprocessor.region.classes</name>
  <value>org.apache.hadoop.hbase.regionserver.MedianSplitObserver</value>
</property>
```

