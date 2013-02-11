{:tables [:Honeycomb :InnoDB]
 :warmup 5
 :bench 30
 :clients [1]
 :queries [create-drop]
 :append false
 :out "benchmark.ssv"}
