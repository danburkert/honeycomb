{:engines [:InnoDB :Honeycomb]
 :ddls [create-drop]
 :warmup 10
 :bench 30
 :clients [1]
 :append false
 :out "benchmark.ssv"}
