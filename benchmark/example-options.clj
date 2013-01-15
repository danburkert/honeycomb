{:tables [:inno_05 :inno_06]
 :warmup 10
 :bench 60
 :clients [1 5 10]
 :queries [range-salary point-name]
 :append false
 :out "bench.ssv"}
