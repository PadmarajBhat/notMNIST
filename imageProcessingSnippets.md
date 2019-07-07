* problem with the below approach is that there are multiple rows
```
val qrt_a_median = df.filter(df("label") === "A").stat.approxQuantile("median", Array(.25,.5,.75), 0)
val qrt_b_median = df.filter(df("label") === "B").stat.approxQuantile("median", Array(.25,.5,.75), 0)

val qrt_a_mean = df.filter(df("label") === "A").stat.approxQuantile("mean", Array(.25,.5,.75), 0)
val qrt_b_mean = df.filter(df("label") === "B").stat.approxQuantile("mean", Array(.25,.5,.75), 0)


val df_A = df.filter(df("label") === "A" ).filter(df("median") >= qrt_a_median(0) and df("median") <= qrt_a_median(2))
val df_B = df.filter(df("label") === "B" ).filter(df("median") >= qrt_b_median(0) and df("median") <= qrt_b_median(2))

val df_A_mean = df.filter(df("label") === "A" ).filter(df("mean") >= qrt_a_mean(0) and df("mean") <= qrt_a_mean(2))
val df_B_mean = df.filter(df("label") === "B" ).filter(df("mean") >= qrt_b_mean(0) and df("mean") <= qrt_b_mean(2))
```
