# Day 14

Yesterday we saw how backfills can enable certain types of parallelism, but parallel computing is actually a much more common part of orchestration.

Create a data pipeline that contains 5-10 assets that all share one common parent. *Pro Challenge*: can you write an asset factory that creates these more dynamically?

How does the orchestrator run these independent steps? What controls do you have over the parallelism of independent steps?