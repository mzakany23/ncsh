# tests

unit testing:
1. ensure no regressions
2. ensure no new test files, fix the ones we have first.
3. be weary of fixing tests by fixing test file, fix lib first if you can - tests should be single source of truth to correctness.

smoke testing (integration):
1. we use the step function to smoke test using a small daily mode run
2. do not call or use the lambdas directly, you should only interface with the step function