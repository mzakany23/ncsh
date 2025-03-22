# Post-Migration Tasks

This document tracks tasks that need to be completed after the successful migration to the new unified workflow with batching.

## Infrastructure Cleanup

- [ ] Remove the original Step Function (`ncsoccer-unified-workflow`) after confirming the new batched workflow is stable in production
- [ ] Clean up any IAM roles, policies, and EventBridge rules associated with the old workflow
- [ ] Update documentation to remove references to the old workflow patterns

## Workflow Migration Timeline

1. **Phase 1: Testing** (Current)
   - Keep both workflows live
   - Run tests against the batched workflow
   - Compare results between both for validation

2. **Phase 2: Gradual Migration**
   - Enable batched workflow's daily and monthly rules
   - Monitor for any issues
   - Keep original workflow as a fallback

3. **Phase 3: Complete Migration**
   - Disable original workflow's rules
   - Complete validation period (2 weeks)
   - Remove original workflow infrastructure

## Success Criteria for Workflow Migration

- [x] Successfully deploy batched workflow
- [ ] Successfully run daily operations for 1 week
- [ ] Successfully run monthly operation (on 1st of month)
- [ ] Confirm no Lambda timeouts during monthly operation
- [ ] Verify data quality and completeness compared to original workflow

## Additional Optimizations for Future

- [ ] Consider increasing the maximum concurrency in the Map state from 5 to a higher value based on performance metrics
- [ ] Evaluate batch size to find optimal balance between parallelism and overhead
- [ ] Add additional CloudWatch metrics for batched workflow performance monitoring
- [ ] Create dashboard for at-a-glance workflow health monitoring