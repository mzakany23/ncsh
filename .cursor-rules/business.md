# development rules

1. always use uv for deps
2. we should build new images in ci @deploy
3. we should pip bash to cat so doesn't hang e.g. echo 'foo' | cat -- or echo 'foo' | jq --
4. run tests always before assuming something is correct -- make test
5. we have one step function -- unified which can run via date range