# IPC Drive

This will be a drive that uses `libublk` to store data by passing to slave processes.
These slave processes simply echo back data they receive from stdin to stdout, with a configurable delay.

## Configuring

The default configuration usually is fine, but generally, these rules apply:

1. A longer delay for the slave's response usually increases stability.
2. The less blocks, the better.
3. The less slave processes that need to be spawned, the better.
4. Don't be too lenient with timeouts, but don't be too restrictive either, especially if the slave's response delay is long.
