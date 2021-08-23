# doRedis: A parallel backend for foreach using Redis.

## Version 3.0.0

- Removed the problematic setReduce function (an API change, but probably
  very little effect on users since it was not widely used and deprecated in
  2.0.0 anyway).
- Simplified default behavior of RNG, see vignette (a significant change,
  but also mostly painless for users).


## Important changes

Version 3 and greater removed default use of L'Ecuyer RNG (repeatable
non-L'Ecuyer RNGs are available by default). See the package vignette for
details.

Version 2 and greater now depend on the redux package (see
https://cran.r-project.org/package=redux) for communication with Redis instead
of the deprecated rredis package.

## Important Redis configuration notes

Set the following parameter in your redis.conf file before using doRedis:

```
timeout 0
```

Exercise caution when using `doRedis` together with `doMC` or any  fork-based R
functions like `mclapply`. If you require a local inner parallel code section,
consider using `parLapply` and `makePSOCKcluster` or the related `doParallel`
functions instead of fork-based methods. The fork-based functions can work in
some cases, but might also lead to trouble because the children share certain
resources with the parent process like open socket descriptors. I have in
particular run in to trouble with some fast BLAS libraries and fork--in
particular the AMD ACML can't be used in this way at all. Again, exercise
caution with fork and `doRedis`!

## Description

Steve Weston's foreach package is a remarkable parametric evaluation device for
the R language. Similarly to lapply-like functions, foreach maps and parameter
values expressions to data and aggregates results. Even better, foreach lets
you do this in parallel across multiple CPU cores and computers.  And even
better yet, foreach abstracts the parallel computing details away into modular
back-end code. Code written using foreach works sequentially in the absence of
a parallel back-end, and works uniformly across a variety of back ends.
Think of foreach as the lingua Franca of parallel computing for R.

Redis is a powerful, fast networked database with many innovative features,
among them a blocking stack-like data structure (Redis "lists"). This feature
makes Redis useful as a lightweight backend for parallel computing.  The
doRedis package relies on the redux package for communication with a Redis
server to define a lightweight parallel backend for foreach using Redis that is
elastic and platform-independent.

Here is a quick example procedure for experimenting with doRedis:

1. Install Redis on your computer.
2. Install foreach, redux and doRedis packages.
3. Start the redis server running (see the redis documentation). We assume
   that the server is running on the host "localhost" and port 6379 (the
   default Redis port). We assume in the examples below that the worker R
   processes and the master are running on the same machine. In practice,
   they can of course run across a network.
4. Open one or more R sessions that will act as back-end worker processes. 
   Run the following in each session:
```r
   require('doRedis')
   redisWorker('jobs')
```
   (The R session will display status messages but otherwise block for
   work.)
   Note: You can add more workers to a work queue at any time. Also note
   that each back-end worker may advertise for work on multiple queues
   simultaneously (see the documentation and examples).
5. Open another R session that will act as the master process. Run the
   following example (a simple sampling approximation of pi):
```r
   require('doRedis')
   registerDoRedis('jobs')
   foreach(j=1:10,.combine=sum, .multicombine=TRUE) %dopar%
            4*sum((runif(1000000) ^ 2 + runif(1000000) ^ 2) < 1) / 10000000
   removeQueue('jobs')
```

Let's define a few terms before we describe how the above example works:

* A _loop iteration_ is the foreach expression together with a single
  loop parameter value.
* A _task_ is a collection of loop iterations.
* Given a foreach expression, a _job_ is the collection of tasks that
  make up the full set of loop iterations.
* A _work queue_ is a collection of of any number of _tasks_ associated
  with number of _jobs_ submitted by one or more master R processes.

The "jobs" parameter above in  the `redisWorker` and `registerDoRedis` function
names a Redis key used to transfer data between master and worker processes.
Think of this name as a reference to a work queue. The master places tasks into
the queue, worker R processes pull tasks out of the queue and then return their
results to an associated result queue.

The doRedis parallel  backend supports dynamic pools of back-end workers.  New
workers may be added to work queues at any time and can be immediately used by
running foreach computations.

The doRedis backend accepts a parameter called `chunkSize` that sets the number
of loop iterations doled out per task, by default one. Optionally set this with
the `setChunkSize` function. Increasing `chunkSize` can improve performance for
quick-running function evaluations by cutting down on the number of tasks.
Here is an example that sets `chunkSize` to 100:

```r
foreach(j=1:500, .options.redis=list(chunkSize=100)) %dopar%  ...
```

Setting `chunkSize` too large will adversely impact load-balancing across
the workers. For instance, setting `chunkSize` to the total number of loop
iterations will run everything sequentially on one worker!

The `redisWorker` function is used to manually invoke worker processes that
advertise for job tasks on one or more work queues. The function also has
parameters for a Redis host, port number and password. For example, if the
Redis server is running on a host called "Cazart" with the default Redis port
6379:
```
redisWorker('jobs', host='Cazart', port=6379)
```

The `registerDoRedis` function also contains host and port parameters.
Neither the worker nor master R session needs to be running on the same
machine as the Redis server.

The `startLocalWorkers` function invokes one or more background R worker
processes on the _local_ machine (internally using the `redisWorker` function).
It's a convenient way to invoke several workers at once on your local box.

Workers self-terminate when their work queues have been deleted with the
`removeQueue` function.
