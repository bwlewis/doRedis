#' Register the Redis back end for foreach.
#'
#' The doRedis package imlpements a simple but flexible parallel back end
#' for foreach that uses Redis for inter-process communication. The work
#' queue name specifies the base name of a small set of Redis keys that the
#' coordinator and worker processes use to exchange data.
#'
#' Back-end worker R processes  advertise their availablility for work
#' with the \code{\link{redisWorker}} function.
#'
#' The doRedis parallel back end tolerates faults among the worker processes and
#' automatically resubmits failed tasks. It is also portable and supports
#' heterogeneous sets of workers, even across operative systems.  The back end
#' supports dynamic pools of worker processes.  New workers may be added to work
#' queues at any time and can be used by running foreach computations.
#'
#' @param queue A work queue name
#' @param host The Redis server host name or IP address
#' @param port The Redis server port number
#' @param password An optional Redis database password
#' @param chunkSize Default iteration granularity, see \code{\link{setChunkSize}}
#' @param ftinterval Default fault tolerance interval in seconds
#' @param progress (logical) Show progress bar for computations?
#' @param ...  Optional arguments passed to \code{\link{redisConnect}}
#'
#' @note
#' All doRedis functions require access to a Redis database server (not included
#' with this package).
#'
#' Worker processes default to same random number generator as
#' the coordinator process by default with seeds set per iteration rather than per
#' worker to yield reproducible output independent of the number of worker
#' processes. The L'Ecuyer-CMRG RNG available from the parallel package is
#' recommended when high-quality distributed pseudorandom numbers are needed.
#' See package vignette for more details and additional options.
#'
#' Avoid using fork-based parallel functions within doRedis expressions.
#' Use of \code{mclapply} and similar functions in the body of a doRedis foreach
#' loop can result in worker faults.
#'
#' @return
#' \code{NULL} is invisibly returned; this function is called for side effect of registering a foreach backend.
#'
#' @examples
#' # Only run if a Redis server is running
#' if (redux::redis_available()) {
#' ## The example assumes that a Redis server is running on the local host
#' ## and standard port.
#'
#' # 1. Start a single local R worker process
#' startLocalWorkers(n=1, queue="jobs", linger=1)
#'
#' # 2. Run a simple sampling approximation of pi:
#' registerDoRedis("jobs")
#' pie = foreach(j=1:10, .combine=sum, .multicombine=TRUE) %dopar%
#'         4 * sum((runif(1000000) ^ 2 + runif(1000000) ^ 2) < 1) / 10000000
#' removeQueue("jobs")
#' print(pie)
#'
#' # Note that removing the work queue automatically terminates worker processes.
#' }
#'
#' @seealso \code{\link{foreach}}, \code{\link{doRedis-package}}, \code{\link{setChunkSize}}, \code{\link{removeQueue}}
#'
#' @import foreach
#' @importFrom iterators nextElem iter
#' @importFrom stats runif
#' @importFrom utils packageDescription flush.console
#' @importFrom redux redis_available
#' @export
registerDoRedis <- function(queue, host="localhost", port=6379, password, ftinterval=30, chunkSize=1, progress=FALSE, ...)
{
  if(missing(password)) redisConnect(host, port, ...)
  else redisConnect(host, port, password=password, ...)
  assign("queue", queue, envir=.doRedisGlobals)
# Set a queue.live key that signals to workers that this queue is
# valid. We need this because Redis removes the key associated with
# empty lists.
  queueLive <- paste(queue, "live", sep=".")
  if(!redisExists(queueLive)) redisSet(queueLive, "")
  if(!missing(ftinterval)) setFtinterval(ftinterval)
  if(!missing(chunkSize)) setChunkSize(chunkSize)
  if(!missing(progress)) setProgress(progress)
  setDoPar(fun=.doRedis, data=list(queue=queue), info=.info)
  invisible()
}

#' Remove a doRedis queue and delete all associated keys from Redis.
#'
#' Removing a doRedis queue cleans up associated keys in the Redis
#' database and signals to workers listening on the queue to terminate.
#' Workers terminate after their timeout period after their work
#' queue is deleted.
#' @param queue the doRedis queue name
#'
#' @note Workers listening for work on more than one queue will only
#' terminate after all their queues have been deleted. See \code{\link{registerDoRedis}}
#' for an example.
#'
#' @return
#' \code{NULL} is invisibly returned; this function is called for the side effect of removing
#' Redis keys associated with the specified queue.
#'
#' @export
removeQueue <- function(queue)
{
  if(redisExists(queue)) redisDelete(queue)
  queueEnv <- redisKeys(pattern=sprintf("%s\\.env.*", queue))
  for (j in queueEnv) redisDelete(j)
  queueOut <- redisKeys(pattern=sprintf("%s\\.out", queue))
  for (j in queueOut) redisDelete(j)
  queueCount <- redisKeys(pattern=sprintf("%s\\.count", queue))
  for (j in queueCount) redisDelete(j)
  queueLive <- redisKeys(pattern=sprintf("%s\\.live", queue))
  for (j in queueLive) redisDelete(j)
  invisible()
}

#' Set the default granularity of distributed tasks.
#'
#' A job is the collection of all tasks in a foreach loop.
#' A task is a collection of loop iterations of at most size \code{chunkSize}.
#' R workers are assigned work by task in blocks of at most
#' \code{chunkSize} loop iterations per task.
#' The default value is one iteration per task.
#' Setting the default chunk size larger for shorter-running jobs can
#' substantially improve performance. Setting this value too high can
#' negatively impact load-balancing across workers, however.
#'
#' @param value positive integer chunk size setting
#'
#' @return \code{value} is invisibly returned; this value is called for its side effect.
#' @examples
#' # Only run if a Redis server is running
#' if (redux::redis_available()) {
#'
#' # Start a single local R worker process
#' startLocalWorkers(n=1, queue="jobs", linger=1)
#'
#' # Register the work queue with the coordinator R process
#' registerDoRedis("jobs")
#' 
#' # Compare verbose task submission output from...
#' setChunkSize(1)
#' foreach(j=1:4, .combine=c, .verbose=TRUE) %dopar% j
#' 
#' # with the verbose task submission output from:
#' setChunkSize(2)
#' foreach(j=1:4, .combine=c, .verbose=TRUE) %dopar% j
#'
#' # Clean up
#' removeQueue("jobs")
#' }
#'
#' @export
setChunkSize <- function(value=1)
{
  if(!is.numeric(value)) stop("setChunkSize requires a numeric argument")
  value <- max(round(value), 1)
  assign("chunkSize", value, envir=.doRedisGlobals)
}

#' Set the fault tolerance check interval in seconds.
#'
#' Failed tasks are automatically re-submitted to the work queue.
#' The \code{setFtinterval} sets an upper bound on how frequently
#' the system checks for failure. See the package vignette for
#' discussion and examples.
#'
#' @param value positive integer number of seconds
#' @return \code{value} is invisibly returned (this function is used for its side effect).
#' @export
setFtinterval <- function(value=30)
{
  if(!is.numeric(value)) stop("setFtinterval requires a numeric argument")
  value <- max(round(value), 3)
  assign("ftinterval", value, envir=.doRedisGlobals)
}



#' Manually add symbol names to the worker environment export list.
#'
#' The setExport function lets users manually declare symbol names
#' of corresponding objects that should be exported to workers.
#'
#' The \code{foreach} function includes a similar \code{.export} parameter.
#'
#' We provide this supplemental export option for users without direct access
#' to the \code{foreach} function, for example, when \code{foreach} is used
#' inside another package.
#'
#' @param names A character vector of symbol names to export.
#'
#' @return The value of \code{names} is invisibly returned (this function is used ofr its side effect).
#'
#' @examples
#' \dontrun{
#' registerDoRedis("work queue")
#' startLocalWorkers(n=1, queue="work queue", linger=1)
#'
#' f <- function() pi
#'
#' (foreach(1) %dopar% tryCatch(eval(call("f")), error = as.character))
#' # Returns the error converted to a message:
#' # Error in eval(call("f")) : task 1 failed - could not find function "f"
#'
#' # Manually export the symbol f:
#' setExport("f")
#' (foreach(1) %dopar% eval(call("f")))
#' # Now f is found.
#'
#' removeQueue("work queue")
#' }
#'
#' @export
setExport <- function(names=c())
{
  assign("export", names, envir=.doRedisGlobals)
}

#' Manually set package names in the worker environment package list.
#'
#' The \code{setPackages} function lets users manually declare packages
#' that R worker processes need to load before running their tasks.
#'
#' The \code{foreach} function includes a similar \code{.packages} parameter.
#'
#' Defines a way to set the foreach \code{.packages} option for users without direct access
#' to the \code{foreach} function, for example, when \code{foreach} is used
#' inside another package.
#'
#' @param packages A character vector of package names.
#'
#' @return The value of \code{packages} is invisibly returned (this function is used for its side effect).
#'
#' @export
setPackages <- function(packages=c())
{
  assign("packages", packages, envir=.doRedisGlobals)
}

#' Progress bar
#' @param value if \code{TRUE}, display a text progress bar indicating status of the computation
#' @return \code{value} is invisibly returned (this function is used for its side effect).
#' @importFrom utils txtProgressBar setTxtProgressBar packageName
#' @export
setProgress <- function(value=FALSE)
{
  assign("progress", value, envir=.doRedisGlobals)
}

# An internal foreach function required of backends The number of workers
# reported here is only an estimate.
.info <- function(data, item)
{
    switch(item,
           workers=
             tryCatch(
               {
                 n <- redisGet(
                         paste(.doRedisGlobals$queue,"count",sep="."))
                 if(length(n) == 0) n <- 0
                 else n <- as.numeric(n)
               }, error=function(e) 0),
           name="doRedis",
           version=packageDescription("doRedis", fields="Version"),
           NULL)
}

# internal function used for its environment, see below for use
.makeDotsEnv <- function(...)
{
  list(...)
  function() NULL
}

#' internal function called by foreach
#' @param obj a foreach object
#' @param expr the expression to evaluate
#' @param envir the expression environment
#' @param data a list of parameters from registerDoRedis
#' @return the foreach result
#' @importFrom redux redis
.doRedis <- function(obj, expr, envir, data)
{
  if (!inherits(obj, "foreach"))
    stop("obj must be a foreach object")

# ID associates the work with a job environment <queue>.env.<ID>. If
# the workers current job environment does not match job ID, they retrieve
# the new job environment data from queueEnv and run workerInit.
  ID <- Sys.getpid()
  ID <- paste( ID, Sys.info()["user"], Sys.info()["nodename"], format(Sys.time(), "%Y-%m-%d-%H:%M:%OS3"), sep="_")
  ID <- gsub(" ", "-", ID)
  queue <- data$queue
  queueEnv <- paste(queue,"env", ID, sep=".")
  queueOut <- paste(queue,"out", ID, sep=".")
  queueStart <- paste(queue,"start", ID, sep=".")
  queueStart <- paste(queueStart, "*", sep="")
  queueAlive <- paste(queue,"alive", ID, sep=".")
  queueAlive <- paste(queueAlive, "*", sep="")

# packageName function added in R 3.0.0
  parentenv <- packageName(envir)

# Clean up the session ID and session environment
  on.exit(
  {
    if(redisExists(queueEnv)) redisDelete(queueEnv)
    if(redisExists(queueOut)) redisDelete(queueOut)
  })

  it <- iter(obj)
  argsList <- .to.list(it)

# Progress bar
  .progress <- FALSE
  if(!is.null(obj$options$redis$progress))
  {
    warning(".options.redis use is deprecated; use setProgress or registerDoRedis options instead.")
    .progress <- obj$options$redis$progress
  }
  if(exists("progress", envir=.doRedisGlobals))
    .progress <- get("progress", envir=.doRedisGlobals)

# Setup the parent environment by first attempting to create an environment
# that has '...' defined in it with the appropriate values
  exportenv <- tryCatch({
    qargs <- quote(list(...))
    args <- eval(qargs, envir)
    environment(do.call(.makeDotsEnv, args))
  },
  error=function(e) {
    new.env(parent=emptyenv())
  })
  exportenv[[".RNGkind"]] = RNGkind()[[1]]
  noexport <- union(obj$noexport, obj$argnames)
  obj$packages = unique(c(obj$packages, getexports(expr, exportenv, envir, bad=noexport), .doRedisGlobals$packages, parentenv))
  vars <- ls(exportenv)
  if(obj$verbose) {
    if(length(vars) > 0) {
      cat("automatically exporting the following objects",
          "from the local environment:\n")
      cat(" ", paste(vars, collapse=", "), "\n")
    } else {
      cat("no objects are automatically exported\n")
    }
    if(length(obj$packages) > 0) {
      cat("exporting the following package requirements\n")
      cat(paste(obj$packages, collapse=", "), "\n")
    } else {
      cat("no package dependencies are automatically exported\n")
    }
    if(!is.null(parentenv)) cat("parent environment: ", parentenv, "\n")
  }
# Compute list of variables to export
  export <- unique(c(obj$export, .doRedisGlobals$export))
  ignore <- intersect(export, vars)
  if (length(ignore) > 0) {
    warning(sprintf("already exporting objects(s): %s",
            paste(ignore, collapse=", ")))
    export <- setdiff(export, ignore)
  }
# Add explicitly exported variables to exportenv
  if (length(export) > 0) {
    if (obj$verbose)
      cat(sprintf('explicitly exporting variables(s): %s\n',
                  paste(export, collapse=', ')))

    for (sym in export) {
      if (!exists(sym, envir, inherits=TRUE))
        stop(sprintf('unable to find variable "%s"', sym))
      val <- get(sym, envir, inherits=TRUE)
      if (is.function(val) &&
          (identical(environment(val), .GlobalEnv) ||
           identical(environment(val), envir))) {
        # Changing this function's environment to exportenv allows it to
        # access/execute any other functions defined in exportenv.  This
        # has always been done for auto-exported functions, and not
        # doing so for explicitly exported functions results in
        # functions defined in exportenv that can't call each other.
        environment(val) <- exportenv
      }
      assign(sym, val, pos=exportenv, inherits=FALSE)
    }
  }

# Upload `exportenv` and related data as common job data for the workers
# making sure the data fit in Redis.
  if(object.size(exportenv) > REDIS_MAX_VALUE_SIZE)
  {
    message("The exported environment size is too large.\nConsider breaking up your data across multiple Redis keys.")
    stop("exportenv too big")
  }
  results <- NULL

  ntasks <- length(argsList)

  chunkSize <- 0
  if(!is.null(obj$options$redis$chunkSize))
  {
    warning(".options.redis use is deprecated; use setChunkSize or registerDoRedis options instead.")
    chunkSize <- obj$options$redis$chunkSize
  }
  if(exists("chunkSize", envir=.doRedisGlobals))
    chunkSize <- get("chunkSize", envir=.doRedisGlobals)
# Accept lower case too
  if(!is.null(obj$options$redis$chunksize))
  {
    warning(".options.redis use is deprecated; use setChunkSize or registerDoRedis options instead.")
    chunkSize <- obj$options$redis$chunksize
  }
  if(exists("chunksize", envir=.doRedisGlobals))
    chunkSize <- get("chunksize", envir=.doRedisGlobals)

  chunkSize <- tryCatch(max(chunkSize - 1, 0), error=function(e) 0)

  redisSet(queueEnv, list(expr=expr, parentenv=parentenv,
                                 exportenv=exportenv, packages=obj$packages))
# Check for a fault-tolerance check interval (in seconds), do not
# allow it to be less than 3 seconds (cf alive.c thread code in the worker).
  ftinterval <- 30
  if(!is.null(obj$options$redis$ftinterval))
  {
    warning(".options.redis use is deprecated; use setFtInterval or registerDoRedis options instead.")
    ftinterval <- obj$options$redis$ftinterval
  }
  if(exists("ftinterval", envir=.doRedisGlobals))
    ftinterval <- get("ftinterval", envir=.doRedisGlobals)
  ftinterval <- max(ftinterval, 3)

# Queue the task(s)
# The task order is encoded in names(argsList).
  nout <- 1
  j <- 1
  done <- c()  # A vector of completed tasks
  blocknames <- list() # List of block names

# use nonblocking call to submit all tasks at once
# NOTE! May-2020: avoid pipelining for now due to likely, now confirmed, bug in hiredis.
# See https://github.com/bwlewis/doRedis/issues/56
#  commands <- redis$MULTI()
  redisMulti()
  while(j <= ntasks)
  {
    k <- min(j + chunkSize, ntasks)
    block <- argsList[j:k]
    if(is.null(block)) break
    names(block) <- j:k
    blocknames <- c(blocknames, list(names(block)))
    if(obj$verbose) message("Submitting task(s) ", j, ":", k)
#    commands <- c(commands, list(redis$RPUSH(queue, serialize(list(ID=ID, argsList=block), NULL))))
    redisRPush(queue, list(ID=ID, argsList=block))
    j <- k + 1
    nout <- nout + 1
  }
  redisExec()
  accumulator <- makeAccum(it)

# Collect the results and pass through the accumulator
# Note! at this point, nout = number of tasks + 1
  ctx <- redisGetContext()
  j <- 1
  if (.progress)
  {
    pb <- txtProgressBar(1, nout - 1, style=3)
    on.exit(close(pb), add=TRUE)
  }
tryCatch(
{
  while(j < nout)
  {
    retry <- TRUE
    recon <- 1L
    if (.progress) setTxtProgressBar(pb, j)
    while(retry)
    {
      results <- tryCatch(redisBRPop(queueOut, timeout=ftinterval),
                   error=function(e)
                   {
                     if(is.numeric(recon))
                     {
                       message("\nInterrupted connection to Redis!")
                       message("doRedis will periodically retry connecting to Redis.\n",
                               "Press CTRL + C (or the stop button in RStudio) to break out of this loop, maybe more than once.")
                     } else cat(".")
# XXX what about passwords, nodelay and timeout in the reconnect?
                     recon <<- tryCatch(redisConnect(host=ctx$host, port=ctx$port), error=function (e) TRUE)
                     if(is.null(recon)) message("Connection to Redis reestablished!")
                     else Sys.sleep(max(floor(ftinterval / 3), 10))
                     e
                   })
      retry <- inherits(results, "condition")
    }
    if(is.null(results))
    {
      # Check for worker fault and re-submit tasks if required...
      # This detects asymmetry between started and alive processes,
      # resubmitting started tasks whose workers are no longer alive.
      started <- sub(paste(queue, "start", "", sep="."), "", redisKeys(queueStart))
      alive <- sub(paste(queue, "alive", "",sep="."), "", redisKeys(queueAlive))
      fault <- setdiff(started,alive)
      if(length(fault) > 0)
      {
        # One or more worker faults have occurred. Re-sumbit the work.
        fault <- paste(queue, "start", fault, sep=".")
        fjobs <- redisMGet(fault)
        redisDelete(fault)
        for (resub in fjobs) {
          block <- argsList[unlist(resub)]
          names(block) <- unlist(resub)
          warning(sprintf("Worker fault: resubmitting job(s) %s", sprintf("%s..%s", names(block)[1],
                           names(block)[length(names(block))])), immediate.=TRUE)
          redisRPush(queue, list(ID=ID, argsList=block))
        }
      }
      # Check for lost results
      qlen <- as.integer(redisLLen(queue))
      if(qlen == 0 && length(started) == 0)
      {
        resub_init <- TRUE
        for(resub in setdiff(1:(nout - 1), done))
        {
          if(resub_init)
          {
            # Reset the job environment just in case
            redisSet(queueEnv, list(expr=expr, exportenv=exportenv, packages=obj$packages))
            resub_init <- FALSE
          }
          block <- argsList[resub]
          names(block) <- blocknames[[resub]]
          warning(sprintf("Lost result: resubmitting task %s", names(block)), immediate.=TRUE)
          redisRPush(queue, list(ID=ID, argsList=block))
        }
      }
    }
    else
    {
      j <- j + 1
      n <- as.numeric(names(results[[1]]))
      done <- c(done, n)
      tryCatch(accumulator(results[[1]], n),
        error=function(e) {
          cat("error calling combine function:\n", file=stderr())
          print(e)
      })
    }
  }
}, interrupt=function(e)
   {
     flushQueue(queue, ID)
   })
#, error=function(e)
#   {
#     flushQueue(queue, ID)
#   })


# check for errors
  errorValue <- getErrorValue(it)
  errorIndex <- getErrorIndex(it)

# throw an error or return the combined results
  if (identical(obj$errorHandling, "stop") && !is.null(errorValue)) {
    msg <- sprintf("task %d failed - \"%s\"", errorIndex,
                   conditionMessage(errorValue))
    stop(simpleError(msg, call=expr))
  } else {
    getResult(it)
  }
}

# internal function to deal with user interrupt
# clean up redis work queue, removing all tasks in the job defined by ID
flushQueue <- function(queue, ID)
{
  startkeys <- redisKeys(pattern=sprintf("%s.start*",queue))
#  redisSetPipeline(TRUE)
#  redisMulti()
  tasks <- redisLRange(queue,0L,1000000000L)  # retrieve everything on the work queue
  tryCatch(redisDelete(queue), error=function(e) NULL) # delete the queue
  if(!is.null(startkeys)) tryCatch(redisDelete(startkeys), error=function(e) NULL)
#  tasks <- redisExec()
#  tasks <- redisGetResponse(all=TRUE)
#  redisSetPipeline(FALSE)
# Re-queue jobs not matching ID (these are other jobs submitted to the queue).
# First we need to locate the IDs, if any, in the result.
# XXX FIXME
  idx <- grep("ID", tasks)
  if(length(idx) == 0) return()
  lapply(tasks[[idx]][[1]], function(j)
  {
    if(!isTRUE(j[["ID"]] == ID)) redisRPush(queue, list(ID=j[["ID"]], argsList=j[["argsList"]]))
  })
}

# Convert the iterator to a list
#
# @param x an iterator
# @return A list with two entries per element. The first entry is the
# corresponding iterator value. The 2nd is a random seed, (not) a L'Ecuyer
# random seed value, rolling with whatever RNG is in use on the
# coordinator--see the .workerInit function in redisWorker.R.  Because doRedis
# is an anonymous work queue with an unknown (indeed, variable) nunmber of
# workers, reproducibility requires that we encode random seeds with each taks.
# @keywords internal
.to.list <- function(x)
{
  n <- 64
  a <- vector("list", length=n)
  i <- 0L
  tryCatch({
    repeat {
      if (i >= n) {
        n <- 2 * n
        length(a) <- n
      }
      seed <- tryCatch(as.integer(i + 1L), error = function(e) 0L)
      rs <- list(.Random.seed=seed)
      a[[i + 1]] <- c(nextElem(x), rs)
      i <- i + 1
    }
  },
  error=function(e) {
    if (!identical(conditionMessage(e), "StopIteration"))
      stop(e)
  })
  length(a) <- i
  a
}

#' List doRedis jobs
#' @param queue List jobs for the specified queue, or set to "*" to list jobs for all queues
#' @return a data frame listing jobs by row with variables queue, id, user, host and time (submitted).
#' @export
jobs <- function(queue="*")
{
  x <- redisKeys(paste(queue, ".env.*", sep=""))
  if(length(x) < 1) return(data.frame())
  ans <- data.frame(rbind(Reduce(rbind,
           lapply(strsplit(x, "_"), function(x) c(strsplit(x[1], "\\.")[[1]][-2], x[-1])))),
             stringsAsFactors=FALSE, row.names=NULL)
  names(ans) <- c("queue", "id", "user", "host", "time")
  ans
}

#' List running doRedis tasks
#' @param queue List jobs for the specified queue, or set to "*" to list jobs for all queues
#' @param id List tasks for the specified job id, or set to "*" to list tasks for all job ids
#' @return a data frame listing jobs by row with variables queue, id, user, coordinator, time, iter, host, pid (see Note)
#' @note The returned values indicate
#' \enumerate{
#' \item \code{queue} the doRedis queue name
#' \item \code{id} the doRedis job id
#' \item \code{user} the user running the job
#' \item \code{coordinator} the host name or I.P. address where the job was submitted (and the coordinator R process runs)
#' \item \code{time} system time on the worker node when the task was started
#' \item \code{iter} the loop iterations being run by the task
#' \item \code{host} the host name or I.P. address where the task is running
#' \item \code{pid} the process ID of the R worker running the task on \code{host}
#' }
#' Tasks are listed until a key associated with them expires in Redis. Thus running tasks
#' are not explicitly removed from the task list immediately when they terminate, but may
#' linger on the list for a short while after (a few seconds).
#' @export
tasks <- function(queue="*", id="*")
{
  x <- redisKeys(sprintf("%s.alive.%s*", queue, id))
  if(length(x) < 1) return(data.frame())
# I know, this is a bit much...
  ans <- data.frame(rbind(Reduce(rbind,
           lapply(lapply(strsplit(x, "_"),
             function(x) c(strsplit(x[1], "\\.")[[1]], x[-1])),
               function(x) c(x[-c(2,6)], strsplit(x[6], " ")[[1]][c(8,2,4,6)])))), stringsAsFactors=FALSE, row.names=NULL)
  names(ans) <- c("queue", "id", "user", "coordinator", "time", "iter", "host", "pid")
  ans$time <- gsub("\\.iters", "", ans$time)
  ans
}

#' Remove Redis keys associated with one or more doRedis jobs
#' @param job Either a named character vector with "queue" and "id" entries corresponding to a doRedis
#'  job queue and job id, or a list with equal-length "queue" and "id" entries, or a data frame with
#'  "queue" and "id" entries, for example as returned by \code{\link{jobs}}.
#' @return \code{NULL} is invisibly returned; this function is used for its side effect--in particular, removing all Redis keys associated with the specified job.
#' @export
removeJob <- function(job)
{
  if(is.data.frame(job))
  {
    job <- as.list(job)
  }
  if(!all(c("queue", "id") %in% names(job))) stop("job must include named queue and id fields")
  flushQueue(job$queue, job$id)
  patterns <- sprintf("%s*", paste(job[["queue"]], job[["id"]], sep=".*."))
  ans <- lapply(patterns, function(k)
  {
    tryCatch(redisDelete(redisKeys(k)), error=function(e) warning(e))
  })
  invisible()
}

#' Print a timestamped message to the  standard error stream.
#'
#' Use to help debug remote doRedis workers.
#' @param msg a character message to print to the standard error stream
#' @return The character message that was printed, decorated with time and system info.
#' @export
logger <- function(msg)
{
  msg <- paste("@", Sys.time(), Sys.info()["nodename"], Sys.getpid(), msg, sep=" ")
  cat(msg, "\n", file=stderr())
  msg
}
