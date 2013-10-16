.doDeployRedis <- function(obj, expr, envir, data)
{
# ID associates the work with a job environment <queue>.env.<ID>. If
# the workers current job environment does not match job ID, they retrieve
# the new job environment data from queueEnv and run workerInit.
  ID_file <- tempfile("doRedis")
  zz <- file(ID_file,"w")
  close(zz)
  ID <- ID_file
# The backslash escape charater present in Windows paths causes problems.
  ID <- gsub("\\\\","_",ID)

  
  #  queue <- data
  # Create the queue for this foreach task.
  queue <- uuid()
  
  # Now request the resources from the resource allocator. 
  # Note that this will change when we have a proper broker.
  payload <- list(type="resource request", queue=queue)

  for (i in 1:data$nWorkers)
    redisRPush(data$queue, payload)

  queueEnv <- paste(queue,"env", ID, sep=".")
  queueOut <- paste(queue,"out", ID, sep=".")
  queueStart <- paste(queue,"start",ID, sep=".")
  queueStart <- paste(queueStart, "*", sep="")
  queueAlive <- paste(queue,"alive",ID, sep=".")
  queueAlive <- paste(queueAlive, "*", sep="")

  if (!inherits(obj, 'foreach'))
    stop('obj must be a foreach object')

  it <- iter(obj)
  argsList <- .to.list(it)
  accumulator <- makeAccum(it)

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
  noexport <- union(obj$noexport, obj$argnames)
  getexports(expr, exportenv, envir, bad=noexport)
  vars <- ls(exportenv)
  if (obj$verbose) {
    if (length(vars) > 0) {
      cat('automatically exporting the following objects',
          'from the local environment:\n')
      cat(' ', paste(vars, collapse=', '), '\n')
    } else {
      cat('no objects are automatically exported\n')
    }
  }
# Compute list of variables to export
  export <- unique(c(obj$export,.doRedisGlobals$export))
  ignore <- intersect(export, vars)
  if (length(ignore) > 0) {
    warning(sprintf('already exporting objects(s): %s',
            paste(ignore, collapse=', ')))
    export <- setdiff(export, ignore)
  }
# Add explicitly exported variables to exportenv
  if (length(export) > 0) {
    if (obj$verbose)
      cat(sprintf('explicitly exporting objects(s): %s\n',
                  paste(export, collapse=', ')))
    for (sym in export) {
      if (!exists(sym, envir, inherits=TRUE))
        stop(sprintf('unable to find variable "%s"', sym))
      assign(sym, get(sym, envir, inherits=TRUE),
             pos=exportenv, inherits=FALSE)
    }
  }
	# export packages
	packages <- unique(c(obj$packages, .doRedisGlobals$packages))
	
# Create a job environment for the workers to use
  redisSet(queueEnv, list(expr=expr, 
                          exportenv=exportenv, packages=packages))
  results <- NULL
  njobs <- length(argsList)
# foreach lets one pass options to a backend with the .options.<label>
# argument. We check for a user-supplied chunkSize option.
# Example: foreach(j=1,.options.redis=list(chuckSize=100)) %dopar% ...
  chunkSize <- 0
  if(exists('chunkSize',envir=.doRedisGlobals))
    chunkSize <- get('chunkSize',envir=.doRedisGlobals)
  if(!is.null(obj$options$redis$chunkSize))
   {
    tryCatch(
      chunkSize <- obj$options$redis$chunkSize - 1,
      error=function(e) {chunkSize <<- 0; warning(e)}
    )
   }
  chunkSize <- max(chunkSize,0)
# We also check for a fault-tolerance check interval (in seconds):
  ftinterval <- 30
  if(!is.null(obj$options$redis$ftinterval))
   {
    tryCatch(
      ftinterval <- obj$options$redis$ftinterval,
      error=function(e) {ftinterval <<- 30; warning(e)}
    )
   }
  ftinterval <- max(ftinterval,1)

# Queue the job(s)
# We encode the job order in names(argsList) XXX This is perhaps not optimal
# since the accumulator requires numeric job tags for ordering.
  nout <- 1
  j <- 1
# XXX XXX To speed this up, we added nonblocking calls to rredis and use them.
  redisSetBlocking(FALSE)
  redisMulti()
  while(j <= njobs)
   {
    k <- min(j+chunkSize,njobs)
    block <- argsList[j:k]
    names(block) <- j:k
    redisRPush(queue, list(ID=ID, argsList=block))
    j <- k + 1
    nout <- nout + 1
   }
   redisExec()
   redisGetResponse(all=TRUE)
   redisSetBlocking(TRUE)

# Collect the results and pass through the accumulator
  j <- 1
  while(j < nout)
   {
    results <- redisBRPop(queueOut, timeout=ftinterval)
    if(is.null(results)) {
# Check for worker fault and re-submit tasks if required...
    started <- redisKeys(queueStart)
    started <- sub(paste(queue,"start","",sep="."),"",started)
    alive <- redisKeys(queueAlive)
    alive <- sub(paste(queue,"alive","",sep="."),"",alive)
    fault <- setdiff(started,alive)
    if(length(fault)>0) {
# One or more worker faults have occurred. Re-sumbit the work.
      fault <- paste(queue, "start", fault, sep=".")
      fjobs <- redisMGet(fault)
      redisDelete(fault)
      for(resub in fjobs) {
        block <- argsList[unlist(resub)]
        names(block) <- unlist(resub)
        if (obj$verbose)
          cat("Worker fault: resubmitting jobs", names(block), "\n")
        redisRPush(queue, list(ID=ID, argsList=block))
      }
    }
    }
    else {
      j <- j + 1
      tryCatch(accumulator(results[[1]], as.numeric(names(results[[1]]))),
        error=function(e) {
          cat('error calling combine function:\n')
          print(e)
      })
    }
   }

# Clean up the session ID and session environment
  unlink(ID_file)
  redisDelete(queueEnv)
  if(redisExists(queueOut)) redisDelete(queueOut)
 
# check for errors
  errorValue <- getErrorValue(it)
  errorIndex <- getErrorIndex(it)

# throw an error or return the combined results
  if (identical(obj$errorHandling, 'stop') && !is.null(errorValue)) {
    msg <- sprintf('task %d failed - "%s"', errorIndex,
                   conditionMessage(errorValue))
    stop(simpleError(msg, call=expr))
  } else {
    getResult(it)
  }
}
