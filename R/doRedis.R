#
# Copyright (c) 2010 by Bryan W. Lewis.
#
# This is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License as published
# by the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307
# USA

# The environment initialization code is adapted (with minor changes)
# from the doMPI package from Steve Weston.

# Register the 'doRedis' function with %dopar%.
registerDoRedis <- function(queue, host="localhost", port=6379)
{
  redisConnect(host,port)
  .redisVersionCheck()
  setDoPar(fun=.doRedis, data=queue, info=.info)
}

removeQueue <- function(queue)
{
  redisDelete(paste(queue,"live",sep="."))
}

setChunkSize <- function(value=1)
{
  if(!is.numeric(value)) stop("setChunkSize requires a numeric argument")
  value <- max(round(value - 1),0)
  assign('chunkSize', value, envir=.doRedisGlobals)
}

# The number of workers should be considered an estimate that may change.
.info <- function(data, item) {
  switch(item,
         workers=
           tryCatch(
             {
               n <- redisGet(
                       paste(foreach:::.foreachGlobals$data,'count',sep='.'))
               if(length(n)==0) n <- 0
               else n <- as.numeric(n)
             }, error=function(e) 0),
         name='doRedis',
         version=packageDescription('doRedis', fields='Version'),
         NULL)
}

.doRedisGlobals <- new.env(parent=emptyenv())

.makeDotsEnv <- function(...) {
  list(...)
  function() NULL
}

.doRedis <- function(obj, expr, envir, data)
{
  queue <- data
  queueEnv <- paste(queue,"env",sep=".")
  queueOut <- paste(queue,"out",sep=".")

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
  export <- unique(obj$export)
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
# Create a job environment for the workers to use
  redisSet(queueEnv, list(expr=expr, 
                         exportenv=exportenv, packages=obj$packages))
# ID associates this work with a job environment in queueEnv. If
# the workers current job environment does not match job ID, they retrieve
# the new job environment data from queueEnv and run workerInit.
  ID <- tempfile("doRedis")
  zz <- file(ID,"w")
  close(zz)
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

# Queue the job(s)
# We encode the job order in names(argsList) XXX This is perhaps not optimal
# since the accumulator requires numeric job tags for ordering.
  nout <- 1
  j <- 1
  while(j <= njobs)
   {
    k <- min(j+chunkSize,njobs)
    block <- argsList[j:k]
    names(block) <- j:k
    redisRPush(queue, list(ID=ID, argsList=block))
    j <- k + 1
    nout <- nout + 1
   }

# Collect the results and pass through the accumulator
  j <- 1
  while(j < nout)
   {
    results <- redisBRPop(queueOut)
    j <- j + 1
    tryCatch(accumulator(results[[1]], as.numeric(names(results[[1]]))), 
      error=function(e) {
        cat('error calling combine function:\n')
        print(e)
    })
   }

# Clean up the session ID
  unlink(ID)
 
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

# Convert the iterator to a list
.to.list <- function(x) {
  n <- 64
  a <- vector('list', length=n)
  i <- 0
  tryCatch({
    repeat {
      if (i >= n) {
        n <- 2 * n
        length(a) <- n
      }
      a[i + 1] <- list(nextElem(x))
      i <- i + 1
    }
  },
  error=function(e) {
    if (!identical(conditionMessage(e), 'StopIteration'))
      stop(e)
  })
  length(a) <- i
  a
}
