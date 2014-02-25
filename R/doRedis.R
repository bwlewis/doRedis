#        __      ____           ___     
#   ____/ /___  / __ \___  ____/ (_)____
#  / __  / __ \/ /_/ / _ \/ __  / / ___/
# / /_/ / /_/ / _, _/  __/ /_/ / (__  ) 
# \__,_/\____/_/ |_|\___/\__,_/_/____/  
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

# The environment initialization code is adapted (with minor changes) from the
# doMPI package by Steve Weston.

# Register the 'doRedis' function with %dopar%.
registerDoRedis <- function(queue, host="localhost", port=6379, password=NULL)
{
  redisConnect(host,port,password=password)
  assign('.queue', queue, envir=.doRedisGlobals)
  setDoPar(fun=.doRedis,
    data=list(queue=queue), 
    info=.info)
}

removeQueue <- function(queue)
{
  tryCatch(redisDelete(queue),error=invisible,warning=invisible)
  k <- redisKeys(sprintf("%s:*",queue))
  for(j in k) tryCatch(redisDelete(j),error=invisible,warning=invisible)
}

removeJob <- function(queue, ID)
{
  k <- redisKeys(sprintf("%s:%.0f*",queue,ID))
  for(j in k) tryCatch(redisDelete(j),error=invisible,warning=invisible)
}

setChunkSize <- function(value=1)
{
  if(!is.numeric(value)) stop("setChunkSize requires a numeric argument")
  value <- max(round(value - 1),0)
  assign('chunkSize', value, envir=.doRedisGlobals)
}

setTaskLabel <- function(fn=I)
{
  assign('taskLabel', fn, envir=.doRedisGlobals)
}

setGetTask <- function(fn=default_getTask)
{
  assign('getTask', fn, envir=.doRedisGlobals)
}

setExport <- function(names=c())
{
  assign('export', names, envir=.doRedisGlobals)
}

setPackages <- function(packages=c())
{
  assign('packages', packages, envir=.doRedisGlobals)
}

.info <- function(data, item)
{
  switch(item,
           workers=
             tryCatch(
               {
                 n <- redisGet(
                         paste(.doRedisGlobals$.queue,'workers',sep=':'))
                 if(length(n)==0) n <- 0
                 else n <- as.numeric(n)
               }, error=function(e) 0),
           name='doRedis',
           version=packageDescription('doRedis', fields='Version'),
           NULL)
}

.doRedisGlobals <- new.env(parent=emptyenv())

# This is used for the closure's enclosing environment.
.makeDotsEnv <- function(...)
{
  list(...)
  function() NULL
}

.doRedis <- function(obj, expr, envir, data)
{
# ID associates the work with a job environment <queue>:<ID>.env. If
# the workers current job environment does not match job ID, they retrieve
# the new job environment data from queueEnv and run workerInit.
  queue <- data$queue
  queueCounter <- sprintf("%s:counter", queue)   # job task ID counter
  ID <- redisIncr(queueCounter)
  queueEnv <- sprintf("%s:%.0f.env",queue,ID) # R job environment
  queueTasks <- sprintf("%s:%.0f",queue,ID) # Job tasks hash
  queueResults <- sprintf("%s:%.0f.results",queue,ID) # Output values
  queueStart <- sprintf("%s:%.0f.start*",queue,ID)
  queueAlive <- sprintf("%s:%.0f.alive*",queue,ID)

  if (!inherits(obj, 'foreach'))
    stop('obj must be a foreach object')

  it <- iter(obj)
  argsList <- .to.list(it)
  accumulator <- makeAccum(it)

# Setup the job parent environment
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
# Add task pulling function to exportenv .getTask:
  getTask <- default_getTask
  if(exists('getTask',envir=.doRedisGlobals))
    getTask <- get('getTask',envir=.doRedisGlobals)
  if(!is.null(obj$options$redis$getTask))
    getTask <- obj$options$redis$getTask
  assign(".getTask",getTask, envir=exportenv)

# Define task labeling function taskLabel:
  taskLabel <- I
  if(exists('taskLabel',envir=.doRedisGlobals))
    taskLabel <- get('taskLabel',envir=.doRedisGlobals)
  if(!is.null(obj$options$redis$taskLabel))
    taskLabel <- obj$options$redis$taskLabel

# Create a job environment in Redis for the workers to use
  redisSet(queueEnv, list(expr=expr, 
                          exportenv=exportenv, packages=obj$packages))

  results <- NULL
  ntasks <- length(argsList)
# foreach lets one pass options to a backend with the .options.<label>
# argument. We check for a user-supplied chunkSize option.
# Example: foreach(j=1,.options.redis=list(chunkSize=100)) %dopar% ...
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

# Queue the tasks (in blocks defined by chunkSize)
# 1. Add each task block to the <queue>:<task id> hash
# 2. Add a job ID notice to the job queue for each task block
#
# We encode the job order in names(argsList) XXX This is perhaps not optimal
# since the accumulator requires numeric job tags for ordering.
# We also maintain a list of dispatched tasks in task_list for fault recovery.
  task_list <- list()
  nout <- 1
  j <- 1
# To speed this up, we use nonblocking calls to Redis.
  redisSetPipeline(TRUE)
  redisMulti()
  while(j <= ntasks)
   {
    k <- min(j+chunkSize,ntasks)
    taskblock <- argsList[j:k]
    names(taskblock) <- j:k
# Note, we're free to identify the task in any unique way.  For example, we
# could add a data location hint.
    task_id = as.character(taskLabel(j))
    task <- list(task_id=task_id, args=taskblock)
    task_list[[task_id]] <- task
    redisHSet(queueTasks, task_id, task)
    redisRPush(queue, ID)
    j <- k + 1
    nout <- nout + 1
   }
   redisExec()
   redisGetResponse(all=TRUE)
   redisSetPipeline(FALSE)

# Collect the results and pass through the accumulator
  j <- 1
  while(j < nout)
   {
    results <- tryCatch(redisBRPop(queueResults, timeout=ftinterval),error=NULL)
    if(is.null(results)) {
# Check for worker fault and re-submit tasks if required...
      started <- redisKeys(queueStart)
      started <- gsub(sprintf("%s:%.0f.start.",queue,ID),"",started)
      alive <- redisKeys(queueAlive)
      alive <- gsub(sprintf("%s:%.0f.alive.",queue,ID),"",alive)
      fault <- setdiff(started,alive)
      if(length(fault)>0) {
# One or more worker faults have occurred. Re-sumbit the work.
        for(k in fault)
        {
          warning(sprintf("Worker fault, resubmitting task %s.",k))
          qs <- sprintf("%s:%.0f.start.%s",queue,ID,k)
          redisDelete(qs)
          redisHSet(queueTasks, k, task_list[[k]])
          redisRPush(queue, ID)
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
  removeJob(queue, ID)
 
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

uuid <- function(uuidLength=10) {
  paste(sample(c(letters[1:6],0:9), uuidLength, replace=TRUE),collapse="")
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

.onLoad <- function(libname, pkgname)
{
  library.dynam('doRedis', pkgname, libname)
}

.onUnload <- function (libpath)
{
  library.dynam.unload('doRedis', libpath)
}
