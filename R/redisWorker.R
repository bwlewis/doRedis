.doRedisGlobals <- new.env(parent=emptyenv())

`.workerInit` <- function(expr, exportenv, packages)
{
  assign('expr', expr, .doRedisGlobals)
  assign('exportenv', exportenv, .doRedisGlobals)
  parent.env(.doRedisGlobals$exportenv) <- globalenv()
  tryCatch(
    {for (p in packages)
      library(p, character.only=TRUE)
    }, error=function(e) conditionMessage(e)
  )
}

`.evalWrapper` <- function(args)
{
  tryCatch({
      lapply(names(args), function(n) 
                         assign(n, args[[n]], pos=.doRedisGlobals$exportenv))
      eval(.doRedisGlobals$expr, envir=.doRedisGlobals$exportenv)
    },
    error=function(e) e
  )
}

`.redisVersionCheck` <- function()
{
  vcheck <- TRUE
  tryCatch(
   {
    rv <- redisInfo()$redis_version
    rv <- strsplit(rv,'\\.')[[1]]
    vcheck <- vcheck && rv[[1]] >= 1
    vcheck <- vcheck && rv[[2]] >= 2
   }, error = function(e) vcheck <<- FALSE)
  if(!vcheck) stop("doRedis requires Redis >= 1.3.0")
}

`startLocalWorkers` <- function(n, queue, host="localhost", port=6379, iter=Inf, timeout=60, log=stderr(), Rbin=paste(R.home(component='bin'),"/R --slave",sep=""))
{
  m <- match.call()
  f <- formals()
  l <- m$log
  if(is.null(l)) l <- f$log 
  cmd <- paste("require(doRedis);redisWorker(queue='",queue,"', host='",host,"', port=",port,", iter=",iter,", timeout=",timeout,", log=",deparse(l),")",sep="")
  for(j in 1:n) 
    system(Rbin,input=cmd,intern=FALSE,wait=FALSE,ignore.stderr=TRUE)
}

`redisWorker` <- function(queue, host="localhost", port=6379, iter=Inf, timeout=60, log=stdout())
{
  redisConnect(host,port)
  .redisVersionCheck()
  assign(".jobID", "0", envir=.doRedisGlobals)
  queueLive <- paste(queue,"live",sep=".")
  for(j in queueLive)
   {
    if(!redisExists(j)) redisSet(j,NULL)
   }
  queueCount <- paste(queue,"count",sep=".")
  for(j in queueCount)
    tryCatch(redisIncr(j),error=function(e) invisible())
  queueEnv <- paste(queue,"env",sep=".")
  queueOut <- paste(queue,"out",sep=".")
  cat("Waiting for doRedis jobs.\n", file=log)
  flush.console()
  k <- 0
  while(k < iter) {
    work <- redisBRPop(queue,timeout=timeout)
# We terminate the worker loop after a timeout when all specified work 
# queues have been deleted.
    if(is.null(work[[1]]))
     {
      ok <- FALSE
      for(j in queueLive) ok <- ok || redisExists(j)
      if(!ok) {
# If we get here, our queues were deleted. Clean up and exit worker loop.
        for(j in queueOut) if(redisExists(j)) redisDelete(j)
        for(j in queueEnv) if(redisExists(j)) redisDelete(j)
        for(j in queueCount) if(redisExists(j)) redisDelete(j)
        for(j in queue) if(redisExists(j)) redisDelete(j)
        break
      }
     }
    else
     {
      k <- k + 1
      cat("Processing task",names(work[[1]]$argsList),"from queue",names(work),"ID",work[[1]]$ID,"\n",file=log)
  flush.console()
# Check that the incoming work ID matches our current environment. If
# not, we need to re-initialize our work environment with data from the
# <queue>.env Redis string.
      if(get(".jobID", envir=.doRedisGlobals) != work[[1]]$ID)
       {
        initdata <- redisGet(queueEnv)
        .workerInit(initdata$expr, initdata$exportenv, initdata$packages)
        assign(".redisWorkerEnvironmentID", work$ID, envir=.doRedisGlobals)
       }
# Now do the work:
# XXX We assume that job order is encoded in names(argsList), cf. doRedis.
      result <- lapply(work[[1]]$argsList, .evalWrapper)
      names(result) <- names(work[[1]]$argsList)
      redisLPush(queueOut, result)
    }
  }
# Either the queue has been deleted, or we've exceeded the number of
# specified work iterations.
  for(j in queueCount) if(redisExists(j)) redisDecr(j)
  cat("Worker exit.\n", file=log)
  redisClose()
}
