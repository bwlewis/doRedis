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

`redisWorker` <- function(queue, host="localhost", port=6379)
{
  redisConnect(host,port)
  assign(".jobID", "0", envir=.doRedisGlobals)
  queueEnv <- paste(queue,"env",sep=".")
  queueOut <- paste(queue,"out",sep=".")
  while(TRUE) {
    work <- redisBRPop(queue)
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
  redisClose()
}
