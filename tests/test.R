require(doRedis)

if(Sys.getenv("TEST_DOREDIS")!="TRUE")
{
  queue = "jobs"
  startLocalWorkers(n=2, queue, timeout=5)
  registerDoRedis(queue)
  ans = foreach(j=1:10,.combine=sum) %dopar% j
  removeQueue(queue)
  if(!isTRUE(ans==55)) stop("incorrect result")
}
