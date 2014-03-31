# Default task pulling function. Pull a task from job id job_id on job queue
# queue. All _getTask functions must follow the following convention:
#
# Function arguments must include at least:
# queue (characer) name of job queue
# job_id (character) job ID
# ...  (required ellipsis argument)
#
# The function must return either NULL or a task list, an R list with the
# following fields:
# task_id (character) task ID
# args (list) expression arguments

default_getTask <-  function(queue, job_id, ...)
{
  key <- sprintf("%s:%s",queue, job_id)
  if(version_check())
  {
# Run a Redis server-side Lua script that extracts the first available
# task. The script also, importantly, sets the task start key.
    return(redisEval("local x=redis.call('hkeys',KEYS[1])[1];if x==nil then return nil end;local ans=redis.call('hget',KEYS[1],x);redis.call('set', KEYS[1] .. '.start.' .. x, x);redis.call('hdel',KEYS[1],x);return ans",key))
  }
# We've got an old version of Redis without Lua. Manually pull a task from the
# task hash. It's really a hack just to support old versions of Redis. Use the
# new approach if possible. There is a failure opportunity here...this worker
# might fail *before* it sets the task start key, in which case the task is
# lost and the master will wait forever for it. Again, use a Lua-equipped
# version of Redis if you can!
  tasks <- redisHKeys(key)
  if(is.null(tasks)) return()
  while(length(tasks)>0)
  {
    redisMulti()
    redisHGet(key, tasks[[1]])
    redisHDel(key, tasks[[1]])
    ans <- redisExec()[[1]]
    if(!is.null(ans))
    {
# Set up F/T start key
      fttag.start <- sprintf("%s:%.0f.start.%s",queue,job_id,ans$task_id)
      redisSet(fttag.start, ans$task_id)
      return(ans)
    }
# Damn, somebody else grabbed this task before us.
    tasks <- tasks[-1]
  }
  NULL
}
