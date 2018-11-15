#!/bin/bash
#
# This is an experimental script that sets up R doRedis workers to start
# automatically on LSB systems. It installs three files:
# /etc/init.d/doRedis
# /usr/local/bin/doRedis
# /etc/doRedis.conf
#
# and configures the doRedis init script to start in the usual runlevels.
# Edit the /etc/doRedis.conf file to point to the right Redis server and
# to set other configuration parameters.
#
# Usage:
# sudo ./redis-worker-installer.sh
#
# On AWS EC2 systems, use:
# sudo ./redis-worker-installer.sh EC2

echo "Installing /etc/init.d/doRedis script..."
cat > /etc/init.d/doRedis <<1ZZZ
#! /bin/bash
### BEGIN INIT INFO
# Provides:          doRedis
# Required-Start:    \$remote_fs \$syslog
# Required-Stop:     \$remote_fs \$syslog
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: doRedis
# Description:       Redis-coordinated concurrent R processes.
### END INIT INFO

# Author: B. W. Lewis <blewis@illposed.net>

# PATH should only include /usr/* if it runs after the mountnfs.sh script
PATH=/sbin:/usr/sbin:/bin:/usr/bin:/usr/local/bin
DESC="Redis-coordinated concurrent R processes."
NAME=doRedis
DAEMON=/usr/local/bin/doRedis
DAEMON_ARGS=/etc/doRedis.conf
PIDFILE=/var/run/doRedis.pid
SCRIPTNAME=/etc/init.d/doRedis
TEMPDIR=/tmp/doRedis
EC2=$1

# Function that starts the daemon/service, optionally initializing
# configuration file from EC2 user data. We skip initialization
# of the config file if the EC2 user data string starts with '#!'.
#
do_start()
{
  if test -n "\${EC2}"; then
    U=\$(wget -O - -q http://169.254.169.254/latest/user-data)
    if test -n "\${U}";  then
      if test -z \$(echo -e "\${U}" | sed -n 1p | grep '#!'); then
        echo -e "\${U}" > /etc/doRedis.conf
      fi
    fi
  fi
  USER=\$(cat \${DAEMON_ARGS} | sed -n /^[[:blank:]]*user:/p | tail -n 1 | sed -e "s/#.*//" | sed -e "s/.*://" | sed -e "s/^ *//" | sed -e "s/[[:blank:]]*$//")
  [ -z "\${USER}" ]   && USER=nobody
# Global parameters
  NUM=\$(cat \${DAEMON_ARGS} | sed -n /^[[:blank:]]*n:/p | tail -n 1 | sed -e "s/#.*//" | sed -e "s/.*://" | sed -e "s/^ *//" | sed -e "s/[[:blank:]]*$//" | tr -s ' ' | tr ' ' '\n' | wc -l)
  R=\$(cat \${DAEMON_ARGS} | sed -n /^[[:blank:]]*R:/p | tail -n 1 | sed -e "s/#.*//" | sed -e "s/.*://" | sed -e "s/^ *//" | sed -e "s/[[:blank:]]*$//")
  LINGER=\$(cat \${DAEMON_ARGS} | sed -n /^[[:blank:]]*linger:/p | tail -n 1 | sed -e "s/#.*//" | sed -e "s/.*://" | sed -e "s/^ *//" | sed -e "s/[[:blank:]]*$//")
  I=\$(cat \${DAEMON_ARGS} | sed -n /^[[:blank:]]*iter:/p | tail -n 1 | sed -e "s/#.*//" | sed -e "s/.*://" | sed -e "s/^ *//" | sed -e "s/[[:blank:]]*$//")
  HOST=\$(cat \${DAEMON_ARGS} | sed -n /^[[:blank:]]*host:/p | tail -n 1 | sed -e "s/#.*//" | sed -e "s/.*://" | sed -e "s/^ *//" | sed -e "s/[[:blank:]]*$//")
  PORT=\$(cat \${DAEMON_ARGS} | sed -n /^[[:blank:]]*port:/p | tail -n 1 | sed -e "s/#.*//" | sed -e "s/.*://" | sed -e "s/^ *//" | sed -e "s/[[:blank:]]*$//")
  LOGLEVEL=\$(cat \${DAEMON_ARGS} | sed -n /^[[:blank:]]*loglevel:/p | tail -n 1 | sed -e "s/#.*//" | sed -e "s/.*://" | sed -e "s/^ *//" | sed -e "s/[[:blank:]]*$//")
  TIMELIMIT=\$(cat \${DAEMON_ARGS} | sed -n /^[[:blank:]]*timelimit:/p | tail -n 1 | sed -e "s/#.*//" | sed -e "s/.*://" | sed -e "s/^ *//" | sed -e "s/[[:blank:]]*$//")
# default values
  [ -z "\${NUM}" ]    && NUM=1
  [ \${NUM} -eq 0 ]   && NUM=1
  [ -z "\${R}" ]      && R=R
  [ -z "\${LINGER}" ] && LINGER=5
  [ -z "\${I}" ]      && I=50
  [ -z "\${HOST}" ]   && HOST=localhost
  [ -z "\${PORT}" ]   && PORT=6379
  [ -z "\${LOGLEVEL}" ] && LOGLEVEL=0
  [ -z "\${TIMELIMIT}" ] && TIMELIMIT=0

  mkdir -p \${TEMPDIR}
  chown \${USER} \${TEMPDIR}

  J=1
  while test \${J} -le \${NUM}; do
# queue-specific parameters
    N=\$(cat \$DAEMON_ARGS | sed -n /^[[:blank:]]*n:/p | tail -n 1 | sed -e "s/#.*//" | sed -e "s/.*://" | sed -e "s/^ *//" | sed -e "s/[[:blank:]]*$//" | tr -s ' ' |  cut -d ' ' -f \${J})
    QUEUE=\$(cat \$DAEMON_ARGS | sed -n /^[[:blank:]]*queue:/p | tail -n 1 | sed -e "s/#.*//" | sed -e "s/.*://" | sed -e "s/^ *//" | sed -e "s/[[:blank:]]*$//" | tr -s ' ' | cut -d ' ' -f \${J})
    [ -z "\${N}" ]     && N=2
    [ -z "\${QUEUE}" ] && QUEUE=RJOBS
    K=1
    while test \${K} -le \${N}; do
      TMPDIR=\${TEMPDIR} sudo -b -n -E -u \${USER} /usr/local/bin/doRedis \${R} --slave -e "suppressPackageStartupMessages({require('doRedis'); tryCatch(redisWorker(queue='\${QUEUE}', host='\${HOST}', port=\${PORT}, linger=\${LINGER}, timelimit=\${TIMELIMIT}, iter=\${I}, loglevel=\${LOGLEVEL}), error=function(e) q(save='no'))});q(save='no')" 0<&- 2> >(logger -s -i -t doRedis)
      K=\$(( \${K} + 1 ))
    done
    J=\$(( \${J} + 1 ))
  done
}

#
# Function that stops the daemon/service
#
do_stop()
{
  rm -rf \${TEMPDIR}
  killall doRedis
}

case "\$1" in
  start)
	do_start && echo "Started doRedis R worker service" || echo "Failed to start doRedis R worker service"
	;;
  stop)
	do_stop && echo "Stoped doRedis R worker service"
	;;
  status)
       [[ \$(ps -aux | grep doRedis | grep slave | wc -l | cut -d ' ' -f 1) -gt 0 ]] && echo "Running" || exit 1
       ;;
  *)
	echo "Usage: doRedis {start|stop|status}" >&2
	exit 3
	;;
esac
1ZZZ

echo "Compiling and installing /usr/local/bin/doRedis program..."
cat > /tmp/doRedis.c << 2ZZZ
#include <stdio.h>
#include <sys/types.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>
#include <stdlib.h>
pid_t pid;
static void catch(int sig) {kill(pid, sig);}
int
main (int argc, char ** argv)
{
  char * cargv[128];
  int status, j;
  int restart = 1;
  if(argc > 127) exit(-1);
  for(j = 0; j < 128; ++j)
    if(j < argc) cargv[j] = argv[j + 1]; else cargv[j] = (char *) NULL;
  signal(SIGTERM, catch);
  while (restart)
    { 
      pid = fork();
      switch (pid)
        { 
          case -1: exit(-1);
          case 0: execvp(cargv[0], cargv);
          default: break;
        }
      wait(&status);
      if(WIFSIGNALED(status))
      { 
        status = WTERMSIG(status);
        if(status == 15 || status == 9 || status == 3) restart = 0;
        else fprintf(stderr, "restarting doRedis R worker after exit status %d\n", status);
      }
    }
  fprintf(stderr, "normal doRedis R worker shutdown\n");
  return 0;
}
2ZZZ
cc -o /usr/local/bin/doRedis /tmp/doRedis.c && rm -f /tmp/doRedis.c || echo "Failed to install doRedis helper program" 1>&2

echo "Installing /etc/doRedis.conf configuration file                          (you probably want to edit this)..."
cat > /etc/doRedis.conf << 3ZZZ
# /etc/doRedis.conf
# This file has a pretty rigid structure. The format per line is
#
# key: vaule
#
# and the colon after the key is required! The settings
# may apper in any order. Everything after a '#' character is
# ignored per line. Default values appear below.
#
n: 2              # number of workers to start
queue: RJOBS      # queue foreach job queue name
R: R              # path to R (default assumes 'R' is in the PATH)
linger: 5         # wait in seconds after job queue is deleted before exiting
iter: 20          # maximum tasks to run before worker exit and restart
host: localhost   # host redis host
port: 6379        # port redis port
user: nobody      # user that runs the service and R workers
loglevel: 0       # set to 1 to log tasks as they run in the system log
timelimit: 0      # per task maximum time limit in seconds, after which worker is killed
#
# The n: and queue: entries may list more than one set of worker numbers and
# queue names delimited by exactly one space. If more than one queue is
# specified, then the n: and queue: entries *must* be of the same length.
# For example,
#
# n: 2 1
# queue: RJOBS SYSTEM
#
# starts a set of two R workers listening on the queue named 'RJOBS' and
# separately, a single R worker listening on the queue named 'SYSTEM'.
3ZZZ

chmod a+x /etc/init.d/doRedis
if test -n "`which update-rc.d 2>/dev/null`"; then
  update-rc.d doRedis defaults
else
  chkconfig --level 35 doRedis on
fi
/etc/init.d/doRedis start
