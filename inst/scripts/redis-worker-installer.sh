#!/bin/bash
#
# This is an experimental script that sets up R doRedis workers to start
# automatically on LSB systems. It installs three files:
# /etc/init.d/doRedis
# /usr/local/bin/doRedis_worker
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
#! /bin/sh
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
DAEMON=/usr/local/bin/doRedis_worker
DAEMON_ARGS=/etc/doRedis.conf
PIDFILE=/var/run/doRedis.pid
SCRIPTNAME=/etc/init.d/doRedis
EC2=$1

# Define LSB log_* functions.
# Depend on lsb-base (>= 3.0-6) to ensure that this file is present.
. /lib/lsb/init-functions

#
# Function that starts the daemon/service, optionally initializing
# configuration file from EC2 user data.
#
do_start()
{
  if test -n "\${EC2}"; then
    U=\$(wget -O - -q http://169.254.169.254/latest/user-data)
    if test -n "\${U}";  then
      echo \${U} > /etc/doRedis.conf
    fi
  fi
  USER=\$(cat /etc/doRedis.conf | sed -n /^[[:blank:]]*user:/p | tail -n 1 | sed -e "s/#.*//" | sed -e "s/.*://" | sed -e "s/^ *//" | sed -e "s/[[:blank:]]*$//")
  [ -z "\${USER}" ]   && USER=nobody
  NUM=\$(cat /etc/doRedis.conf | sed -n /^[[:blank:]]*n:/p | tail -n 1 | sed -e "s/#.*//" | sed -e "s/.*://" | sed -e "s/^ *//" | sed -e "s/[[:blank:]]*$//" | tr ' ' '\n' | wc -l)
  [ -z "\${NUM}" ]   && NUM=1
  [ \${NUM} -eq 0 ]   && NUM=1
  J=1
  while test \${J} -le \${NUM}; do
    J=\$(( \${J} + 1 ))
    sudo -b -n -E -u \${USER} /usr/local/bin/doRedis_worker /etc/doRedis.conf \${J} start >/dev/null 2>&1 &
  done;
}

#
# Function that stops the daemon/service
#
do_stop()
{
  killall doRedis_worker
}


case "\$1" in
  start)
	do_start && log_success_msg "Started Redis R worker service" || log_failure_msg "Failed to start Redis R worker service"
	;;
  stop)
	do_stop && log_success_msg "Stoped Redis R worker service"
	;;
  status)
       [[ \$(ps -aux | grep doRedis_worker | grep doRedis.conf | wc -l | cut -d ' ' -f 1) -gt 0 ]] && exit 0 || exit 1
       ;;
  *)
	echo "Usage: doRedis {start|stop|status}" >&2
	exit 3
	;;
esac

1ZZZ

echo "Installing /usr/local/bin/doRedis_worker helper script..."
cat > /usr/local/bin/doRedis_worker << 2ZZZ
#!/bin/bash
# doRedis R worker startup script
export PATH="${PATH}:/usr/bin:/usr/local/bin"

CONF=\$1
NUM=\$2

if test \$# -eq 3; then  # daemonize
  nohup "\${0}" "\${CONF}" "\${NUM}" 0<&- &>/dev/null &
  disown
  exit 0
fi

[ ! -x \$CONF ]  || echo "Can't find configuration file doRedis.conf, exiting"
[ ! -x \$CONF ] || exit 1

[ -z "\${NUM}" ]   && NUM=1
[ \${NUM} -eq 0 ]  && NUM=1

N=\$(cat \$CONF | sed -n /^[[:blank:]]*n:/p | tail -n 1 | sed -e "s/#.*//" | sed -e "s/.*://" | sed -e "s/^ *//" | sed -e "s/[[:blank:]]*$//" | cut -d ' ' -f \${NUM})
QUEUE=\$(cat \$CONF | sed -n /^[[:blank:]]*queue:/p | tail -n 1 | sed -e "s/#.*//" | sed -e "s/.*://" | sed -e "s/^ *//" | sed -e "s/[[:blank:]]*$//" | cut -d ' ' -f \${NUM})
R=\$(cat \$CONF | sed -n /^[[:blank:]]*R:/p | tail -n 1 | sed -e "s/#.*//" | sed -e "s/.*://" | sed -e "s/^ *//" | sed -e "s/[[:blank:]]*$//")
T=\$(cat \$CONF | sed -n /^[[:blank:]]*timeout:/p | tail -n 1 | sed -e "s/#.*//" | sed -e "s/.*://" | sed -e "s/^ *//" | sed -e "s/[[:blank:]]*$//")
I=\$(cat \$CONF | sed -n /^[[:blank:]]*iter:/p | tail -n 1 | sed -e "s/#.*//" | sed -e "s/.*://" | sed -e "s/^ *//" | sed -e "s/[[:blank:]]*$//")
HOST=\$(cat \$CONF | sed -n /^[[:blank:]]*host:/p | tail -n 1 | sed -e "s/#.*//" | sed -e "s/.*://" | sed -e "s/^ *//" | sed -e "s/[[:blank:]]*$//")
PORT=\$(cat \$CONF | sed -n /^[[:blank:]]*port:/p | tail -n 1 | sed -e "s/#.*//" | sed -e "s/.*://" | sed -e "s/^ *//" | sed -e "s/[[:blank:]]*$//")
LOG=\$(cat \$CONF | sed -n /^[[:blank:]]*log:/p | tail -n 1 | sed -e "s/#.*//" | sed -e "s/.*://" | sed -e "s/^ *//" | sed -e "s/[[:blank:]]*$//")
ROTATE=\$(cat \$CONF | sed -n /^[[:blank:]]*rotate:/p | tail -n 1 | sed -e "s/#.*//" | sed -e "s/.*://" | sed -e "s/^ *//" | sed -e "s/[[:blank:]]*$//")

# Set default values
[ -z "\${N}" ]     && N=2
[ -z "\${R}" ]     && R=R
[ -z "\${T}" ]     && T=5
[ -z "\${I}" ]     && I=50
[ -z "\${HOST}" ]  && HOST=localhost
[ -z "\${PORT}" ]  && PORT=6379
[ -z "\${QUEUE}" ] && QUEUE=RJOBS
[ -z "\${LOG}" ]   && LOG=/dev/null
[ -z "\${ROTATE}" ]   && ROTATE=3600

Terminator ()
{
  for j in \$(jobs -p -r); do
    kill \$j 2>/dev/null
  done
  exit
}
trap "Terminator" SIGHUP SIGINT SIGTERM

timeout=1
count=1
while :; do
  # Initial start up
  count=\$((\$count + 1))
  if test \$count -gt 1800; then  # rotate log
    count=1
    L=\$(wc -l \${LOG} | cut -d ' ' -f 1)
    if test \$L -gt 10000; then
      sed  -e :a  -e '\$q;N;10001,\$D;ba' -i.old \${LOG}
    fi
  fi
  j=\$(jobs -p -r| wc -l)
  if test \$j -lt \$N; then       # start worker
    \${R} --slave -e "require('doRedis'); tryCatch(redisWorker(queue='\${QUEUE}', host='\${HOST}', port=\${PORT},timeout=\${T},iter=\${I}), error=function(e) q(save='no'));q(save='no')"  >>\${LOG} 2>&1  &
  fi
#  read -n1 -s -t\$timeout # XXX doesn't really work!
  sleep \${timeout}
done
2ZZZ

chmod +x /usr/local/bin/doRedis_worker

echo "Installing /etc/doRedis.conf configuration file                                   (you probably want to edit this)..."
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
timeout: 5        # wait in seconds after job queue is deleted before exiting
iter: 50          # maximum tasks to run before worker exit and restart
host: localhost   # host redis host
port: 6379        # port redis port
user: nobody      # user that runs the service and R workers
log: /dev/null    # direct stderr of each worker to this file (web scale)
rotate: 3600      # interval in seconds for log rotation
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
update-rc.d doRedis defaults || chkconfig --level 35 doRedis on
/etc/init.d/doRedis start
