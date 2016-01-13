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
  sudo -b -n -E -u nobody /usr/local/bin/doRedis_worker /etc/doRedis.conf start >/dev/null 2>&1 &
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
       [[ \$(ps -aux | grep doRedis_worker | grep doRedis.conf | wc -l) -gt 0 ]] && exit 0 || exit 1
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

if test \$# -eq 2; then
  nohup "\${0}" "\${CONF}" 0<&- &>/dev/null &
  disown
  exit 0
fi

[ ! -x \$CONF ]  || echo "Can't find configuration file doRedis.conf, exiting"
[ ! -x \$CONF ] || exit 1

N=\$(cat \$CONF | sed -n /^[[:blank:]]*n:/p | tail -n 1 | sed -e "s/.*:[[:blank:]*]//")
R=\$(cat \$CONF | sed -n /^[[:blank:]]*R:/p  | tail -n 1 | sed -e "s/.*:[[:blank:]*]//")
T=\$(cat \$CONF | sed -n /^[[:blank:]]*timeout:/p  | tail -n 1 | sed -e "s/.*:[[:blank:]*]//")
I=\$(cat \$CONF | sed -n /^[[:blank:]]*iter:/p | tail -n 1 | sed -e "s/.*:[[:blank:]*]//")
HOST=\$(cat \$CONF | sed -n /^[[:blank:]]*host:/p | tail -n 1 | sed -e "s/.*:[[:blank:]*]//")
PORT=\$(cat \$CONF | sed -n /^[[:blank:]]*port:/p | tail -n 1 | sed -e "s/.*:[[:blank:]*]//")
QUEUE=\$(cat \$CONF | sed -n /^[[:blank:]]*queue:/p | tail -n 1 | sed -e "s/.*:[[:blank:]*]//")
LOG=\$(cat \$CONF | sed -n /^[[:blank:]]*log:/p | tail -n 1 | sed -e "s/.*:[[:blank:]*]//")

# Set default values
[ -z "\${N}" ]     && N=2
[ -z "\${R}" ]     && R=R
[ -z "\${T}" ]     && T=5
[ -z "\${I}" ]     && I=Inf
[ -z "\${HOST}" ]  && HOST=localhost
[ -z "\${PORT}" ]  && PORT=6379
[ -z "\${QUEUE}" ] && QUEUE=RJOBS
[ -z "\${LOG}" ]   && LOG=/dev/null

Terminator ()
{
  for j in \$(jobs -p -r); do
    kill \$j 2>/dev/null
  done
  exit
}
trap "Terminator" SIGHUP SIGINT SIGTERM

timeout=1
while :; do
  # Initial start up
  j=\$(jobs -p -r| wc -l)
  if test \$j -lt \$N; then
    \${R} --slave -e "require('doRedis'); tryCatch(redisWorker(queue='\${QUEUE}', host='\${HOST}', port=\${PORT},timeout=\${T},iter=\${I}), error=function(e) q(save='no'));q(save='no')"  >>\${LOG} 2>&1  &
  fi
  sleep \$timeout
done
2ZZZ

chmod +x /usr/local/bin/doRedis_worker

echo "Installing /etc/doRedis.conf configuration file                                   (you probably want to edit this)..."
cat > /etc/doRedis.conf << 3ZZZ
# /etc/doRedis.conf
# This file has a pretty rigid structure. The format per line is
# key: vaule
# and the colon and space after key are required! The settings
# may apper in any order.
#
# Set n to the number of workers to start.
n: 2
# Set R to the path to R (default assumes 'R' is in the PATH)
R: R
# Set timeout to wait period after job queue is deleted before exiting
timeout: 5
# Set iter to maximum number of iterations to run before exiting
iter: 50
host: localhost
port: 6379
queue: RJOBS
3ZZZ

chmod a+x /etc/init.d/doRedis
update-rc.d doRedis defaults || chkconfig --level 35 doRedis on
/etc/init.d/doRedis start
