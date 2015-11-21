/*
 * Copyright (c) 2010 by Bryan W. Lewis.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as published
 * by the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307
 * USA
 */

/*
 * alive.c: a simple fault detection system for doRedis.  doRedis worker fault
 * tolerance works by assigning two keys to each in process task: a permanent
 * key created when the task starts, and an ephemeral key kept alive by this
 * thread while the task is running.  If the thread defined in this code stops,
 * for example if R crashes, then the ephemeral key will expire and the master
 * doRedis process can detect the resulting keypair imbalance and re-submit the
 * corresponding task.
 */
#ifdef Win32
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#else
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <errno.h>
#include <pthread.h>
#include <unistd.h>
#include <signal.h>
#endif

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include <R.h>
#define USE_RINTERNALS
#include <Rinternals.h>

#define BS 4096
#define BS_LARGE 16384

int go;
int n_sentinel = 0;
#ifdef Win32
HANDLE t;
SOCKET s;
#else
int s = 0;
pthread_t t;
#endif

typedef struct
{
  int port;
  char host[BS];
} Conn;

void
snooze (int milliseconds)
{
#ifdef Win32
  Sleep (milliseconds);
#else
  usleep (1000 * milliseconds);
#endif
}

/* NOTE!
 * The https://cran.r-project.org/doc/manuals/R-exts.html manual states
 *
 *     Compiled code should not call entry points which might terminate R ...
 *
 * However, that outcome is explicitly desired here. When the connection to
 * Redis is lost, the R worker process might get stuck in a very slow to
 * timeout blocking connection, for example. We desire to terminate the R
 * process sooner rather than later. That way, doRedis can re-schedule the
 * failed task and a management wrapper like thie one in the inst/scripts
 * directory can start a fresh worker process going.
 */
void
die ()
{
#ifdef Win32
  ExitProcess (-1);
#else
  kill (getpid(), 15);
  snooze (1000);
  kill (getpid(), 9);
#endif
}

/* tcpconnect
 * connect to the specified host and port, setting the
 * socket value s to a socket connected to the host/port.
 */
#ifdef Win32
void
tcpconnect (SOCKET * s, const char *host, int port)
{
  int j;
  char portstr[16];
  struct addrinfo *a = NULL, *ap = NULL;
  struct addrinfo hints;
  *s = INVALID_SOCKET;
  snprintf (portstr, 16, "%d", port);
  ZeroMemory (&hints, sizeof (hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;
  j = getaddrinfo (host, portstr, &hints, &a);
  if (j != 0)
    {
      *s = INVALID_SOCKET;
      return;
    }
  ap = a;
  *s = socket (ap->ai_family, ap->ai_socktype, ap->ai_protocol);
  if (*s < 0)
    return;
  j = connect (*s, ap->ai_addr, (int) ap->ai_addrlen);
  if (j != 0)
    {
      close (*s);
      *s = INVALID_SOCKET;
    }
}
#else
void
tcpconnect (int *s, char *host, int port)
{
  struct hostent *h;
  struct sockaddr_in sa;
  int j;

  h = gethostbyname (host);
  if (!h)
    {
      *s = -1;
    }
  else
    {
      *s = socket (AF_INET, SOCK_STREAM, 0);
      if (s < 0)
        return;
      memset ((void *) &sa, 0, sizeof (sa));
      sa.sin_family = AF_INET;
      sa.sin_port = htons (port);
      sa.sin_addr = *(struct in_addr *) h->h_addr;
      j = connect (*s, (struct sockaddr *) &sa, sizeof (sa));
      if (j < 0)
        {
          close (*s);
          *s = -1;
          return;
        }
    }
}
#endif

/* From Brian "Beej Jorgensen" Hall Beej's Guide to Network Programming
 * Keep sending until entire buffer is sent
 */
#ifdef Win32
int
sendall (SOCKET s, char *buf, size_t * len)
#else
int
sendall (int s, char *buf, size_t * len)
#endif
{
  size_t total = 0;             // how many bytes we've sent
  size_t bytesleft = *len;      // how many we have left to send
  int n;

  while (total < *len)
    {
      n = send (s, buf + total, bytesleft, 0);
      if (n == -1)
        break;
      total += n;
      bytesleft -= n;
    }

  *len = total;                 // return number actually sent here
  return n == -1 ? -1 : 0;      // return -1 on failure, 0 on success
}

#ifdef Win32
int
msg (SOCKET sock, char *cmd, char *response)
#else
int
msg (int sock, char *cmd, char *response)
#endif
{
  int j;
  size_t cmd_len = strlen (cmd);
  j = sendall (sock, cmd, &cmd_len);
  if (j < 0)
    return j;
  memset (response, 0, BS);
  j = (int) recv (sock, response, BS, 0);
  if (j < 0)
    return j;
  if (response[0] == '-')
    j = -1;
  return j;
}


void
thread_exit ()
{
#ifdef Win32
  ExitThread ((DWORD) (0));
#else
  pthread_exit (NULL);
#endif
}


#ifdef Win32
void *
ok (LPVOID x)
#else
void *
ok (void *x)
#endif
{
  char set[BS_LARGE];
  char expire[BS_LARGE];
  char buf[BS];                 /* asumption is that response is short */
  int pr_n = -1;
  int j, m;
  char *key = (char *) x;
  size_t k = strlen (key);
  if (k > BS_LARGE - 128)
    {
      thread_exit ();
    }
  memset (set, 0, BS_LARGE);
  memset (expire, 0, BS_LARGE);
  pr_n =
    snprintf (set, BS_LARGE, "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$2\r\nOK\r\n",
              (int) k, key);
  if (pr_n < 0 || pr_n >= BS_LARGE)
    {
      thread_exit ();
    }
  pr_n =
    snprintf (expire, BS_LARGE,
              "*3\r\n$6\r\nEXPIRE\r\n$%d\r\n%s\r\n$1\r\n5\r\n", (int) k, key);
  if (pr_n < 0 || pr_n >= BS_LARGE)
    {
      thread_exit ();
    }

/* Check for thread termination every 1/10 sec, but only update Redis
 * every 3s (expire alive key after 5s). If messages to redis fail,
 * exit this thread.
 */
  m = 30;
  while (go > 0)
    {
      m += 1;
      if (m > 30)
        {
          j = msg (s, set, buf);
          if (j < 0)
            {
              thread_exit ();
            }
          j = msg (s, expire, buf);
          if (j < 0)
            {
              thread_exit ();
            }
          m = 0;
        }
      snooze (100);
    }
  return NULL;
}

/* OK to call delOK repeatedly */
SEXP
delOK ()
{
  if (go == 0)
    return (R_NilValue);
  go = 0;
#ifdef Win32
  closesocket (s);
  WaitForSingleObject (t, INFINITE);
  WSACleanup ();
#else
  close (s);
  pthread_join (t, NULL);
#endif
  return (R_NilValue);
}

SEXP
setOK (SEXP PORT, SEXP HOST, SEXP KEY, SEXP AUTH)
{
#ifdef Win32
  WSADATA wsaData;
  DWORD dw_thread_id;
#endif
  char authorize[BS];
  char buf[BS];
  char *host = (char *) CHAR (STRING_ELT (HOST, 0));
  int port = *(INTEGER (PORT));
  const char *key = CHAR (STRING_ELT (KEY, 0));
  const char *auth = CHAR (STRING_ELT (AUTH, 0));
  int j, k = strlen (auth);
  if (go > 0)
    return (R_NilValue);
#ifdef Win32
  WSAStartup (MAKEWORD (2, 2), &wsaData);
#endif
  tcpconnect (&s, host, port);
  go = 1;
/* check for AUTH and authorize if needed */
  if (k > 0)
    {
      memset (authorize, 0, BS);
      snprintf (authorize, BS, "*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n", k, auth);
      j = msg (s, authorize, buf);
      if (j < 0)
        error ("Redis communication error during authentication");
    }
#ifdef Win32
  t = CreateThread (NULL, 0, (LPTHREAD_START_ROUTINE) ok, (LPVOID) key, 0,
                    &dw_thread_id);
#else
  pthread_create (&t, NULL, &ok, (void *) key);
#endif
  return (R_NilValue);
}

/* At most one of these threads are allowed to run at a time.
 * Input: pointer to a Conn struct
 * This function does not return. It pings the redis server specified
 * in the Conn struct every 10 seconds. If a connection cannot be established
 * the function terminates R. If the connection is lost once established,
 * the function attempts to regain it three times. If after those tries
 * a connection can't be established, the function terminates R.
 */
#ifdef Win32
void *
sentinel_thread (LPVOID x)
#else
void *
sentinel_thread (void *x)
#endif
{
#ifdef Win32
  SOCKET q;
#else
  int q = 0;
#endif
  int j, try = 0;
  Conn conn;
  memcpy (&conn, (Conn *) x, sizeof (conn));
  const char *buf = "time\r\n";
  size_t len = strlen (buf);
  tcpconnect (&q, conn.host, conn.port);
  if (q < 0)
    {
      Rprintf ("sentinel could not connect to Redis %d\n", q);
      die ();
    }
  for (;;)
    {
      j = send (q, buf, len, MSG_NOSIGNAL);
      if (j < 0)
        {
          try++;
          if (try < 3)
            {
              Rprintf ("Connection lost, retrying %d\n", try);
              close (q);
              snooze (2000);
              tcpconnect (&q, conn.host, conn.port);
              if (q >= 0)
                try = 0;
            }
          else
            {
              Rprintf ("Connection lost, exiting\n");
              die ();
            }
        }
      snooze (10000);
    }
}

/* Interface to the sentinel_thread function above. */
SEXP
sentinel (SEXP PORT, SEXP HOST)
{
#ifdef Win32
  HANDLE t;
  WSADATA wsaData;
  DWORD dw_thread_id;
#else
  pthread_t st;
#endif
  Conn conn;
  /* Only allow at most one sentinel thread */
  if (n_sentinel > 0)
    return (R_NilValue);
  conn.port = *(INTEGER (PORT));
  snprintf (conn.host, BS, "%s", (char *) CHAR (STRING_ELT (HOST, 0)));
#ifdef Win32
  st =
    CreateThread (NULL, 0, (LPTHREAD_START_ROUTINE) sentinel_thread,
                  (LPVOID) & conn, 0, &dw_thread_id);
  if (st == NULL)
    {
      Rprintf ("error creating sentinel thread\n");
      die ();
    }
  CloseHandle (st);
#else
  if (pthread_create (&st, NULL, &sentinel_thread, (void *) &conn) != 0)
    {
      Rprintf ("error creating sentinel thread\n");
      die ();
    }
  pthread_detach (st);
#endif
  n_sentinel++;
  return (R_NilValue);
}
