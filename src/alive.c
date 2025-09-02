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
#include <netinet/tcp.h> // TCP_NODELAY
#include <netinet/in.h>
#include <netdb.h>
#include <errno.h>
#include <pthread.h>
#include <unistd.h>
#include <signal.h>
#endif

#include <time.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include <R.h>
#define USE_RINTERNALS
#include <Rinternals.h>

#define BS 4096
#define BS_LARGE 16384

time_t start_time;
double time_limit;
int go;
#ifdef Win32
HANDLE t;
SOCKET s;
#else
int s = 0;
pthread_t t;
#endif

void
snooze (int milliseconds)
{
#ifdef Win32
  Sleep (milliseconds);
#else
  usleep (1000 * milliseconds);
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
      closesocket (*s);
      *s = INVALID_SOCKET;
    }
}
#else
void
tcpconnect (int *s, char *host, int port)
{
  struct hostent *h;
  socklen_t len;
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
      if (*s < 0)
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
  /* Try to disable Nagle; proceed regardless of success. */
  j = 1;
  len = sizeof(j);
  setsockopt(*s, IPPROTO_TCP, TCP_NODELAY, (const void *)&j, len);
}
#endif

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
thread_exit (void)
{
#ifdef Win32
  ExitThread ((DWORD) (0));
#else
  pthread_exit (NULL);
#endif
}


/* This thread keeps the Redis ephemeral 'alive' key alive.
 * Only one copy of this thread ever runs.
 */
#ifdef Win32
void *
ok (LPVOID x)
#else
void *
ok (void *x)
#endif
{
  char transaction[BS_LARGE];
  char buf[BS];  /* asumption is that response is short, otherwise its truncated to BS */
  int pr_n = -1;
  int j, m;
  char *key = (char *) x;
  double dt;
  size_t k = strlen (key);
  if (k > BS_LARGE - 128)
    {
      thread_exit ();
    }
  pr_n = snprintf(transaction, BS_LARGE, 
    "*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$2\r\nOK\r\n*3\r\n$6\r\nEXPIRE\r\n$%d\r\n%s\r\n$2\r\n10\r\n*1\r\n$4\r\nEXEC\r\n",
    (int)k, key, (int)k, key);
  if (pr_n < 0 || pr_n >= BS_LARGE)
    {
      thread_exit ();
    }
/* Check for thread termination every 1/10 sec, update Redis every 5s (expire
 * alive key after 10s). Exit the thread if the transaction fails.
 * Every 5s also check time limit and exit if exceeded.
 */
  m = 50;
  while (go > 0)
    {
      m += 1;
      snooze (100);
      if (m < 50) continue;
      if (time_limit > 0)
        {
          dt = difftime(time(NULL), start_time);
          if(dt > time_limit)
          {
            go = 0;
#ifdef Win32
  // WRITE ME XXX
#else
/* Rudely shut down this R process if task time limit is exceeded */
            kill (getpid(), SIGHUP);
#endif
            thread_exit ();
          }
        }
      j = msg (s, transaction, buf);
      if (j < 0)
        {
          go = 0;
          thread_exit ();
        }
      m = 0;
    }
  return NULL;
}

/* OK to call delOK repeatedly */
SEXP
delOK (void)
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
setOK (SEXP PORT, SEXP HOST, SEXP KEY, SEXP AUTH, SEXP TIMELIMIT)
{
  if (go > 0)
    return (R_NilValue);
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
  time_limit = *(REAL (TIMELIMIT));
  int j, k = strlen (auth);
#ifdef Win32
  WSAStartup (MAKEWORD (2, 2), &wsaData);
#endif
  tcpconnect (&s, host, port);
  go = 1;
  time(&start_time);
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

static const R_CallMethodDef CallEntries[] = {
  {"setOK", (DL_FUNC) &setOK, 5},
  {"delOK", (DL_FUNC) &delOK, 0},
  {NULL, NULL, 0}
};

#ifdef HAVE_VISIBILITY_ATTRIBUTE
__attribute__ ((visibility ("default")))
#endif
void
R_init_doRedis (DllInfo * dll)
{
  R_registerRoutines (dll, NULL, CallEntries, NULL, NULL);
  R_useDynamicSymbols (dll, FALSE);
  R_forceSymbols(dll, TRUE);
}
