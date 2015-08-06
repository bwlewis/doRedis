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
#endif

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include <R.h>
#define USE_RINTERNALS
#include <Rinternals.h>

#define BS 4096

int go;
#ifdef Win32
HANDLE t;
SOCKET s;
#else
int s;
pthread_t t;
#endif

/* tcpconnect
 * connect to the specified host and port, returning a socket
 */
#ifdef Win32
void tcpconnect(const char *host, int port)
{
  int j;
  char portstr[16];
  struct addrinfo *a = NULL, *ap = NULL;
  struct addrinfo hints;
  s = INVALID_SOCKET;
  snprintf(portstr, 16, "%d", port);
  ZeroMemory( &hints, sizeof(hints) );
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;
  j = getaddrinfo(host, portstr, &hints, &a);
  ap = a;
  s = socket(ap->ai_family, ap->ai_socktype, ap->ai_protocol);
  j=connect(s, ap->ai_addr, (int)ap->ai_addrlen);
  if(j!=0){
    s = INVALID_SOCKET;
  }
}
#else
void
tcpconnect(char *host, int port)
{
  struct hostent *h;
  struct sockaddr_in sa;
  int j;

  h = gethostbyname(host);
  if (!h)
    {
      s = -1;
    }
  else
    {
      s = socket(AF_INET, SOCK_STREAM, 0);
      memset((void *)&sa, 0, sizeof(sa));
      sa.sin_family = AF_INET;
      sa.sin_port = htons(port);
      sa.sin_addr = *(struct in_addr *) h->h_addr;
      j=connect(s, (struct sockaddr *) &sa, sizeof(sa));
      if (j < 0)
        {
          close(s);
          return;
        }
    }
}
#endif

#ifdef Win32
int
msg(SOCKET sock, char *cmd, char *response)
#else
int
msg(int sock, char *cmd, char *response)
#endif
{
  int j;
  j = send(sock, cmd, strlen(cmd), 0);
  if(j<0) return j;
  memset(response,0,BS);
  j = recv(sock, response, BS, 0);
  if(response[0] == '-') j = -1;
  return j;
}

#ifdef Win32
void *ok(LPVOID x)
#else
void *ok(void *x)
#endif
{
  char set[BS];
  char expire[BS];
  char buf[BS];
  int j, m;
  char *key = (char *)x;
  int k = strlen(key);
  memset(set,0,BS);
  memset(expire,0,BS);
  snprintf(set,BS,"*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$2\r\nOK\r\n", k, key);
  snprintf(expire,BS,"*3\r\n$6\r\nEXPIRE\r\n$%d\r\n%s\r\n$1\r\n5\r\n", k, key);

/* Check for thread termination every 1/10 sec, but only update Redis
 * every 3s (expire alive key after 5s).
 */
  m = -1;
  while(go>0){
    m += 1;
    if(m==0 || m>30) { 
      j = msg(s, set, buf);
#ifdef Win32
      if(j<0) ExitThread((DWORD)j);
#else
      if(j<0) pthread_exit(&j);
#endif
      j = msg(s, expire, buf);
#ifdef Win32
      if(j<0) ExitThread((DWORD)j);
#else
      if(j<0) pthread_exit(&j);
#endif
      m = 0;
    }
#ifdef Win32
    Sleep(100);
#else
    usleep(100000);
#endif
  }
  return NULL;
}

SEXP
delOK()
{
  if(go==0) return(R_NilValue);
  go = 0;
#ifdef Win32
  closesocket(s);
  WaitForSingleObject(t, INFINITE);
  WSACleanup();
#else
  close(s);
  pthread_join(t, NULL);
#endif
  return(R_NilValue);
}

SEXP
setOK(SEXP PORT, SEXP HOST, SEXP KEY, SEXP AUTH)
{
#ifdef Win32
  WSADATA wsaData;
  DWORD dw_thread_id;
#endif
  char authorize[BS];
  char buf[BS];
  char *host = (char *)CHAR(STRING_ELT(HOST, 0));
  int port = *(INTEGER(PORT));
  const char *key = CHAR(STRING_ELT(KEY, 0));
  const char *auth = CHAR(STRING_ELT(AUTH, 0));
  int k = strlen(auth);
/* check for AUTH and authorize if needed */
  if(k>0)
  {
    memset(authorize,0,BS);
    snprintf(authorize,BS,"*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n", k, auth);
    j = msg(s, authorize, buf);
  }

  if(go>0) return(R_NilValue);
#ifdef Win32
  WSAStartup(MAKEWORD(2, 2), &wsaData);
#endif
  tcpconnect(host, port);
  go = 1;
#ifdef Win32
  t = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)ok, (LPVOID)key, 0,
		   &dw_thread_id);
#else
  pthread_create(&t, NULL, ok, (void *)key);
#endif
  return(R_NilValue);
}
