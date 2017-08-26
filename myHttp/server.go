package myHttp

import (
	"syscall"
	"fmt"
	"os"
	"bytes"
	"strconv"
)

type Server struct {
	Threads int
	Port int
}

const (
	MaxEpollEvents = 10
	KB             = 2048
)

type Headers map[string][]byte
type Handler func(*Request)

func (c Headers) Has(key string) bool {
	_, ok := c[key]
	return ok
}

func (c Headers) GetUint(key string) (a int, b error) {
	val := c[key]
	a, b = strconv.Atoi(string(val))
	return
}

func (c Headers) GetString(key string) []byte {
	return c[key]
}

type Request struct {
	Path []byte
	Url []byte
	Arg []byte
	Body []byte
	Post bool

	Args Headers
	Answer bytes.Buffer
	Status int
}

func (c *Server) parseRequest(buf [KB]byte, r *Request, nbytes *int) {
	if bytes.Equal(buf[0:4], []byte("POST")) {
		r.Post = true
		for i := 0; i < *nbytes - 4; i++ {
			if bytes.Equal(buf[i : i + 4], []byte("\r\n\r\n")) {
				r.Body = buf[i + 4 : *nbytes]
				break
			}
		}
	}
	space := -1
	for i := 0; i < *nbytes; i++ {
		if buf[i] == ' ' {
			if space == -1 {
				space = i + 1
			} else {
				r.Url = buf[space : i]
				r.Path = r.Url
				break
			}
		}
	}

	for i, v := range r.Url {
		if v == '?' {
			r.Path = r.Url[:i]

			a := r.Url[i + 1 : ]
			if len(a) > 0 {
				splitted := bytes.Split(a, []byte("&"))
				for _, v := range splitted {
					kv := bytes.Split(v, []byte("="))
					r.Args[string(kv[0])] = kv[1]
				}
			}

			break
		}
	}
}

func (c *Server) prepareResponse( r *Request, b *bytes.Buffer) {
	b.Reset()
	b.WriteString("HTTP/1.1 ")
	if r.Status == 200 {
		b.WriteString("200 OK")
	} else if r.Status == 400 {
		b.WriteString("400 Not Found")
	} else if r.Status == 404 {
		b.WriteString("404 Bad Request")
	}
	b.WriteString("\r\nContent-Type: application/json\r\nContent-Length: ")
	b.WriteString(strconv.Itoa(r.Answer.Len()))
	b.WriteString("\r\nConnection: keep-alive\r\nServer: GoWebServ 1.0.2\r\n\r\n")
	b.Write(r.Answer.Bytes())
}

func (c *Server) clear(r *Request) {
	r.Answer.Reset()
	r.Path = nil
	r.Url = nil
	r.Args = nil
	r.Arg = nil
	r.Body = nil
	r.Post = false
	r.Status = 200
	r.Args = make(map[string][]byte)
}

func (c *Server) echo(in int, buf [KB]byte, r *Request, b *bytes.Buffer, h Handler) {
	c.clear(r)

	nbytes, _ := syscall.Read(in, buf[:])
	if nbytes == 0 {
		syscall.Close(in)
	} else {
		c.parseRequest(buf, r, &nbytes)
		h(r)
		c.prepareResponse(r, b)

		syscall.Write(in, b.Bytes())
		if r.Post {
			syscall.Close(in)
		}
	}
}

func (c *Server) worker(h Handler) {
	var event syscall.EpollEvent

	s, e := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, syscall.IPPROTO_TCP)
	if e != nil {
		fmt.Println("create socket: ", e)
		os.Exit(1)
	}

	syscall.SetNonblock(s, true)

	e = syscall.SetsockoptInt(s, syscall.SOL_SOCKET, syscall.SO_REUSEADDR | 0x0F, 1)
	if e != nil {
		fmt.Println("reuse-addr: ", e)
		os.Exit(1)
	}

	e = syscall.SetsockoptInt(s, syscall.SOL_SOCKET, 0x0F, 1)
	if e != nil {
		fmt.Println("reuse-port: ", e)
		os.Exit(1)
	}

	e = syscall.SetsockoptInt(s, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
	if e != nil {
		fmt.Println("nodelay: ", e)
		os.Exit(1)
	}

	e = syscall.SetsockoptInt(s, syscall.SOL_TCP, 0x17, 16384)
	if e != nil {
		fmt.Println("fastopen: ", e)
		os.Exit(1)
	}

	e = syscall.Bind(s, &syscall.SockaddrInet4{Port: c.Port, Addr: [4]byte{0, 0, 0, 0}})
	if e != nil {
		fmt.Println("bind: ", e)
		os.Exit(1)
	}

	e = syscall.Listen(s, 100)
	if e != nil {
		fmt.Println("listen: ", e)
		os.Exit(1)
	}

	epfd, e := syscall.EpollCreate1(0)
	if e != nil {
		fmt.Println("epoll_create1: ", e)
		os.Exit(1)
	}
	defer syscall.Close(epfd)

	event.Events = syscall.EPOLLIN
	event.Fd = int32(s)
	if e = syscall.EpollCtl(epfd, syscall.EPOLL_CTL_ADD, s, &event); e != nil {
		fmt.Println("epoll_ctl: ", e)
		os.Exit(1)
	}

	var ss = int32(s)
	var events [MaxEpollEvents]syscall.EpollEvent
	var buf [KB]byte
	var b *bytes.Buffer
	var r *Request
	b = new(bytes.Buffer)
	b.Grow(1000)
	r = new(Request)
	r.Answer.Grow(8192)

	for {
		nevents, e := syscall.EpollWait(epfd, events[:], -1)
		if e != nil {
			fmt.Println("epoll_wait: ", e)
			//break
		}

		for ev := 0; ev < nevents; ev++ {
			if events[ev].Fd != ss {
				c.echo(int(events[ev].Fd), buf, r, b, h)
			} else {
				ndf, _, e := syscall.Accept4(s, syscall.SOCK_NONBLOCK)
				if e != nil {
					fmt.Println("accept: ", e)
					os.Exit(1)
				}

				event.Events = syscall.EPOLLIN
				event.Fd = int32(ndf)
				if e = syscall.EpollCtl(epfd, syscall.EPOLL_CTL_ADD, ndf, &event); e != nil {
					fmt.Println("epoll_ctl: ", e)
					os.Exit(1)
				}
			}
		}
	}
}

func (c *Server) Start(h Handler) {
	for i := 0; i < c.Threads - 1; i++ {
		go c.worker(h)
	}
	c.worker(h)
}