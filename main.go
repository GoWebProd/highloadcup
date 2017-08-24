package main

import (
	"fmt"
	"sort"
	"bytes"

	"github.com/buger/jsonparser"
	//"net/http"
	//_ "net/http/pprof"
	"syscall"
	"os"
	"strconv"
	"net/url"
	"runtime"
)

var db = initializeSchema()

func getUser(_id []byte, c *Request) {
	id, ok := byteToInt(_id)
	if !ok {
		c.status = 404
		return
	}

	user := db.users[id]
	if user == nil {
		c.status = 404
		return
	}

	c.answer.Write(user.json)
	return
}

func getLocation(_id []byte, c *Request) {
	id, ok := byteToInt(_id)
	if !ok {
		c.status = 404
		return
	}

	loc := db.locations[id]
	if loc == nil {
		c.status = 404
		return
	}

	c.answer.Write(loc.json)
	return
}

func getVisit(_id []byte, c *Request) {
	id, ok := byteToInt(_id)
	if !ok {
		c.status = 404
		return
	}

	visit := db.visits[id]
	if visit == nil {
		c.status = 404
		return
	}

	c.answer.Write(visit.json)
}

func getUserVisits(_id []byte, c *Request) {
	id, ok := byteToInt(_id)
	if !ok {
		c.status = 404
		return
	}

	u := db.users[id]
	if u == nil {
		c.status = 404
		return
	}

	from := -9223372036854775808
	to := 9223372036854775807
	country := ""
	distance := 9223372036854775807

	var err error
	if c.args.Has("fromDate") {
		from, err = c.args.GetUint("fromDate")
		if err != nil {
			c.status = 400
			return
		}
	}
	if c.args.Has("toDate") {
		to, err = c.args.GetUint("toDate")
		if err != nil {
			c.status = 400
			return
		}
	}
	if c.args.Has("toDistance") {
		distance, err = c.args.GetUint("toDistance")
		if err != nil {
			c.status = 400
			return
		}
	}
	if c.args.Has("country") {
		country = string(c.args.GetString("country"))
		country, _ = url.QueryUnescape(country)
	}

	mem := Visits{}
	for _, v := range u.visits {
		if v.user.id != id {
			continue
		}
		if v.visited_at > int64(from) && v.visited_at < int64(to) && v.location.distance < int64(distance) && (country == "" || country == v.location.country) {
			mem = append(mem, v)
		}
	}
	sort.Slice(mem, func (i, j int) bool {
		return mem[i].visited_at < mem[j].visited_at
	})

	c.answer.Write(getVisitsJson(mem))
}

func getLocationAvg(_id []byte, c *Request) {
	id, ok := byteToInt(_id)
	if !ok {
		c.status = 404
		return
	}

	l := db.locations[id]
	if l == nil {
		c.status = 404
		return
	}

	from := -9223372036854775808
	to := 9223372036854775807
	from2 := -9223372036854775808
	to2 := 9223372036854775807
	gender := 2

	var err error
	if c.args.Has("fromDate") {
		from, err = c.args.GetUint("fromDate")
		if err != nil {
			c.status = 400
			return
		}
	}
	if c.args.Has("toDate") {
		to, err = c.args.GetUint("toDate")
		if err != nil {
			c.status = 400
			return
		}
	}
	if c.args.Has("fromAge") {
		from2, err = c.args.GetUint("fromAge")
		if err != nil {
			c.status = 400
			return
		}
	}
	if c.args.Has("toAge") {
		to2, err = c.args.GetUint("toAge")
		if err != nil {
			c.status = 400
			return
		}
	}
	if c.args.Has("gender") {
		g := c.args.GetString("gender")
		if bytes.Equal(g, []byte{'m'}) {
			gender = 1
		} else if bytes.Equal(g, []byte{'f'}) {
			gender = 0
		} else {
			c.status = 400
			return
		}
	}

	var sum int64
	var count int64
	for _, v := range l.visits {
		if v.location.id != id {
			continue
		}
		if v.visited_at > int64(from) && v.visited_at < int64(to) && v.user.age > from2 && v.user.age <= to2 &&
			(gender == 2 || v.user.gender == (gender == 1)){
			sum = sum + v.mark
			count++
		}
	}

	if count == 0 || sum == 0 {
		c.answer.Write(getAvgJson(0))
	} else {
		c.answer.Write(getAvgJson(float64(sum) / float64(count) + 1e-7))
	}
}

func addUser(c *Request) {
	u := &User{}
	var id, email, first_name, last_name, gender, birth_date bool
	jsonparser.ObjectEach(c.body, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
		switch {
		case bytes.Equal(key, []byte("id")) && dataType == jsonparser.Number:
			i, err := jsonparser.GetInt(value)
			if err != nil {
				return nil
			}
			u.id = uint(i)
			id = true
		case bytes.Equal(key, []byte("email")) && dataType == jsonparser.String:
			u.email = string(value)
			email = true
		case bytes.Equal(key, []byte("first_name"))&& dataType == jsonparser.String:
			u.first_name, _ = jsonparser.ParseString(value)
			first_name = true
		case bytes.Equal(key, []byte("last_name")) && dataType == jsonparser.String:
			u.last_name, _ = jsonparser.ParseString(value)
			last_name = true
		case bytes.Equal(key, []byte("gender")) && dataType == jsonparser.String:
			i := string(value)
			if i != "m" && i != "f" {
				return nil
			}
			u.gender = i == "m"
			gender = true
		case bytes.Equal(key, []byte("birth_date")) && dataType == jsonparser.Number:
			i, err := jsonparser.GetInt(value)
			if err != nil {
				return nil
			}
			u.birth_date = i
			u.age = countAge(&u.birth_date)
			birth_date = true
		}
		return nil
	})
	if !id || !email || !first_name || !last_name || !gender || !birth_date {
		c.status = 400
	} else {
		getUserJson(u)
		c.answer.WriteString("{}")
		db.users[u.id] = u
	}
}

func addLocation(c *Request) {
	l := &Location{}
	var id, place, country, city, distance bool

	jsonparser.ObjectEach(c.body, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
		switch {
		case bytes.Equal(key, []byte("id")) && dataType == jsonparser.Number:
			i, err := jsonparser.GetInt(value)
			if err != nil {
				return nil
			}
			l.id = uint(i)
			id = true
		case bytes.Equal(key, []byte("place")) && dataType == jsonparser.String:
			l.place, _ = jsonparser.ParseString(value)
			place = true
		case bytes.Equal(key, []byte("country")) && dataType == jsonparser.String:
			l.country, _ = jsonparser.ParseString(value)
			country = true
		case bytes.Equal(key, []byte("city")) && dataType == jsonparser.String:
			l.city, _ = jsonparser.ParseString(value)
			city = true
		case bytes.Equal(key, []byte("distance")) && dataType == jsonparser.Number:
			i, err := jsonparser.GetInt(value)
			if err != nil {
				return nil
			}
			l.distance = i
			distance = true
		}
		return nil
	})

	if !id || !place || !country || !city || !distance {
		c.status = 400
	} else {
		getLocationJson(l)
		c.answer.WriteString("{}")
		db.locations[l.id] = l
	}
}

func addVisit(c *Request) {
	v := &Visit{}
	var id, location, user, visited_at, mark bool

	jsonparser.ObjectEach(c.body, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
		switch {
		case bytes.Equal(key, []byte("id")):
			i, err := jsonparser.GetInt(value)
			if err != nil {
				return nil
			}
			v.id = uint(i)
			id = true
		case bytes.Equal(key, []byte("location")):
			i, err := jsonparser.GetInt(value)
			if err != nil {
				return nil
			}
			v.location = db.locations[uint(i)]
			if v.location != nil {
				location = true
			}
		case bytes.Equal(key, []byte("user")):
			i, err := jsonparser.GetInt(value)
			if err != nil {
				return nil
			}
			v.user = db.users[uint(i)]
			if v.user != nil {
				user = true
			}
		case bytes.Equal(key, []byte("visited_at")):
			i, err := jsonparser.GetInt(value)
			if err != nil {
				return nil
			}
			v.visited_at = i
			visited_at = true
		case bytes.Equal(key, []byte("mark")):
			i, err := jsonparser.GetInt(value)
			if err != nil {
				return nil
			}
			v.mark = i
			mark = true
		}
		return nil
	})

	if !id || !location || !user || !visited_at || !mark {
		c.status = 400
	} else {
		getVisitJson(v)
		c.answer.WriteString("{}")
		db.visits[v.id] = v
		v = db.visits[v.id]
		v.user.visits = append(v.user.visits, v)
		v.location.visits = append(v.location.visits, v)
	}
}

func updateUser(_id []byte, c *Request) {
	id, ok := byteToInt(_id)
	if !ok {
		c.status = 404
		return
	}

	u := db.users[id]
	if u == nil {
		c.status = 404
		return
	}

	var cc int
	var err bool
	var u2 User
	u2.email = u.email
	u2.first_name = u.first_name
	u2.birth_date = u.birth_date
	u2.last_name = u.last_name
	u2.age = u.age
	u2.gender = u.gender

	jsonparser.ObjectEach(c.body, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
		cc++
		if err {
			return nil
		}

		switch {
		case bytes.Equal(key, []byte("email")):
			if dataType != jsonparser.String {
				err = true
				return nil
			}
			u2.email = string(value)
		case bytes.Equal(key, []byte("first_name")):
			if dataType != jsonparser.String {
				err = true
				return nil
			}
			u2.first_name, _ = jsonparser.ParseString(value)
		case bytes.Equal(key, []byte("last_name")):
			if dataType != jsonparser.String {
				err = true
				return nil
			}
			u2.last_name, _ = jsonparser.ParseString(value)
		case bytes.Equal(key, []byte("gender")):
			if dataType != jsonparser.String {
				err = true
				return nil
			}
			i := string(value)
			if i != "m" && i != "f" {
				err = true
				return nil
			}
			u2.gender = i == "m"
		case bytes.Equal(key, []byte("birth_date")):
			if dataType != jsonparser.Number {
				err = true
				return nil
			}
			i, er := jsonparser.GetInt(value)
			if er != nil {
				err = true
				return nil
			}
			u2.birth_date = i
			u2.age = countAge(&u2.birth_date)
		}
		return nil
	})

	if err || cc == 0 {
		c.status = 400
	} else {
		u.email = u2.email
		u.first_name = u2.first_name
		u.last_name = u2.last_name
		u.birth_date = u2.birth_date
		u.age = u2.age
		u.gender = u2.gender

		getUserJson(u)
		c.answer.WriteString("{}")
	}
}

func updateLocation(_id []byte, c *Request) {
	id, ok := byteToInt(_id)
	if !ok {
		c.status = 404
		return
	}

	l := db.locations[id]
	if l == nil {
		c.status = 404
		return
	}

	var cc int
	var err bool
	var l2 Location
	l2.place = l.place
	l2.distance = l.distance
	l2.city = l.city
	l2.country = l.country

	jsonparser.ObjectEach(c.body, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
		cc++
		if err {
			return nil
		}

		switch {
		case bytes.Equal(key, []byte("place")):
			if dataType != jsonparser.String {
				err = true
				return nil
			}
			l2.place, _ = jsonparser.ParseString(value)
		case bytes.Equal(key, []byte("country")):
			if dataType != jsonparser.String {
				err = true
				return nil
			}
			l2.country, _ = jsonparser.ParseString(value)
		case bytes.Equal(key, []byte("city")):
			if dataType != jsonparser.String {
				err = true
				return nil
			}
			l2.city, _ = jsonparser.ParseString(value)
		case bytes.Equal(key, []byte("distance")):
			if dataType != jsonparser.Number {
				err = true
				return nil
			}
			i, er := jsonparser.GetInt(value)
			if er != nil {
				err = true
				return nil
			}
			l2.distance = i
		}
		return nil
	})

	if err || cc == 0 {
		c.status = 400
	} else {
		l.place = l2.place
		l.country = l2.country
		l.city = l2.city
		l.distance = l2.distance

		getLocationJson(l)
		c.answer.WriteString("{}")
	}
}

func updateVisit(_id []byte, c *Request) {

	id, ok := byteToInt(_id)
	if !ok {
		c.status = 404
		return
	}

	v := db.visits[id]
	if v == nil {
		c.status = 404
		return
	}

	var err bool
	var cc int
	var v2 Visit
	v2.user = v.user
	v2.location = v.location
	v2.mark = v.mark
	v2.visited_at = v.visited_at

	jsonparser.ObjectEach(c.body, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
		cc++
		if err {
			return nil
		}

		switch {
		case bytes.Equal(key, []byte("location")):
			i, er := jsonparser.GetInt(value)
			if er != nil {
				err = true
				return nil
			}
			v2.location = db.locations[uint(i)]
			if v.location == nil {
				err = true
				return nil
			}
		case bytes.Equal(key, []byte("user")):
			i, er := jsonparser.GetInt(value)
			if er != nil {
				err = true
				return nil
			}
			v2.user = db.users[uint(i)]
			if v.user == nil {
				err = true
				return nil
			}
		case bytes.Equal(key, []byte("visited_at")):
			i, er := jsonparser.GetInt(value)
			if er != nil {
				err = true
				return nil
			}
			v2.visited_at = i
		case bytes.Equal(key, []byte("mark")):
			i, er := jsonparser.GetInt(value)
			if er != nil {
				err = true
				return nil
			}
			v2.mark = i
		}
		return nil
	})

	if err || cc == 0 {
		c.status = 400
	} else {
		v.mark = v2.mark
		v.visited_at = v2.visited_at
		if v2.user != v.user {
			v.user = v2.user
			v.user.visits = append(v.user.visits, v)
		}
		if v2.location != v.location {
			v.location = v2.location
			v.location.visits = append(v.location.visits, v)
		}
		getVisitJson(v)
		c.answer.WriteString("{}")
	}

	return
}

func HandleRequest(c *Request)  {
	p := c.path
	l := len(p)

	switch {
	case l > 7 && bytes.Equal(p[:7], []byte("/users/")):
		if c.post {
			if l == 10 && bytes.Equal(p[7:], []byte("new")) {
				addUser(c)
			} else {
				updateUser(p[7:], c)
			}
		} else {
			if l < 15 || !bytes.Equal(p[l-7:], []byte("/visits")) {
				getUser(p[7:], c)
			} else {
				getUserVisits(p[7:l-7], c)
			}
		}

	case l > 11 && bytes.Equal(p[:11], []byte("/locations/")):
		if c.post {
			if l == 14 && bytes.Equal(p[11:], []byte("new")) {
				addLocation(c)
				return
			} else {
				updateLocation(p[11:], c)
				return
			}
		} else {
			if l < 16 || !bytes.Equal(p[l-4:], []byte("/avg")) {
				getLocation(p[11:], c)
			} else {
				getLocationAvg(p[11:l-4], c)
			}
		}
	case l > 8 && bytes.Equal(p[:8], []byte("/visits/")):
		if c.post {
			if l == 11 && bytes.Equal(p[8:], []byte("new")) {
				addVisit(c)
				return
			} else {
				updateVisit(p[8:], c)
				return
			}
		} else {
			getVisit(p[8:], c)
		}
	default:
		c.status = 404
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const (
	MaxEpollEvents = 10
	KB             = 1024
)

type headers map[string][]byte

func (c headers) Has(key string) bool {
	_, ok := c[key]
	return ok
}

func (c headers) GetUint(key string) (a int, b error) {
	val := c[key]
	a, b = strconv.Atoi(string(val))
	return
}

func (c headers) GetString(key string) []byte {
	return c[key]
}

type Request struct {
	path []byte
	url []byte
	arg []byte
	body []byte
	post bool

	args headers
	answer bytes.Buffer
	status int
}

var buf [KB]byte
func echo(in int) {

	nbytes, _ := syscall.Read(in, buf[:])
	if nbytes == 0 {
		syscall.Close(in)
	} else if nbytes > 0 {
		r := Request{}
		r.status = 200
		if bytes.Equal(buf[0:4], []byte("POST")) {
			r.post = true
			for i := 0; i < nbytes - 4; i++ {
				if bytes.Equal(buf[i : i + 4], []byte("\r\n\r\n")) {
					r.body = buf[i + 4 : nbytes]
					break
				}
			}
		}
		space := -1
		for i := 0; i < nbytes; i++ {
			if buf[i] == ' ' {
				if space == -1 {
					space = i + 1
				} else {
					r.url = buf[space : i]
					r.path = r.url
					break
				}
			}
		}

		r.args = make(map[string][]byte)
		for i, v := range r.url {
			if v == '?' {
				r.path = r.url[:i]

				a := r.url[i + 1 : ]
				if len(a) > 0 {
					splitted := bytes.Split(a, []byte("&"))
					for _, v := range splitted {
						kv := bytes.Split(v, []byte("="))
						r.args[string(kv[0])] = kv[1]
					}
				}

				break
			}
		}

		HandleRequest(&r)
		var b bytes.Buffer
		b.Grow(250)
		b.WriteString("HTTP/1.1 ")
		if r.status == 200 {
			b.WriteString("200 OK")
		} else if r.status == 400 {
			b.WriteString("400 Not Found")
		} else if r.status == 404 {
			b.WriteString("404 Bad Request")
		}
		b.WriteString("\r\nContent-Type: application/json\r\nContent-Length: ")
		b.WriteString(strconv.Itoa(r.answer.Len()))
		b.WriteString("\r\nConnection: keep-alive\r\nServer: GoWebProd 1.0\r\n\r\n")
		b.Write(r.answer.Bytes())

		syscall.Write(in, b.Bytes())
		if r.post {
			syscall.Close(in)
		}
	}
}

func main()  {
	runtime.GOMAXPROCS(runtime.NumCPU())
	//go http.ListenAndServe("0.0.0.0:8081", nil)

	var event syscall.EpollEvent

	s, e := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, syscall.IPPROTO_TCP)
	if e != nil {
		fmt.Println("create socket: ", e)
		os.Exit(1)
	}

	syscall.SetNonblock(s, true)

	e = syscall.SetsockoptInt(s, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
	if e != nil {
		fmt.Println("reuse: ", e)
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

	e = syscall.Bind(s, &syscall.SockaddrInet4{Port: 80, Addr: [4]byte{0, 0, 0, 0}})
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
	for {
		nevents, e := syscall.EpollWait(epfd, events[:], -1)
		if e != nil {
			fmt.Println("epoll_wait: ", e)
			break
		}

		for ev := 0; ev < nevents; ev++ {
			if events[ev].Fd != ss {
				echo(int(events[ev].Fd))
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