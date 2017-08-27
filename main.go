package main

import (
	"bytes"
	"net/url"
	"runtime"

	"github.com/buger/jsonparser"
	"highload/myHttp"

	
	//"net/http"
	//_ "net/http/pprof"
	"os"
	"runtime/pprof"
	"fmt"
)

var db = initializeSchema()

func getUser(_id []byte, c *myHttp.Request) {
	id, ok := byteToInt(_id)
	if !ok {
		c.Status = 404
		return
	}

	user := db.users[id]
	if user == nil {
		c.Status = 404
		return
	}

	getUserJson(user, &c.Answer)
	return
}

func getLocation(_id []byte, c *myHttp.Request) {
	id, ok := byteToInt(_id)
	if !ok {
		c.Status = 404
		return
	}

	loc := db.locations[id]
	if loc == nil {
		c.Status = 404
		return
	}

	getLocationJson(loc, &c.Answer)
	return
}

func getVisit(_id []byte, c *myHttp.Request) {
	id, ok := byteToInt(_id)
	if !ok {
		c.Status = 404
		return
	}

	visit := db.visits[id]
	if visit == nil {
		c.Status = 404
		return
	}

	getVisitJson(visit, &c.Answer)
}

func getUserVisits(_id []byte, c *myHttp.Request) {
	id, ok := byteToInt(_id)
	if !ok {
		c.Status = 404
		return
	}

	u := db.users[id]
	if u == nil {
		c.Status = 404
		return
	}

	from := -9223372036854775808
	to := 9223372036854775807
	country := ""
	distance := 9223372036854775807

	var err bool
	if c.Args.Has([]byte("fromDate")) {
		from, err = c.Args.GetInt([]byte("fromDate"))
		if err {
			c.Status = 400
			return
		}
	}
	if c.Args.Has([]byte("toDate")) {
		to, err = c.Args.GetInt([]byte("toDate"))
		if err {
			c.Status = 400
			return
		}
	}
	if c.Args.Has([]byte("toDistance")) {
		distance, err = c.Args.GetInt([]byte("toDistance"))
		if err {
			c.Status = 400
			return
		}
	}
	if c.Args.Has([]byte("country")) {
		country = string(c.Args.GetString([]byte("country")))
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

	for i := 0; i < len(mem) - 1; i++ {
		if mem[i + 1].visited_at < mem[i].visited_at {
			mem[i + 1], mem[i] = mem[i], mem[i + 1]
			if i > 0 {
				i -= 2
			}
		}
	}

	getVisitsJson(mem, &c.Answer)
}

func getLocationAvg(_id []byte, c *myHttp.Request) {
	id, ok := byteToInt(_id)
	if !ok {
		c.Status = 404
		return
	}

	l := db.locations[id]
	if l == nil {
		c.Status = 404
		return
	}

	from := -9223372036854775808
	to := 9223372036854775807
	from2 := -9223372036854775808
	to2 := 9223372036854775807
	gender := 2

	var err bool
	if c.Args.Has([]byte("fromDate")) {
		from, err = c.Args.GetInt([]byte("fromDate"))
		if err {
			c.Status = 400
			return
		}
	}
	if c.Args.Has([]byte("toDate")) {
		to, err = c.Args.GetInt([]byte("toDate"))
		if err {
			c.Status = 400
			return
		}
	}
	if c.Args.Has([]byte("fromAge")) {
		from2, err = c.Args.GetInt([]byte("fromAge"))
		if err {
			c.Status = 400
			return
		}
	}
	if c.Args.Has([]byte("toAge")) {
		to2, err = c.Args.GetInt([]byte("toAge"))
		if err {
			c.Status = 400
			return
		}
	}
	if c.Args.Has([]byte("gender")) {
		g := c.Args.GetString([]byte("gender"))
		if bytes.Equal(g, []byte{'m'}) {
			gender = 1
		} else if bytes.Equal(g, []byte{'f'}) {
			gender = 0
		} else {
			c.Status = 400
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
		getAvgJson(0, &c.Answer)
	} else {
		getAvgJson(float64(sum) / float64(count) + 1e-7, &c.Answer)
	}
}

func addUser(c *myHttp.Request) {
	u := &User{}
	var id, email, first_name, last_name, gender, birth_date bool
	jsonparser.ObjectEach(c.Body, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
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
		c.Status = 400
	} else {
		c.Answer.WriteString("{}")
		db.users[u.id] = u
	}
}

func addLocation(c *myHttp.Request) {
	l := &Location{}
	var id, place, country, city, distance bool

	jsonparser.ObjectEach(c.Body, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
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
		c.Status = 400
	} else {
		c.Answer.WriteString("{}")
		db.locations[l.id] = l
	}
}

func addVisit(c *myHttp.Request) {
	v := &Visit{}
	var id, location, user, visited_at, mark bool

	jsonparser.ObjectEach(c.Body, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
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
		c.Status = 400
	} else {
		c.Answer.WriteString("{}")
		db.visits[v.id] = v
		v = db.visits[v.id]
		v.user.visits = append(v.user.visits, v)
		v.location.visits = append(v.location.visits, v)
	}
}

func updateUser(_id []byte, c *myHttp.Request) {
	id, ok := byteToInt(_id)
	if !ok {
		c.Status = 404
		return
	}

	u := db.users[id]
	if u == nil {
		c.Status = 404
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

	jsonparser.ObjectEach(c.Body, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
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
		c.Status = 400
	} else {
		u.email = u2.email
		u.first_name = u2.first_name
		u.last_name = u2.last_name
		u.birth_date = u2.birth_date
		u.age = u2.age
		u.gender = u2.gender

		c.Answer.WriteString("{}")
	}
}

func updateLocation(_id []byte, c *myHttp.Request) {
	id, ok := byteToInt(_id)
	if !ok {
		c.Status = 404
		return
	}

	l := db.locations[id]
	if l == nil {
		c.Status = 404
		return
	}

	var cc int
	var err bool
	var l2 Location
	l2.place = l.place
	l2.distance = l.distance
	l2.city = l.city
	l2.country = l.country

	jsonparser.ObjectEach(c.Body, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
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
		c.Status = 400
	} else {
		l.place = l2.place
		l.country = l2.country
		l.city = l2.city
		l.distance = l2.distance

		c.Answer.WriteString("{}")
	}
}

func updateVisit(_id []byte, c *myHttp.Request) {

	id, ok := byteToInt(_id)
	if !ok {
		c.Status = 404
		return
	}

	v := db.visits[id]
	if v == nil {
		c.Status = 404
		return
	}

	var err bool
	var cc int
	var v2 Visit
	v2.user = v.user
	v2.location = v.location
	v2.mark = v.mark
	v2.visited_at = v.visited_at

	jsonparser.ObjectEach(c.Body, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
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
		c.Status = 400
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
		c.Answer.WriteString("{}")
	}

	return
}

func HandleRequest(c *myHttp.Request) {
	p := c.Path
	l := len(p)

	switch {
	case l > 7 && bytes.Equal(p[:7], []byte("/users/")):
		if c.Post {
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
		if c.Post {
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
		if c.Post {
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
	case l == 2 && bytes.Equal(p[:2], []byte("/a")):
		if fd, err := os.Create(`pprof.mem`); err == nil {
			pprof.WriteHeapProfile(fd)
			fd.Close()
		}
	default:
		c.Status = 404
	}
}


func main()  {
	fmt.Println("Processors: ", runtime.NumCPU())
	runtime.GOMAXPROCS(runtime.NumCPU())
	//go http.ListenAndServe("0.0.0.0:8081", nil)
	serv := myHttp.Server{Threads: runtime.NumCPU(), Port: 80}
	serv.Start(HandleRequest)
}