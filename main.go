package main

import (
	"log"
	"strconv"
	"fmt"
	"sort"
	"bytes"

	"github.com/valyala/fasthttp"
	"github.com/qiangxue/fasthttp-routing"
	"github.com/buger/jsonparser"
)

var db = initializeSchema()

func getUser(c *routing.Context) error {
	id, _ := strconv.Atoi(c.Param("id"))

	user := db.users[uint(id)]
	if user == nil {
		c.Response.SetStatusCode(404)
		return nil
	}

	gender := "m"
	if user.gender == false {
		gender = "f"
	}
	fmt.Fprintf(c, "{\"id\":%d,\"email\":\"%s\",\"first_name\":\"%s\",\"last_name\":\"%s\",\"gender\":\"%s\",\"birth_date\":%d}",
		user.id, user.email, user.first_name, user.last_name, gender, user.birth_date)
	return nil
}

func getLocation(c *routing.Context) error {
	id, _ := strconv.Atoi(c.Param("id"))

	loc := db.locations[uint(id)]
	if loc == nil {
		c.Response.SetStatusCode(404)
		return nil
	}

	fmt.Fprintf(c, "{\"id\":%d,\"place\":\"%s\",\"country\":\"%s\",\"city\":\"%s\",\"distance\":%d}",
		loc.id, loc.place, loc.country, loc.city, loc.distance)
	return nil
}

func getVisit(c *routing.Context) error {
	id, _ := strconv.Atoi(c.Param("id"))

	visit := db.visits[uint(id)]
	if visit == nil {
		c.Response.SetStatusCode(404)
		return nil
	}

	fmt.Fprintf(c, "{\"id\":%d,\"location\":%d,\"user\":%d,\"visited_at\":%d,\"mark\":%d}",
		visit.id, visit.location.id, visit.user.id, visit.visited_at, visit.mark)
	return nil
}

func getUserVisits(c *routing.Context) error {
	id2, _ := strconv.Atoi(c.Param("id"))
	id := uint(id2)

	u, ok := db.users[id]
	if !ok {
		c.Response.SetStatusCode(404)
		return nil
	}

	from := -9223372036854775808
	to := 9223372036854775807
	country := ""
	distance := 9223372036854775807

	qA := c.QueryArgs()
	var err error
	if qA.Has("fromDate") {
		from, err = qA.GetUint("fromDate")
		if err != nil {
			c.Response.SetStatusCode(400)
			return nil
		}
	}
	if qA.Has("toDate") {
		to, err = qA.GetUint("toDate")
		if err != nil {
			c.Response.SetStatusCode(400)
			return nil
		}
	}
	if qA.Has("toDistance") {
		distance, err = qA.GetUint("toDistance")
		if err != nil {
			c.Response.SetStatusCode(400)
			return nil
		}
	}
	if qA.Has("country") {
		country = string(qA.Peek("country"))
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
	sort.Sort(mem)

	fmt.Fprint(c, "{\"visits\":[")
	for i, v := range mem {
		if i > 0 {
			fmt.Fprint(c, ",")
		}
		fmt.Fprintf(c, "{\"mark\":%d,\"visited_at\":%d,\"place\":\"%s\"}",
			v.mark, v.visited_at, v.location.place)
	}
	fmt.Fprint(c, "]}")

	return nil
}

func getLocationAvg(c *routing.Context) error {
	id2, _ := strconv.Atoi(c.Param("id"))
	id := uint(id2)

	l, ok := db.locations[id]
	if !ok {
		c.Response.SetStatusCode(404)
		return nil
	}

	from := -9223372036854775808
	to := 9223372036854775807
	from2 := -9223372036854775808
	to2 := 9223372036854775807
	gender := 2

	qA := c.QueryArgs()
	var err error
	if qA.Has("fromDate") {
		from, err = qA.GetUint("fromDate")
		if err != nil {
			c.Response.SetStatusCode(400)
			return nil
		}
	}
	if qA.Has("toDate") {
		to, err = qA.GetUint("toDate")
		if err != nil {
			c.Response.SetStatusCode(400)
			return nil
		}
	}
	if qA.Has("fromAge") {
		from2, err = qA.GetUint("fromAge")
		if err != nil {
			c.Response.SetStatusCode(400)
			return nil
		}
	}
	if qA.Has("toAge") {
		to2, err = qA.GetUint("toAge")
		if err != nil {
			c.Response.SetStatusCode(400)
			return nil
		}
	}
	if qA.Has("gender") {
		g := qA.Peek("gender")
		if bytes.Equal(g, []byte{'m'}) {
			gender = 1
		} else if bytes.Equal(g, []byte{'f'}) {
			gender = 0
		} else {
			c.Response.SetStatusCode(400)
			return nil
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

	fmt.Fprint(c, "{\"avg\":")
	if count == 0 || sum == 0 {
		fmt.Fprint(c, 0)
	} else {
		fmt.Fprintf(c, "%.5f", float64(sum) / float64(count) + 1e-7)
	}
	fmt.Fprint(c, "}")

	return nil
}

func addUser(c *routing.Context) error {
	u := &User{}
	var id, email, first_name, last_name, gender, birth_date bool
	jsonparser.ObjectEach(c.PostBody(), func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
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
		c.Response.SetStatusCode(400)
	} else {
		db.users[u.id] = u
		fmt.Fprint(c, "{}")
	}
	c.SetConnectionClose()
	return nil
}

func addLocation(c *routing.Context) error {
	defer c.SetConnectionClose()

	l := &Location{}
	var id, place, country, city, distance bool

	jsonparser.ObjectEach(c.PostBody(), func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
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
		c.Response.SetStatusCode(400)
	} else {
		db.locations[l.id] = l
		fmt.Fprint(c, "{}")
	}
	return nil
}

func addVisit(c *routing.Context) error {
	defer c.SetConnectionClose()

	v := &Visit{}
	var id, location, user, visited_at, mark bool

	jsonparser.ObjectEach(c.PostBody(), func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
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
		c.Response.SetStatusCode(400)
	} else {
		db.visits[v.id] = v
		v = db.visits[v.id]
		v.user.visits = append(v.user.visits, v)
		v.location.visits = append(v.location.visits, v)
		fmt.Fprint(c, "{}")
	}
	return nil
}

func updateUser(c *routing.Context) error {
	defer c.SetConnectionClose()

	id, _ := strconv.Atoi(c.Param("id"))

	u, ok := db.users[uint(id)]
	if !ok {
		c.Response.SetStatusCode(404)
		return nil
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

	jsonparser.ObjectEach(c.PostBody(), func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
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
		c.Response.SetStatusCode(400)
	} else {
		u.email = u2.email
		u.first_name = u2.first_name
		u.last_name = u2.last_name
		u.birth_date = u2.birth_date
		u.age = u2.age
		u.gender = u2.gender
		fmt.Fprint(c, "{}")
	}

	return nil
}

func updateLocation(c *routing.Context) error {
	defer c.SetConnectionClose()

	id, _ := strconv.Atoi(c.Param("id"))

	l, ok := db.locations[uint(id)]
	if !ok {
		c.Response.SetStatusCode(404)
		return nil
	}

	var cc int
	var err bool
	var l2 Location
	l2.place = l.place
	l2.distance = l.distance
	l2.city = l.city
	l2.country = l.country

	jsonparser.ObjectEach(c.PostBody(), func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
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
		c.Response.SetStatusCode(400)
	} else {
		l.place = l2.place
		l.country = l2.country
		l.city = l2.city
		l.distance = l2.distance
		fmt.Fprint(c, "{}")
	}

	return nil
}

func updateVisit(c *routing.Context) error {
	defer c.SetConnectionClose()

	id, _ := strconv.Atoi(c.Param("id"))

	v, ok := db.visits[uint(id)]
	if !ok {
		c.Response.SetStatusCode(404)
		return nil
	}

	var err bool
	var cc int
	var v2 Visit
	v2.user = v.user
	v2.location = v.location
	v2.mark = v.mark
	v2.visited_at = v.visited_at

	jsonparser.ObjectEach(c.PostBody(), func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
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
		c.Response.SetStatusCode(400)
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
		fmt.Fprint(c, "{}")
	}

	return nil
}

func main() {
	router := routing.New()
	router.Get("/users/<id:\\d+>", getUser).Post(updateUser)
	router.Get("/locations/<id:\\d+>", getLocation).Post(updateLocation)
	router.Get("/visits/<id:\\d+>", getVisit).Post(updateVisit)

	router.Get("/users/<id:\\d+>/visits", getUserVisits)
	router.Get("/locations/<id:\\d+>/avg", getLocationAvg)

	router.Post("/users/new", addUser)
	router.Post("/locations/new", addLocation)
	router.Post("/visits/new", addVisit)


	if err := fasthttp.ListenAndServe(":80", router.HandleRequest); err != nil {
		log.Fatalf("Error in ListenAndServe: %s", err)
	}
}