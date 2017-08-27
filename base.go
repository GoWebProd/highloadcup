package main

import (
	"os"
	"os/exec"
	"strconv"
	"io/ioutil"
	"time"
	"fmt"

	"github.com/buger/jsonparser"
	"runtime"
)

func countAge(timestamp *int64) int {
	now := time.Now()
	t := time.Unix(*timestamp, 0)

	years := now.Year() - t.Year()
	if now.Month() > t.Month() || now.Month() == t.Month() && now.Day() >= t.Day() {
		years += 1
	}

	return years
}

func initializeSchema() (db*Schema) {
	db = &Schema{make(map[uint]*User, 1100000),make(map[uint]*Location, 800000),make(map[uint]*Visit, 10050000)}
	_, err := exec.Command("sh","-c", "unzip /tmp/data/data.zip -d /tmp/base/").Output()
	if err != nil {
		fmt.Fprintln(os.Stdout, err.Error())
	}

	var id = 1
	var fileName = "/tmp/base/users_" + strconv.Itoa(id) + ".json"
	for fileExists(fileName) {
		dat, _ := ioutil.ReadFile(fileName)
		var c = 0
		jsonparser.ArrayEach(dat, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
			c++
			id, _ := jsonparser.GetInt(value, "id")
			email, _ := jsonparser.GetString(value, "email")
			f_name, _ := jsonparser.GetString(value, "first_name")
			l_name, _ := jsonparser.GetString(value, "last_name")
			gender, _ := jsonparser.GetString(value, "gender")
			b_date, _ := jsonparser.GetInt(value, "birth_date")
			u := &User{uint(id),email,f_name,l_name,gender == "m",b_date,countAge(&b_date),Visits{}}
			db.users[uint(id)] = u
		}, "users")
		id++
		fileName = "/tmp/base/users_" + strconv.Itoa(id) + ".json"
	}
	runtime.GC()

	id = 1
	fileName = "/tmp/base/locations_" + strconv.Itoa(id) + ".json"
	for fileExists(fileName) {
		dat, _ := ioutil.ReadFile(fileName)
		var c = 0
		jsonparser.ArrayEach(dat, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
			c++
			id, _ := jsonparser.GetInt(value, "id")
			place, _ := jsonparser.GetString(value, "place")
			country, _ := jsonparser.GetString(value, "country")
			city, _ := jsonparser.GetString(value, "city")
			distance, _ := jsonparser.GetInt(value, "distance")
			l := &Location{uint(id),place,country,city,distance,Visits{}}
			db.locations[uint(id)] = l

		}, "locations")
		id++
		fileName = "/tmp/base/locations_" + strconv.Itoa(id) + ".json"
	}
	runtime.GC()

	id = 1
	fileName = "/tmp/base/visits_" + strconv.Itoa(id) + ".json"
	for fileExists(fileName) {
		dat, _ := ioutil.ReadFile(fileName)
		var c = 0
		jsonparser.ArrayEach(dat, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
			c++
			id, _ := jsonparser.GetInt(value, "id")
			location, _ := jsonparser.GetInt(value, "location")
			user, _ := jsonparser.GetInt(value, "user")
			visited_at, _ := jsonparser.GetInt(value, "visited_at")
			mark, _ := jsonparser.GetInt(value, "mark")

			l := db.locations[uint(location)]
			u := db.users[uint(user)]
			v := &Visit{uint(id),l,u,visited_at,mark}
			//getVisitJson(v)
			db.visits[uint(id)] = v
			l.visits = append(l.visits, v)
			u.visits = append(u.visits, v)

		}, "visits")
		id++
		fileName = "/tmp/base/visits_" + strconv.Itoa(id) + ".json"
	}
	runtime.GC()

	fmt.Fprintf(os.Stdout, "Users: %d\nLocations: %d\nVisits: %d\n", len(db.users), len(db.locations), len(db.visits))
	return
}