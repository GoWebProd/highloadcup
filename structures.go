package main

type User struct {
	id  uint
	email string
	first_name  string
	last_name string
	gender bool
	birth_date int64

	age   int
	visits Visits
	json []byte
}

type Location struct {
	id uint
	place string
	country string
	city string
	distance int64

	visits Visits
	json []byte
}

type Visit struct {
	id uint
	location *Location
	user *User
	visited_at int64
	mark int64

	json []byte
}

type Schema struct {
	users map[uint]*User
	locations map[uint]*Location
	visits map[uint]*Visit
}

type Visits []*Visit