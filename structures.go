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
}

type Location struct {
	id uint
	place string
	country string
	city string
	distance int64

	visits Visits
}

type Visit struct {
	id uint
	location *Location
	user *User
	visited_at int64
	mark int64
}

type Schema struct {
	users map[uint]*User
	locations map[uint]*Location
	visits map[uint]*Visit
}

type Visits []*Visit

func (slice Visits) Len() int {
	return len(slice)
}

func (slice Visits) Less(i, j int) bool {
	return slice[i].visited_at < slice[j].visited_at
}

func (slice Visits) Swap(i, j int)  {
	slice[i], slice[j] = slice[j], slice[i]
}