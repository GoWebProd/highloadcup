package myHttp

type Request struct {
	Path []byte
	Url []byte
	Arg []byte
	Body []byte
	Post bool

	Args Headers
	Answer Buffer
	Status int
}