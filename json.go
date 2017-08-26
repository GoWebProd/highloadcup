package main

import (
	"github.com/mailru/easyjson/jwriter"
	"bytes"
)

func getUserJson(u *User, b *bytes.Buffer){
	w := jwriter.Writer{}

	w.RawString("{\"id\":")
	w.Uint(u.id)
	w.RawString(",\"email\":")
	w.String(u.email)
	w.RawString(",\"first_name\":")
	w.String(u.first_name)
	w.RawString(",\"last_name\":")
	w.String(u.last_name)
	w.RawString(",\"gender\":")
	if u.gender {
		w.String("m")
	} else {
		w.String("f")
	}
	w.RawString(",\"birth_date\":")
	w.Int64(u.birth_date)
	w.RawString("}")

	b.Write(w.Buffer.BuildBytes())
}

func getLocationJson(l *Location, b* bytes.Buffer) {
	w := jwriter.Writer{}

	w.RawString("{\"id\":")
	w.Uint(l.id)
	w.RawString(",\"place\":")
	w.String(l.place)
	w.RawString(",\"country\":")
	w.String(l.country)
	w.RawString(",\"city\":")
	w.String(l.city)
	w.RawString(",\"distance\":")
	w.Int64(l.distance)
	w.RawString("}")

	b.Write(w.Buffer.BuildBytes())
}

func getVisitJson(v *Visit, b* bytes.Buffer) {
	w := jwriter.Writer{}

	w.RawString("{\"id\":")
	w.Uint(v.id)
	w.RawString(",\"location\":")
	w.Uint(v.location.id)
	w.RawString(",\"user\":")
	w.Uint(v.user.id)
	w.RawString(",\"visited_at\":")
	w.Int64(v.visited_at)
	w.RawString(",\"mark\":")
	w.Int64(v.mark)
	w.RawString("}")

	b.Write(w.Buffer.BuildBytes())
}

func getVisitsJson(v Visits, buf *bytes.Buffer) {
	w := jwriter.Writer{}

	w.RawString("{\"visits\":[")
	for i, vv := range v {
		if i > 0 {
			w.RawString(",{\"mark\":")
		} else {
			w.RawString("{\"mark\":")
		}
		w.Int64(vv.mark)
		w.RawString(",\"visited_at\":")
		w.Int64(vv.visited_at)
		w.RawString(",\"place\":")
		w.String(vv.location.place)
		w.RawString("}")
	}
	w.RawString("]}")

	buf.Write(w.Buffer.BuildBytes())
}

func getAvgJson(avg float64, buf *bytes.Buffer) {
	w := jwriter.Writer{}

	w.RawString("{\"avg\":")
	w.Float64(toFixed(avg, 5))
	w.RawString("}")

	buf.Write(w.Buffer.BuildBytes())
	return
}