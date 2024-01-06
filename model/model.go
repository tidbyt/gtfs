package model

import (
	"strconv"
	"time"
)

// Holds all external facing types and constants.

type LocationType int

const (
	LocationTypeStop LocationType = iota
	LocationTypeStation
	LocationTypeEntranceExit
	LocationTypeGenericNode
	LocationTypeBoardingArea
)

type RouteType int

const (
	RouteTypeTram       RouteType = 0
	RouteTypeSubway               = 1
	RouteTypeRail                 = 2
	RouteTypeBus                  = 3
	RouteTypeFerry                = 4
	RouteTypeCable                = 5
	RouteTypeAerial               = 6
	RouteTypeFunicular            = 7
	RouteTypeTrolleybus           = 11
	RouteTypeMonorail             = 12
)

type Agency struct {
	ID       string
	Name     string
	URL      string
	Timezone string
}

type Calendar struct {
	ServiceID string
	StartDate string
	EndDate   string
	Weekday   int8
}

type CalendarDate struct {
	ServiceID     string
	Date          string
	ExceptionType int8
	// TODO: Enum for exception types?
}

type Stop struct {
	ID            string
	Code          string
	Name          string
	Desc          string
	Lat           float64
	Lon           float64
	URL           string
	LocationType  LocationType
	ParentStation string
	PlatformCode  string
}

type Trip struct {
	ID          string
	RouteID     string
	ServiceID   string
	Headsign    string
	ShortName   string
	DirectionID int8
}

type Route struct {
	ID        string
	AgencyID  string
	ShortName string
	LongName  string
	Desc      string
	Type      RouteType
	URL       string
	Color     string
	TextColor string
}

type StopTime struct {
	TripID       string
	StopID       string
	Headsign     string
	StopSequence uint32
	Arrival      string
	Departure    string
}

func (st *StopTime) ArrivalTime() time.Duration {
	h, _ := strconv.Atoi(st.Arrival[0:2])
	m, _ := strconv.Atoi(st.Arrival[2:4])
	s, _ := strconv.Atoi(st.Arrival[4:6])
	return time.Duration(h)*time.Hour + time.Duration(m)*time.Minute + time.Duration(s)*time.Second
}

func (st *StopTime) DepartureTime() time.Duration {
	h, _ := strconv.Atoi(st.Departure[0:2])
	m, _ := strconv.Atoi(st.Departure[2:4])
	s, _ := strconv.Atoi(st.Departure[4:6])
	return time.Duration(h)*time.Hour + time.Duration(m)*time.Minute + time.Duration(s)*time.Second
}

// Holds all Headsigns for trips passing through a stop, for a given
// route and direction.
type RouteDirection struct {
	StopID      string
	RouteID     string
	DirectionID int8
	Headsigns   []string
}

// A vehicle departing from a stop.
type Departure struct {
	StopID       string
	RouteID      string
	TripID       string
	StopSequence uint32
	DirectionID  int8
	Time         time.Time
	Headsign     string
	Delay        time.Duration
}
