package cmdutil

import "net"

// NetworkAddressFlag represents a network address in host:port format.
type NetworkAddressFlag struct {
	Address   string
	Host      string
	Port      string
	Specified bool
}

// Set implements flag.Value.Set
func (na *NetworkAddressFlag) Set(v string) error {
	host, port, err := net.SplitHostPort(v)
	if err != nil {
		na.Host = v
		na.Port = "0"
	} else {
		na.Host = host
		na.Port = port
	}
	na.Address = v
	na.Specified = true
	return nil
}

// String implements flag.Value.String
func (na *NetworkAddressFlag) String() string {
	return na.Address
}

// Get implements flag.Value.Get
func (na *NetworkAddressFlag) Get() interface{} {
	return na.String()
}
