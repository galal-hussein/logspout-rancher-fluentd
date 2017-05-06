package fluentdRancher

import (
	"encoding/json"
	"errors"
	"log"
	"net"
	"strings"
	"time"

	"github.com/gliderlabs/logspout/router"
)

// FluentdAdapter is an adapter for streaming JSON to a fluentd collector.
type FluentdAdapter struct {
	conn  net.Conn
	route *router.Route
}

var infraStackImages map[string][]string

func init() {
	router.AdapterFactories.Register(NewFluentdAdapter, "fluentd-rancher")
	infraStackImages = make(map[string][]string)
	infraStackImages["healthcheck"] = append(infraStackImages["healtcheck"], "rancher/healthcheck")
	infraStackImages["scheduler"] = append(infraStackImages["healtcheck"], "rancher/scheduler")
	infraStackImages["network"] = append(infraStackImages["healtcheck"], "rancher/network-manager")
	infraStackImages["network"] = append(infraStackImages["healtcheck"], "rancher/metadata")
	infraStackImages["network"] = append(infraStackImages["healtcheck"], "rancher/dns")
	infraStackImages["ipsec"] = append(infraStackImages["healtcheck"], "rancher/net")

}

// Stream handles a stream of messages from Logspout. Implements router.logAdapter.
func (adapter *FluentdAdapter) Stream(logstream chan *router.Message) {
	for message := range logstream {
		timestamp := int32(time.Now().Unix())
		tag := getInfraTag(message)
		if len(tag) == 0 {
			continue
		}
		record := make(map[string]string)
		record["log"] = message.Data
		record["containerID"] = message.Container.ID
		record["containerName"] = message.Container.Name

		data := []interface{}{tag, timestamp, record}

		json, err := json.Marshal(data)
		if err != nil {
			log.Println("fluentd-adapter: ", err)
			continue
		}

		_, err = adapter.conn.Write(json)
		if err != nil {
			log.Println("fluentd-adapter: ", err)
			continue
		}
	}
}

// NewFluentdAdapter creates a Logspout fluentd adapter instance.
func NewFluentdAdapter(route *router.Route) (router.LogAdapter, error) {
	transport, found := router.AdapterTransports.Lookup(route.AdapterTransport("tcp"))

	if !found {
		return nil, errors.New("unable to find adapter: " + route.Adapter)
	}

	conn, err := transport.Dial(route.Address, route.Options)
	if err != nil {
		return nil, err
	}

	return &FluentdAdapter{
		conn:  conn,
		route: route,
	}, nil
}

func getInfraTag(m *router.Message) string {
	containerImage := strings.Split(m.Container.Image, ":")[0]
	for k, v := range infraStackImages {
		for _, image := range v {
			if image == containerImage {
				return k
			}
		}
	}
	return ""
}
