package test

import (
	"os"
)

const nodeHostEnvName = "CLIENT_TEST_NODE_HOST"
const nodePortEnvName = "CLIENT_TEST_NODE_PORT"

func getNodeAddress() (string, string) {
	host := os.Getenv(nodeHostEnvName)
	if host == "" {
		host = "localhost"
	}

	port := os.Getenv(nodePortEnvName)
	if port == "" {
		return host, "5299"
	}
	return host, port
}
