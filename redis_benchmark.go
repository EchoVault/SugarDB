package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"text/tabwriter"
	"time"
)

const (
	Host          = "localhost"
	EchoVaultPort = "7480"
	RedisPort     = "6379"
)

type Metrics struct {
	CommandName       string
	RequestsPerSecond string
	P50Latency        string
}

func getCommandArgs() (string, bool) {
	defaultCommands := "ping,set,get,incr,lpush,rpush,lpop,rpop,sadd,hset,zpopmin,lrange,mset"
	commands := flag.String("commands", defaultCommands, "Commands to run")
	useLocal := flag.Bool("use_local_server", false, "Run benchamark using local EchoVault server")
	flag.Parse()
	fmt.Printf("Provided commands: %s\n", *commands)
	if *useLocal {
		fmt.Println("Using local running EchoVault server")
	}
	return *commands, *useLocal
}

func runBenchmark(port string, commands string) ([]Metrics, error) {
	var results []Metrics

	// Run benchmark
	cmd := exec.Command("redis-benchmark", "-h", Host, "-p", port, "-q", "-t", commands)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, err
	}

	// Parse the output
	strOutput := string(output)
	fmt.Println(strOutput)
	lines := strings.Split(strOutput, "\n")
	for _, line := range lines {
		if !strings.HasPrefix(line, "WARNING") && line != "" {
			// Get command name
			colonIndex := strings.Index(line, ":")
			commandName := line[:colonIndex]

			// Get requests per second
			reqSecIndex := strings.Index(line, " requests per second")
			spaceIndex := strings.LastIndex(line[:reqSecIndex], " ")
			requestsPerSecond := line[spaceIndex+1 : reqSecIndex]

			// Get p50 latency
			p50Index := strings.Index(line, "p50=")
			spaceAfterP50 := strings.Index(line[p50Index:], " ")
			p50Latency := line[p50Index+4 : p50Index+spaceAfterP50]

			results = append(results, Metrics{
				CommandName:       commandName,
				RequestsPerSecond: requestsPerSecond,
				P50Latency:        p50Latency,
			})
		}
	}

	return results, nil
}

func createDisplayTable(redisResults []Metrics, echovaultResults []Metrics) {
	// check if the number of commands is the same
	if len(echovaultResults) != len(redisResults) {
		fmt.Println("Error: Number of commands in Redis and EchoVault do not match")
	}

	fmt.Println("Benchmark Performance Results:")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprint(w, "Command\tRedis (req/sec)\tRedis p50 Latency (msec)\tEchovault (req/sec)\tEchoVault p50 Latency (msec)\t\n")
	for i := 0; i < len(redisResults); i++ {
		command := redisResults[i].CommandName
		redisReqSec := redisResults[i].RequestsPerSecond
		redisLatency := redisResults[i].P50Latency
		echovaultReqSec := echovaultResults[i].RequestsPerSecond
		echovaultLatency := echovaultResults[i].P50Latency

		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t\n", command, redisReqSec, redisLatency, echovaultReqSec, echovaultLatency)
	}
	w.Flush()
}

func main() {

	commands, useLocal := getCommandArgs()

	// Start a local Redis server, wait a few seconds for it to start
	exec.Command("redis-server", "--port", RedisPort).Start()
	time.Sleep(2 * time.Second)

	// Run benchmark on local Redis server
	fmt.Println("-------Running Redis Benchmarks------")
	redisResults, err := runBenchmark(RedisPort, commands)
	if err != nil {
		fmt.Println("Error running benchmark on Redis server:", err)
		return
	}

	if !useLocal {
		// run the packaged EchoVault server, wait a few seconds for it to start
		exec.Command("echovault", "--bind-addr=localhost", "--data-dir=persistence").Start()
		time.Sleep(5 * time.Second)
	}

	// Run benchmark on EchoVault server
	fmt.Println("-------Running EchoVault Benchmarks------")
	echovaultResults, err := runBenchmark(EchoVaultPort, commands)
	if err != nil {
		fmt.Println("Error running benchmark on EchoVault server:", err)
		fmt.Println("Check that the EchoVault server is running")
		return
	}

	// Display results in a table format
	createDisplayTable(redisResults, echovaultResults)

	// Kill the local Redis server
	exec.Command("pkill", "-f", "redis-server").Run()

	if !useLocal {
		// run the packaged EchoVault server
		exec.Command("pkill", "-f", "echovault").Run()
		if err := os.RemoveAll("persistence"); err != nil { // remove persistence directory
			fmt.Println("Error removing persistence directory:", err)
		}
	}
}
