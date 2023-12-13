package main

import (
	// "errors"
    "bufio"
    "bytes"
	"flag"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"whisper/whisper"
	"github.com/marpaia/graphite-golang"
)

type stats struct {
	startTime    time.Time// duration := time.Since(startTime)
	completed    int64
	failed       int64
	skipped      int64
    totalMetrics int64
	ratePerMin   float64   // number of metrics copied per minute
	workersDone  int64
	workers      int64
}

type metricInfo struct {
	name string
	filepath string
}

type sentMetricNames struct {
	sent []string
	lock            *sync.Mutex
}

type rateLimiter struct {
	pointsPerSecond int64
	currentPoints   int64
	full            chan bool
	lock            *sync.Mutex
	enabled         bool
}

func newRateLimiter(pointsPerSecond int64) *rateLimiter {
	rl := new(rateLimiter)
	rl.pointsPerSecond = pointsPerSecond
	rl.currentPoints = 0
	rl.full = make(chan bool)
	rl.lock = new(sync.Mutex)
	if pointsPerSecond == 0 {
		rl.enabled = false
	} else {
		rl.enabled = true
		go func() {
			for {
				time.Sleep(1 * time.Second)
				select {
				case <-rl.full:
				default:
				}
			}
		}()
		return rl
	}
	return rl
}


func (rl *rateLimiter) limit(n int64) {
	if !rl.enabled {
		return
	}
	rl.lock.Lock()
	defer rl.lock.Unlock()

	rl.currentPoints += n
	if rl.currentPoints >= rl.pointsPerSecond {
		rl.full <- true
		rl.currentPoints = 0
	}
}

func sendWhisperData(
	metric *metricInfo,
	graphiteConn *graphite.Graphite,
	fromTs int,
	toTs int,
	connectRetries int,
	rateLimiter *rateLimiter,
) error {
	whisperData, err := whisper.Open(metric.filepath)
	if err != nil {
		return err
	}

	archiveDataPoints, err := whisperData.DumpDataPoints()
	if err != nil {
		return err
	}
	metrics := make([]graphite.Metric, 0, 1000)
	for _, dataPoint := range archiveDataPoints {
		interval, value := dataPoint.Point()

		if math.IsNaN(value) || interval < fromTs || interval > toTs {
			continue
		}

		v := strconv.FormatFloat(value, 'f', 20, 64)
		metrics = append(metrics, graphite.NewMetric(metric.name, v, int64(interval)))

	}
	rateLimiter.limit(int64(len(metrics)))
	for r := 1; r <= connectRetries; r++ {
		err = graphiteConn.SendMetrics(metrics)
		if err != nil && r != connectRetries {
			// Trying to reconnect to graphite with given parameters
			sleep := time.Duration(math.Pow(2, float64(r))) * time.Second
			log.Printf("Failed to send metric %v to graphite: %v", metric.name, err.Error())
			log.Printf("Trying to reconnect and send metric again %v times", connectRetries-r)
			log.Printf("Sleeping for %v", sleep)
			time.Sleep(sleep)
			graphiteConn.Connect()
		} else {
			break
		}
	}
	if err != nil {
		log.Printf("Failed to send metric %v after %v retries", metric.name, connectRetries)
		return err
	}
	return err
}


func findWhisperFiles(ch chan metricInfo,
	quit chan int,
	directory string,
	metricFile string,
	s *stats,
	sentMetrics map[string]bool,
) {

	metricNames, err := os.Open(metricFile)
	if err != nil {
		log.Fatal(err)
	    close(quit)
	}
    defer metricNames.Close()

	scanner := bufio.NewScanner(metricNames)
	for scanner.Scan() {
		line := scanner.Text()
		if _, ok := sentMetrics[line]; ok {
			//log.Printf("Skipping %s. Already processed\n", line)
			atomic.AddInt64(&s.skipped, 1)
		} else {
			whisperFile := strings.ReplaceAll(line, ".", "/")
			whisperFile = filepath.Join(directory, whisperFile)
			whisperFile += ".wsp"
			// log.Printf("Sending metric %s to worker.\n", line)
			ch <- metricInfo{name: line, filepath: whisperFile}
		}
	}
}

func worker(ch chan metricInfo,
	quit chan int,
	wg *sync.WaitGroup,
	baseDirectory string,
	graphiteHost string,
	graphitePort int,
	graphiteProtocol string,
	fromTs int,
	toTs int,
	connectRetries int,
	rateLimiter *rateLimiter,
	s *stats,
    sentMetric *sentMetricNames,
) {
	defer wg.Done()

	graphiteConn, err := graphite.GraphiteFactory(graphiteProtocol, graphiteHost, graphitePort, "")
	sentDone := false
	if err != nil {
		log.Printf("Failed to connect to graphite host with error: %v", err.Error())
		return
	}

	atomic.AddInt64(&s.workers, 1)
	tickerDone := time.NewTicker(15 * time.Second)
	for {
		select {
		case metric := <-ch:
			{

				err := sendWhisperData(&metric, graphiteConn, fromTs, toTs, connectRetries, rateLimiter)
				if err != nil {
					atomic.AddInt64(&s.failed, 1)
					log.Println(err)
				} else {
                    sentMetric.lock.Lock()
                    sentMetric.sent = append(sentMetric.sent, metric.name)
                    sentMetric.lock.Unlock()
					//log.Println("OK: " + metric.name)
				}
				atomic.AddInt64(&s.completed, 1)
			}
		case <- tickerDone.C:
			if (s.skipped + s.completed) == s.totalMetrics && !sentDone {
				sentDone = true
				atomic.AddInt64(&s.workersDone, 1)
			}
		case <-quit:
			return
		}

	}
}

func dumpSentToFile(f io.Writer, sentMetric *sentMetricNames) {

	sentMetric.lock.Lock()
	defer sentMetric.lock.Unlock()
	datawriter := bufio.NewWriter(f)
	for _, metric := range sentMetric.sent {
		if metric != "" {
			if _, err := datawriter.WriteString(metric + "\n"); err != nil {
				log.Printf("Unable to write complete metrics to file: %v\n", err)
			}
		}
	}
	sentMetric.sent = make([]string, 10)
	datawriter.Flush()
}

func showProgress(s *stats) {
	duration := time.Since(s.startTime)
	completed := s.completed + s.skipped
	percDone := (float64(completed) / float64(s.totalMetrics)) * 100
	log.Printf("Workers: [%d], Total Metrics: [%d], Progress: [%d/%d (%.2f)], Skipped: [%d], failed:[%d], CopyPerMin: [%.2f], Elapsed: [%s]\n",
			   s.workers, s.totalMetrics, s.completed, s.totalMetrics - s.skipped, percDone, s.skipped, s.failed, s.ratePerMin, duration)
}

func houseKeeping(quit chan int,
    quitWorkers chan int,
	totalWorkers int,
	sentMetricFile string,
	s *stats,
	sentMetric *sentMetricNames,
) {
	tickerInfo := time.NewTicker(5 * time.Second)
	tickerDumptoFile := time.NewTicker(60 * time.Second)
	f, err := os.OpenFile(sentMetricFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Unable to open %s for writing. Unable to resume on failure: %v\n", sentMetricFile, err)
	    close(quitWorkers)
		return
	}
	defer f.Close()
	for {
		select {
			case <- tickerInfo.C:
				duration := time.Since(s.startTime)
				if durationMins := duration.Minutes(); durationMins >= 1 {
					s.ratePerMin = float64(s.completed) / durationMins
				}
				showProgress(s)
			case <- tickerDumptoFile.C:
				dumpSentToFile(f, sentMetric)
				if (s.workersDone == s.workers) {
					log.Println("Work done..")
					close(quitWorkers)
				}
			case <- quit:
				dumpSentToFile(f, sentMetric)
				return
		}
	}
}

func lineCounter(metricFile string) (int, error) {

    buf := make([]byte, 32*1024)
    count := 0
    lineSep := []byte{'\n'}
	metricNames, err := os.Open(metricFile)
	if err != nil {
		log.Fatal(err)
		return count, err
	} else {
		defer metricNames.Close()

		for {
			c, err := metricNames.Read(buf)
			count += bytes.Count(buf[:c], lineSep)

			switch {
			case err == io.EOF:
				return count, nil

			case err != nil:
				return count, err
			}
		}
	}
}


func loadCompletedMetrics(completedFilePath string) (map[string]bool, error) {
	metricNames, err := os.Open(completedFilePath)
    completedMetrics := make(map[string]bool)
	if err != nil {
		log.Printf("Unable to load metrics that have been sent from from %s.: %v\n", completedFilePath, err)
		return completedMetrics, err
	} else {
		defer metricNames.Close()
		scanner := bufio.NewScanner(metricNames)
		for scanner.Scan() {
			line := scanner.Text()
			completedMetrics[line] = true  // risks using lots of memory
		}
		return completedMetrics, nil
	}
}

func main() {

	baseDirectory := flag.String(
		"basedirectory",
		"/var/lib/graphite/whisper",
		"Base directory where whisper files are located. Used to retrieve the metric name from the filename.")
	metricFile := flag.String(
		"metricfile",
		"/tmp/metrics.txt",
		"A file containing metrics in the basedirectory that should be copied over. e.g. host.cpu.cpu-0.cpu-idle")
	completedMetricFile := flag.String(
		"completedmetricfile",
		"/tmp/completed-metrics.txt",
		"A file used to track metrics that have been sent. Useful when you need to resume on failure.")
	graphiteHost := flag.String(
		"host",
		"127.0.0.1",
		"Hostname/IP of the graphite server")
	graphitePort := flag.Int(
		"port",
		2003,
		"graphite Port")
	graphiteProtocol := flag.String(
		"protocol",
		"tcp",
		"Protocol to use to transfer graphite data (tcp/udp/nop)")
	workers := flag.Int(
		"workers",
		5,
		"Workers to run in parallel")
	fromTs := flag.Int(
		"from",
		0,
		"Starting timestamp to dump data from")
	toTs := flag.Int(
		"to",
		2147483647,
		"Ending timestamp to dump data up to")
	pointsPerSecond := flag.Int64(
		"pps",
		0,
		"Number of maximum points per second to send (0 means rate limiter is disabled)")
	connectRetries := flag.Int(
		"retries",
		3,
		"How many connection retries worker will make before failure. It is progressive and each next pause will be equal to 'retry * 1s'")
	flag.Parse()

	if !(*graphiteProtocol == "tcp" ||
		*graphiteProtocol == "udp" ||
		*graphiteProtocol == "nop") {
		log.Fatalln("Graphite protocol " + *graphiteProtocol + " not supported, use tcp/udp/nop.")
	}
	ch := make(chan metricInfo)
	quit := make(chan int)
	quitHouseKeeping := make(chan int)
	sentMetrics := new(sentMetricNames)
	sentMetrics.lock = new(sync.Mutex)
	sentMetrics.sent = make([]string, 10)
	var wg sync.WaitGroup
	s := new(stats)
    s.startTime = time.Now()
	s.workersDone = 0
	totalLines, err := lineCounter(*metricFile)
	if err != nil {
		log.Fatalf("Unable to get read number of metric from %s:  %v\n", metricFile, err)
	} else if totalLines <= 0 {
	    log.Printf("File %s contains %d metrics. Nothing to do here!\n", metricFile, totalLines)
	} else {
		s.totalMetrics = int64(totalLines)
        completedMetrics, _ := loadCompletedMetrics(*completedMetricFile)
		rl := newRateLimiter(*pointsPerSecond)
		wg.Add(*workers)
		for i := 0; i < *workers; i++ {
			go worker(ch, quit, &wg, *baseDirectory, *graphiteHost, *graphitePort, *graphiteProtocol, *fromTs, *toTs, *connectRetries, rl, s, sentMetrics)
		}
		go findWhisperFiles(ch, quit, *baseDirectory, *metricFile, s, completedMetrics)
		go houseKeeping(quitHouseKeeping, quit, *workers,  *completedMetricFile, s, sentMetrics)
		wg.Wait()
		close(quitHouseKeeping)
		log.Println("Done......")
		showProgress(s)
	}
}
