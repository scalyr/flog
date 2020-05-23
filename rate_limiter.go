package main

/**
 * WriteLimiter is a generic interface that controls how many bytes
 * of log lines are written and when to advance the line timestamp.
 *
 * There are two implementions, RateBasedLimiter and FixedDelayLimiter.
 * You generally should not need to work directly with these classes,
 * but just get a reference to the appropriate one using
 * CreateWriteLimiter based on the options.
 *
 * RateBasedLimiter is used to control how many log bytes are written to conform
 * to some sort of desired rate in MB/s.  It will instruct the caller
 * on how many bytes should be written and when (by blocking the caller).
 *
 * The underlying algorithm is based on tracking the last 10 seconds of
 * writes and delaying writes when necessary.
 *
 * Example:
 *
 *    limiter = new(RateBasedLimiter)
 *    limiter.Initialize(5) // MBs 
 *    while bytesToWrite, lineTimestamp = limiter.BlockUntilCanWriteBytes() {
 *      // Write out at least `bytesToWrite` number of bytes.  Use `lineTimestamp` as the time for the log lines.
 *      limiter.ReportBytesWritten(actualNumberOfBytesWritten, actualNumberOfLinesWritten, lineTimestamp)
 *    }
 *
 *
 * FixedDelayLimiter just sleeps a fixed amount of time for every log
 * line generated and artificially advances the clock.
 */

import (
         "math"
         "time"
         "log"
         "github.com/natefinch/lumberjack"
)


type WriteLimiter interface {
  BlockUntilCanWriteBytes() (bytesToWrite int64, lineTimestamp time.Time)
  ReportBytesWritten(actualBytesWritten int64, actualLinesWritten int, lineTimestamp time.Time)
}

func CreateWriteLimiter(mBsRate float64, timeAdvancePerLine time.Duration, sleepPerLine time.Duration, reportTo string) (WriteLimiter) {
  if !math.IsInf(mBsRate, 0) {
    result := new(RateBasedLimiter)
    result.Initialize(mBsRate, reportTo)
    return result
  } else {
    result := new(FixedDelayLimiter)
    result.Initialize(timeAdvancePerLine, sleepPerLine, reportTo)
    return result
  }
}

// Simple clock interface to allow faking out time and sleeping during tests
type Clock interface {
  Now() time.Time
  Sleep(d time.Duration)
}

// The number of samples (i.e., how many bytes where written over a given time interval) to collect per second.
const SamplesPerSec = 100
// The sampling window to use when calculating the current write rate.
const SamplingWindow = time.Duration(10 * time.Second)
// Do not bother to delay writes less than this amount of time.
const MinimumTimeToSleep = time.Duration(20 * time.Millisecond)

const NumSamples = int(SamplingWindow * SamplesPerSec / time.Second)

type RateReporter struct {
  currentSecond time.Time
  totalBytesInSecond int64
  totalLinesInSecond int
}

func (r *RateReporter) Initialize(reportTo string) {
  log.SetOutput(&lumberjack.Logger{
    Filename:   reportTo,
    MaxSize:    10, // megabytes
    MaxBackups: 3,
    MaxAge:     1, //days
    Compress:   false, // disabled by default
  })
  // Prevent logging library from adding its timestamp because we want ton control very carefully the timestamp we use to record the data.
  log.SetFlags(0)
}

func (r *RateReporter) Report(linesWritten int, bytesWritten int64, lineTimestamp time.Time) {
  timestampTruncatedBySecond := lineTimestamp.Truncate(1 * time.Second)

  if r.currentSecond.IsZero() {
    r.currentSecond = timestampTruncatedBySecond
  }

  if r.currentSecond != timestampTruncatedBySecond {
    // We hard code a format here because it is difficult to override at the log library level.
    log.Printf("[%s] lines=%d bytes=%d\n", r.currentSecond.Format("2/Jan/2006:15:04:05 -0700"), r.totalLinesInSecond, r.totalBytesInSecond)
    r.currentSecond = timestampTruncatedBySecond
    r.totalBytesInSecond = bytesWritten
    r.totalLinesInSecond = linesWritten
  } else {
    r.totalBytesInSecond += bytesWritten
    r.totalLinesInSecond += linesWritten
  }
}

// A WriteLimiter that limits based on a desired MB/s write rate
type RateBasedLimiter struct {
  // For the given sample, how many bytes we written
  bytesWritten [NumSamples]int64
  // When where the bytes written (the time when the sample was started to be written).
  timeWritten [NumSamples]time.Time
  // The index of the sample with the earliest time.
  earliestSample int
  // The sum of the bytesWritten.
  totalBytesWritten int64
  // The ideal number of bytes to write per sample to maintain the desired write rate
  targetBytesPerSample int64
  // The report (if not nil) to use to report lines / bytes written
  reporter *RateReporter
  // Used to calculate time and sleep.  Overridden by tests.
  clock Clock
}

type realClock struct{}
func (realClock) Now() time.Time { return time.Now() }
func (realClock) Sleep(d time.Duration) { time.Sleep(d) }

// Initialize an instance which will limit writes to the specified MB per second.
func (r *RateBasedLimiter) Initialize(targetRateMBs float64, reportTo string) {
  r.InitializeWithClock(targetRateMBs, reportTo, new(realClock))
}

func (r *RateBasedLimiter) InitializeWithClock(targetRateMBs float64, reportTo string, clock Clock) {
  r.targetBytesPerSample = int64(targetRateMBs * 1_000_000 / SamplesPerSec)
  r.clock = clock

  now := r.clock.Now()

  timePerSampleNanos := SamplingWindow.Nanoseconds() / int64(NumSamples - 1)
  
  for i := 0; i < NumSamples; i++ {
    r.bytesWritten[i] = r.targetBytesPerSample
    r.timeWritten[i] = now.Add(time.Duration(int64(i - NumSamples) * timePerSampleNanos))
  }

  r.totalBytesWritten = int64(NumSamples) * r.targetBytesPerSample

  if len(reportTo) > 0 {
    r.reporter = new(RateReporter)
    r.reporter.Initialize(reportTo)
  }
}

// Returns the number of bytes that should be written in order to
// conform to the desired write rate.  This method will block if
// necessary to slow down the writer.  This returns both the
// number of bytes that should be written as well as the time
// that should be used when invoking `ReportBytesWritten` to
// record this write.
func (r *RateBasedLimiter) BlockUntilCanWriteBytes() (int64, time.Time) {
  // To calculate how long we should block, we essentially remove the earliest sample and
  // recalculate when we should write the next one to maintain the ideal write rate.

  totalBytesWithoutEarliest := r.totalBytesWritten - r.bytesWritten[r.earliestSample]
  startTimeWithoutEarliest := r.timeWritten[(r.earliestSample + 1) % NumSamples]

  numBytesToWrite := r.targetBytesPerSample

  idealTimeToWriteAllSamples := time.Duration((totalBytesWithoutEarliest + numBytesToWrite) * (SamplingWindow.Nanoseconds() / (r.targetBytesPerSample * int64(NumSamples))))

  idealNextWriteTime := startTimeWithoutEarliest.Add(idealTimeToWriteAllSamples)
  // If the time for the next sample is far enough in the future, sleep to wait for it.
  timeToWait := idealNextWriteTime.Sub(r.clock.Now())
  if timeToWait >= MinimumTimeToSleep {
    r.clock.Sleep(timeToWait)
  }

  return numBytesToWrite, r.clock.Now()
}

// Records that the specified number of bytes was written.  The first
// argument should be the actual number of bytes written and the second
// should be the time returned from `BlockUntilCanWriteBytes`.
func (r *RateBasedLimiter) ReportBytesWritten(bytesWritten int64, linesWritten int, timeWritten time.Time) {
  r.totalBytesWritten += bytesWritten - r.bytesWritten[r.earliestSample]
  r.bytesWritten[r.earliestSample] = bytesWritten
  r.timeWritten[r.earliestSample] = timeWritten
  r.earliestSample = (r.earliestSample + 1) % NumSamples

  if r.reporter != nil {
    r.reporter.Report(linesWritten, bytesWritten, timeWritten)
  }
}

// Only used for tests.
func (r *RateBasedLimiter) OverrideClock(newClock Clock) {
  r.clock = newClock
}

// Only used for tests.  Calculates the actual average MBs based on the last 10 seconds of writes.
func (r *RateBasedLimiter) AverageMBs() float64 {
  mostRecentSample := (r.earliestSample + NumSamples - 1) % NumSamples
  totalTime := r.timeWritten[mostRecentSample].Sub(r.timeWritten[r.earliestSample])
  return float64(r.totalBytesWritten) / (1_000_000 * totalTime.Seconds())
}

// A WriteLimiter that limits a fixed delay
type FixedDelayLimiter struct {
  // The amount to increment the line timestamp for each line.
  timeAdvancePerLine time.Duration
  // The amount to sleep for each line
  sleepPerLine time.Duration
  // The timestamp to use for the next log line
  lineTimestamp time.Time
  // The report (if not nil) to use to report lines / bytes written
  reporter *RateReporter
  // Used to calculate time and sleep.  Overridden by tests.
  clock Clock
}

// Initialize an instance which will limit by fixed amounts
func (r *FixedDelayLimiter) InitializeWithClock(timeAdvancePerLine time.Duration, sleepPerLine time.Duration, reportTo string, clock Clock) {
  r.timeAdvancePerLine = timeAdvancePerLine
  r.sleepPerLine = sleepPerLine
  r.clock = clock
  r.lineTimestamp = clock.Now()
  if len(reportTo) > 0 {
    r.reporter = new(RateReporter)
    r.reporter.Initialize(reportTo)
  }
}

func (r *FixedDelayLimiter) Initialize(timeAdvancePerLine time.Duration, sleepPerLine time.Duration, reportTo string) {
  r.InitializeWithClock(timeAdvancePerLine, sleepPerLine, reportTo, new(realClock))
}

// Returns the number of bytes that should be written in order to
// conform to the desired write rate.  This method will block if
// necessary to slow down the writer.  This returns both the
// number of bytes that should be written as well as the time
// that should be used when invoking `ReportBytesWritten` to
// record this write.
func (r *FixedDelayLimiter) BlockUntilCanWriteBytes() (int64, time.Time) {
  resultTimestamp := r.lineTimestamp
  r.lineTimestamp = r.lineTimestamp.Add(r.timeAdvancePerLine)
  r.clock.Sleep(r.sleepPerLine)
  return 1, resultTimestamp
}

// Records that the specified number of bytes was written.  The first
// argument should be the actual number of bytes written and the second
// should be the time returned from `BlockUntilCanWriteBytes`.
func (r *FixedDelayLimiter) ReportBytesWritten(bytesWritten int64, linesWritten int, timeWritten time.Time) {
  if r.reporter != nil {
    r.reporter.Report(linesWritten, bytesWritten, timeWritten)
  }
}

// Only used for tests.
func (r *FixedDelayLimiter) OverrideClock(newClock Clock) {
  r.clock = newClock
}
