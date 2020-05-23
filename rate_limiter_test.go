package main

import (
         "testing"
         "time"
         "github.com/stretchr/testify/assert"
)

type TestClock struct{
  currentTime time.Time
  totalSleepTime time.Duration
}

func (c *TestClock) Now() time.Time {
  return c.currentTime
}

func (c *TestClock) SetTime(newTime time.Time) {
  c.currentTime = newTime
}

func (c *TestClock) Sleep(d time.Duration) {
  c.currentTime = c.currentTime.Add(d)
  c.totalSleepTime = c.totalSleepTime + d
}

func (c *TestClock) Advance(d time.Duration) {
  c.currentTime = c.currentTime.Add(d)
}

func CreateTestRateBasedLimiter() (*RateBasedLimiter, *TestClock) {
  testClock := new(TestClock)
  testClock.SetTime(time.Unix(1_000_000, 0))

  r := new(RateBasedLimiter)
  r.OverrideClock(testClock)

  return r, testClock
}

func CreateTestFixedDelayLimiter() (*FixedDelayLimiter, *TestClock) {
  testClock := new(TestClock)
  testClock.SetTime(time.Unix(1_000_000, 0))

  r := new(FixedDelayLimiter)
  r.OverrideClock(testClock)

  return r, testClock
}

func TestNormalFunction(t *testing.T) {
  a := assert.New(t)

  r, testClock := CreateTestRateBasedLimiter()

  // Write limit at 1MB/s
  r.InitializeWithClock(1, "", testClock)
  a.InDelta(1.0, r.AverageMBs(), 0.0001, "Verify initial population generated correct write rate")
  bytesToWrite, writeTime := r.BlockUntilCanWriteBytes()
  a.Equal(int64(10_000), bytesToWrite, "Verify correct write size")
  a.Equal(int64(0), testClock.totalSleepTime.Milliseconds(), "Should not need to sleep for first write")

  r.ReportBytesWritten(bytesToWrite, 5, writeTime)

  a.InDelta(1.0, r.AverageMBs(), 0.0001, "Verify correct write rate after first real write")
  bytesToWrite, writeTime = r.BlockUntilCanWriteBytes()
  a.Equal(int64(10_000), bytesToWrite, "Verify correct write size")
  a.Equal(int64(0), testClock.totalSleepTime.Milliseconds(), "Verify did not sleep even though ideally we would have waited")

  r.ReportBytesWritten(bytesToWrite, 5, writeTime)

  a.InDelta(1.001, r.AverageMBs(), 0.0001, "Verify still correct write rate")
  bytesToWrite, writeTime = r.BlockUntilCanWriteBytes()
  a.Equal(int64(20), testClock.totalSleepTime.Milliseconds(), "Verify did sleep for 20 millis")
}

func TestWrittenMoreThanRequested(t *testing.T) {
  a := assert.New(t)

  r, testClock := CreateTestRateBasedLimiter()

  // Write limit at 1MB/s
  r.InitializeWithClock(1, "", testClock)

  for i := 0; i < 50_000; i++ {
    a.InDelta(1.0, r.AverageMBs(), 0.05, "Verify initial population generated correct write rate")
    bytesToWrite, writeTime := r.BlockUntilCanWriteBytes()
    r.ReportBytesWritten(int64(1.1 * float64(bytesToWrite)), 5, writeTime)
  }
}

func TestWrittenLessThanRequested(t *testing.T) {
  a := assert.New(t)

  r, testClock := CreateTestRateBasedLimiter()

  // Write limit at 1MB/s
  r.InitializeWithClock(1, "", testClock)

  for i := 0; i < 50_000; i++ {
    a.InDelta(1.0, r.AverageMBs(), 0.05, "Verify initial population generated correct write rate")
    bytesToWrite, writeTime := r.BlockUntilCanWriteBytes()
    r.ReportBytesWritten(int64(0.9 * float64(bytesToWrite)), 5, writeTime)
  }
}

func TestFixedDelayLimiter(t *testing.T) {
  a := assert.New(t)

  r, testClock := CreateTestFixedDelayLimiter()

  timeAtInitialization := testClock.Now()

  r.InitializeWithClock(time.Duration(10 * time.Second), time.Duration(5 * time.Second), "", testClock)

  bytesToWrite, writeTime := r.BlockUntilCanWriteBytes()
  a.Equal(timeAtInitialization, writeTime, "Initial line should be the same as time when limiter created")
  a.Equal(float64(5), testClock.totalSleepTime.Seconds(), "Verify slept for 5 seconds")

  r.ReportBytesWritten(bytesToWrite, 5, writeTime)

  bytesToWrite, writeTime = r.BlockUntilCanWriteBytes()
  a.Equal(timeAtInitialization.Add(time.Duration(10 * time.Second)), writeTime, "Line timestamp has been incremented")
  a.Equal(float64(10), testClock.totalSleepTime.Seconds(), "Verify slept for 5 more seconds")
}
