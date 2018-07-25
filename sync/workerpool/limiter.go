package workerpool

// Limiter implements a counting semaphore based on a channel. Acquire is
// implemented by putting an integer in the channel and blocking if it
// is full; Release is implemented by removing an integer from the channel.
type Limiter chan int

// NewLimiter creates a new semaphore with a given capacity.
func NewLimiter(count int) Limiter {
	return make(Limiter, count)
}

// Acquire adds an integer to the channel, blocking if it is full.
func (s Limiter) Acquire() {
	s <- 1
}

// Release removes an integer from the channel.
func (s Limiter) Release() {
	<-s
}
