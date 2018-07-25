package kernel

// MaxReadAhead configures the kernel's maximum readahead for file handles on this FUSE mount
// (via ConfigureMount) and our corresponding "trailing" buffer.
//
// Our sizingHandle implements Read operations for read-only fsctx.File objects that don't support
// random access or seeking. Generally this requires that the user reading such a file does so
// in-order. However, the kernel attempts to optimize i/o speed by reading ahead into the page cache
// and to do so it can issue concurrent reads for a few blocks ahead of the user's current position.
// We respond to such requests from our trailing buffer.
// TODO: Choose a value more carefully. This value was chosen fairly roughly based on some
// articles/discussion that suggested this was a kernel default.
const MaxReadAhead = 512 * 1024
