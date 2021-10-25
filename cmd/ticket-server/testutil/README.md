# Ticket-Server/TestUtil

The following functions and future functionality appeared to be needed across multiple packages and in multiple tests and relevant to v23 functionality. I opted to create a smaller package for them to be imported in a slim, DRY fashion.

The general use of these functions is to mock and test ticket-server functionality when verifying blessings and other v23 artifacts.

## func RunBlesserServer

Creates a generic ticket server to use in a test with all permissions.

# func RunAndCapture

Runs a command and captures the output, somewhat specifically designed for running and capturing grail-access output against a mock ticket server.