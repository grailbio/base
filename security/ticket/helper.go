package ticket

import (
	"fmt"
	"reflect"
	"strings"
	"v.io/v23/context"
)

// An UnexpectedTicketType error is produced when a ticket cannot be cast to the expected type.
type UnexpectedTicketType struct {
	Expected string
	Actual   string
}

func (err UnexpectedTicketType) Error() string {
	return fmt.Sprintf("ticket was a %q, not a %q", err.Actual, err.Expected)
}

func expected(expected interface{}, actual interface{}) UnexpectedTicketType {
	return UnexpectedTicketType{
		Expected: reflect.TypeOf(expected).Name(),
		Actual:   reflect.TypeOf(actual).Name(),
	}
}

// A Getter retrieves a ticket value for the key.
//
// Users of this package should use the default Client.
// This type exists primarily for unit tests which do not rely on the ticket-server.
type Getter func(ctx *context.T, key string) (Ticket, error)

/*
Client is the default Getter which uses Vanadium to interact with the ticket-server.

For example, to get a string value:

  myValue, err := ticket.Client.GetString(ctx, "ticket/path")
*/
var Client Getter = func(ctx *context.T, key string) (Ticket, error) {
	return TicketServiceClient(key).Get(ctx)
}

func (g Getter) getTicket(ctx *context.T, path ...string) (Ticket, error) {
	key := strings.Join(path, "/")
	return g(ctx, key)
}

// GetData for key from the ticket-server. It must be stored in a GenericTicket.
// Path components will be joined with a `/`.
func (g Getter) GetData(ctx *context.T, path ...string) (data []byte, err error) {
	tick, err := g.getTicket(ctx, path...)
	if err != nil {
		return nil, err
	}

	cast, ok := tick.(TicketGenericTicket)
	if !ok {
		return nil, expected(TicketGenericTicket{}, tick)
	}

	return cast.Value.Data, nil
}

// GetString for key from the ticket-server. It must be stored in a GenericTicket.
// Path components will be joined with a `/`.
func (g Getter) GetString(ctx *context.T, path ...string) (value string, err error) {
	data, err := g.GetData(ctx, path...)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

// GetAws credentials and helpers for key from the ticket-server.
// Path components will be joined with a `/`.
func (g Getter) GetAws(ctx *context.T, path ...string) (aws AwsTicket, err error) {
	tick, err := g.getTicket(ctx, path...)
	if err != nil {
		return aws, err
	}

	cast, ok := tick.(TicketAwsTicket)
	if !ok {
		return aws, expected(TicketAwsTicket{}, cast)
	}

	return cast.Value, nil
}

// GetS3 credentials and helpers for key from the ticket-server.
// Path components will be joined with a `/`.
func (g Getter) GetS3(ctx *context.T, path ...string) (S3 S3Ticket, err error) {
	tick, err := g.getTicket(ctx, path...)
	if err != nil {
		return S3, err
	}

	cast, ok := tick.(TicketS3Ticket)
	if !ok {
		return S3, expected(TicketS3Ticket{}, cast)
	}

	return cast.Value, nil
}

// GetSshCertificate for key from the ticket-server.
// Path components will be joined with a `/`.
func (g Getter) GetSshCertificate(ctx *context.T, path ...string) (SshCertificate SshCertificateTicket, err error) {
	tick, err := g.getTicket(ctx, path...)
	if err != nil {
		return SshCertificate, err
	}

	cast, ok := tick.(TicketSshCertificateTicket)
	if !ok {
		return SshCertificate, expected(TicketSshCertificateTicket{}, cast)
	}

	return cast.Value, nil
}

// GetEcr endpoint and helpers for key from the ticket-server.
// Path components will be joined with a `/`.
func (g Getter) GetEcr(ctx *context.T, path ...string) (Ecr EcrTicket, err error) {
	tick, err := g.getTicket(ctx, path...)
	if err != nil {
		return Ecr, err
	}

	cast, ok := tick.(TicketEcrTicket)
	if !ok {
		return Ecr, expected(TicketEcrTicket{}, cast)
	}

	return cast.Value, nil
}

// GetTlsServer credentials and helpers for key from the ticket-server.
// Path components will be joined with a `/`.
func (g Getter) GetTlsServer(ctx *context.T, path ...string) (TlsServer TlsServerTicket, err error) {
	tick, err := g.getTicket(ctx, path...)
	if err != nil {
		return TlsServer, err
	}

	cast, ok := tick.(TicketTlsServerTicket)
	if !ok {
		return TlsServer, expected(TicketTlsServerTicket{}, cast)
	}

	return cast.Value, nil
}

// GetTlsClient credentials and helpers for key from the ticket-server.
// Path components will be joined with a `/`.
func (g Getter) GetTlsClient(ctx *context.T, path ...string) (TlsClient TlsClientTicket, err error) {
	tick, err := g.getTicket(ctx, path...)
	if err != nil {
		return TlsClient, err
	}

	cast, ok := tick.(TicketTlsClientTicket)
	if !ok {
		return TlsClient, expected(TicketTlsClientTicket{}, cast)
	}

	return cast.Value, nil
}

// GetDocker credentials and helpers for key from the ticket-server.
// Path components will be joined with a `/`.
func (g Getter) GetDocker(ctx *context.T, path ...string) (Docker DockerTicket, err error) {
	tick, err := g.getTicket(ctx, path...)
	if err != nil {
		return Docker, err
	}

	cast, ok := tick.(TicketDockerTicket)
	if !ok {
		return Docker, expected(TicketDockerTicket{}, cast)
	}

	return cast.Value, nil
}

// GetDockerServer credentials and helpers for key from the ticket-server.
// Path components will be joined with a `/`.
func (g Getter) GetDockerServer(ctx *context.T, path ...string) (DockerServer DockerServerTicket, err error) {
	tick, err := g.getTicket(ctx, path...)
	if err != nil {
		return DockerServer, err
	}

	cast, ok := tick.(TicketDockerServerTicket)
	if !ok {
		return DockerServer, expected(TicketDockerServerTicket{}, cast)
	}

	return cast.Value, nil
}

// GetDockerClient credentials and helpers for key from the ticket-server.
// Path components will be joined with a `/`.
func (g Getter) GetDockerClient(ctx *context.T, path ...string) (DockerClient DockerClientTicket, err error) {
	tick, err := g.getTicket(ctx, path...)
	if err != nil {
		return DockerClient, err
	}

	cast, ok := tick.(TicketDockerClientTicket)
	if !ok {
		return DockerClient, expected(TicketDockerClientTicket{}, cast)
	}

	return cast.Value, nil
}

// GetB2 credentials and helpers for key from the ticket-server.
// Path components will be joined with a `/`.
func (g Getter) GetB2(ctx *context.T, path ...string) (B2 B2Ticket, err error) {
	tick, err := g.getTicket(ctx, path...)
	if err != nil {
		return B2, err
	}

	cast, ok := tick.(TicketB2Ticket)
	if !ok {
		return B2, expected(TicketB2Ticket{}, cast)
	}

	return cast.Value, nil
}

// GetVanadium blessing and helpers for key from the ticket-server.
// Path components will be joined with a `/`.
func (g Getter) GetVanadium(ctx *context.T, path ...string) (Vanadium VanadiumTicket, err error) {
	tick, err := g.getTicket(ctx, path...)
	if err != nil {
		return Vanadium, err
	}

	cast, ok := tick.(TicketVanadiumTicket)
	if !ok {
		return Vanadium, expected(TicketVanadiumTicket{}, cast)
	}

	return cast.Value, nil
}
