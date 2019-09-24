package index

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
)

const kPrefixOK = "OK "
const kPrefixERR = "ERR "

var ErrTxDone = errors.New("transaction already closed")
var ErrTxActive = errors.New("another transaction is still active")

func EncodeFingerprint(hashes []uint32) string {
	var b strings.Builder
	for i, hash := range hashes {
		if i > 0 {
			b.WriteRune(',')
		}
		b.WriteString(strconv.FormatUint(uint64(hash), 10))
	}
	return b.String()
}

type IndexClient struct {
	conn        net.Conn
	buf         *bufio.ReadWriter
	closed      bool
	hasError    bool
	hasDeadline bool
	tx          *IndexClientTx
}

type FingerprintInfo struct {
	ID     uint32
	Hashes []uint32
}

func NewIndexClient(conn net.Conn) *IndexClient {
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	buf := bufio.NewReadWriter(reader, writer)
	return &IndexClient{conn: conn, buf: buf}
}

func Connect(host string, port int) (*IndexClient, error) {
	conn, err := net.Dial("tcp", net.JoinHostPort(host, strconv.Itoa(port)))
	if err != nil {
		return nil, err
	}
	return NewIndexClient(conn), nil
}

func ConnectWithConfig(cfg *IndexConfig) (*IndexClient, error) {
	return Connect(cfg.Host, cfg.Port)
}

func (c *IndexClient) IsOK() bool {
	return !c.closed && !c.hasError
}

func (c *IndexClient) Close() error {
	if c.closed {
		return nil
	}

	if c.tx != nil {
		err := c.tx.Rollback(context.Background())
		if err != nil {
			return err
		}
	}

	err := c.conn.Close()
	if err == nil {
		c.closed = true
	}
	return err
}

func WriteLine(writer *bufio.Writer, line string) error {
	_, err := writer.WriteString(line + "\r\n")
	if err != nil {
		return err
	}
	err = writer.Flush()
	if err != nil {
		return err
	}
	return nil
}

func ReadLine(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}

func (c *IndexClient) sendRequest(ctx context.Context, request string) (string, error) {
	deadline, hasDeadline := ctx.Deadline()
	if hasDeadline || c.hasDeadline {
		c.conn.SetWriteDeadline(deadline)
		c.conn.SetReadDeadline(deadline)
		c.hasDeadline = hasDeadline
	}

	err := ctx.Err()
	if err != nil {
		return "", err
	}

	WriteLine(c.buf.Writer, request)

	err = ctx.Err()
	if err != nil {
		return "", err
	}

	response, err := ReadLine(c.buf.Reader)
	if err != nil {
		return "", err
	}

	if strings.HasPrefix(response, kPrefixOK) {
		response = strings.TrimPrefix(response, kPrefixOK)
		return response, nil
	}

	if strings.HasPrefix(response, kPrefixERR) {
		response = strings.TrimPrefix(response, kPrefixERR)
		return "", errors.New(response)
	}

	return "", fmt.Errorf("Invalid response: %v", response)
}

func (c *IndexClient) Ping(ctx context.Context) error {
	_, err := c.sendRequest(ctx, "echo")
	return err
}

func (c *IndexClient) GetLastFingerprintID(ctx context.Context) (uint32, error) {
	strValue, err := c.GetAttribute(ctx, "max_document_id")
	if err != nil {
		return 0, err
	}
	if strValue == "" {
		return 0, nil
	}
	value, err := strconv.ParseUint(strValue, 10, 32)
	if err != nil {
		return 0, err
	}
	return uint32(value), nil
}

func (c *IndexClient) GetAttribute(ctx context.Context, name string) (string, error) {
	return c.sendRequest(ctx, fmt.Sprintf("get attribute %s", name))
}

func (c *IndexClient) SetAttribute(ctx context.Context, name string, value string) error {
	_, err := c.sendRequest(ctx, fmt.Sprintf("set attribute %s %s", name, value))
	return err
}

func (c *IndexClient) BeginTx(ctx context.Context) (*IndexClientTx, error) {
	if c.tx != nil {
		return nil, ErrTxActive
	}
	tx := &IndexClientTx{c: c}
	err := tx.begin(ctx)
	if err != nil {
		return nil, err
	}
	return tx, nil
}

func (c *IndexClient) Insert(ctx context.Context, fingerprints []FingerprintInfo) error {
	tx, err := c.BeginTx(ctx)
	if err != nil {
		return err
	}

	for _, fingerprint := range fingerprints {
		err = tx.Insert(ctx, fingerprint.ID, fingerprint.Hashes)
		if err != nil {
			tx.Rollback(ctx)
			return err
		}
	}

	return tx.Commit(ctx)
}

type IndexClientTx struct {
	c    *IndexClient
	done bool
}

func (tx *IndexClientTx) Insert(ctx context.Context, id uint32, hashes []uint32) error {
	if tx.done {
		return ErrTxDone
	}
	err := ctx.Err()
	if err != nil {
		return err
	}
	_, err = tx.c.sendRequest(ctx, fmt.Sprintf("insert %d %s", id, EncodeFingerprint(hashes)))
	return err
}

func (tx *IndexClientTx) begin(ctx context.Context) error {
	err := ctx.Err()
	if err != nil {
		return err
	}
	_, err = tx.c.sendRequest(ctx, "begin")
	return err
}

func (tx *IndexClientTx) Commit(ctx context.Context) error {
	if tx.done {
		return ErrTxDone
	}
	err := ctx.Err()
	if err != nil {
		return err
	}
	_, err = tx.c.sendRequest(ctx, "commit")
	if err == nil {
		tx.done = true
		tx.c.tx = nil
	}
	return err
}

func (tx *IndexClientTx) Rollback(ctx context.Context) error {
	if tx.done {
		return ErrTxDone
	}
	err := ctx.Err()
	if err != nil {
		return err
	}
	_, err = tx.c.sendRequest(ctx, "rollback")
	if err == nil {
		tx.done = true
		tx.c.tx = nil
	}
	return err
}