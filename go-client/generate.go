//go:build ignore
// +build ignore

package main

import (
	"io"
	"log"
	"os/exec"
)

func main() {
	args := []string{
		"--go_out=.", "--go_opt=module=github.com/deephaven/deephaven-core/go-client",
		"--go-grpc_out=.", "--go-grpc_opt=module=github.com/deephaven/deephaven-core/go-client",
		"--experimental_allow_proto3_optional",
		"-I../proto/proto-backplane-grpc/src/main/proto",
		"deephaven/proto/application.proto",
		"deephaven/proto/console.proto",
		"deephaven/proto/inputtable.proto",
		"deephaven/proto/object.proto",
		"deephaven/proto/session.proto",
		"deephaven/proto/table.proto",
		"deephaven/proto/ticket.proto",
	}

	cmd := exec.Command("protoc", args...)

	p, err := cmd.StderrPipe()
	if err != nil {
		log.Fatal("Couldn't get stderr", err.Error())
	}

	if err = cmd.Start(); err != nil {
		log.Fatal(err)
	}

	out, _ := io.ReadAll(p)

	if err := cmd.Wait(); err != nil {
		log.Printf("%s\n", out)

		log.Fatal("Error when running protoc:", err.Error())
	}
}
