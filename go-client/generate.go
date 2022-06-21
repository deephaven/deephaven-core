//go:generate go run generate.go
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
	}

	files := []string{
		"application",
		"inputtable",
		"object",
		"session",
		"table",
		"ticket",
		"inputtable",
	}

	for _, file := range files {
		fileName := "deephaven/proto/" + file + ".proto"
		modPathPair := fileName + "=github.com/deephaven/deephaven-core/go-client/internal/proto/" + file
		goOpt := "--go_opt=M" + modPathPair
		goGrpcOpt := "--go-grpc_opt=M" + modPathPair

		args = append(args, fileName, goOpt, goGrpcOpt)
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
