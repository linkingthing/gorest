package main

import (
	"fmt"
	"os"

	"github.com/linkingthing/gorest/gencode/generator"
)

const usage = `gencode — A gorest code generator

Usage:
  gencode <name> [options]

parameter:
  name            resource name, support（user_order）or（UserOrder）

options:
  -f <path>       output directory（default: current working directory）
                  this will create sub folder: resource/ service/ api/
  -pkg <module>   package name（default: read from go.mod module）
  -only <layer>   specify layer: resource / service / api
  -h              for help

examples:
  gencode user -f .
  gencode user_order -f ./internal/app
  gencode UserOrder  -f ./internal -pkg github.com/myorg/myapp
  gencode order_item -f . -only service
`

func main() {
	args := os.Args[1:]

	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" {
		fmt.Print(usage)
		os.Exit(0)
	}

	if args[0][0] == '-' {
		fmt.Fprintln(os.Stderr, "invalid parameter,valid parameter is: gencode user_order -f .")
		fmt.Print(usage)
		os.Exit(1)
	}

	name := args[0]
	rest := args[1:]

	cfg := generator.Config{
		Name:   name,
		OutDir: ".",
	}

	for i := 0; i < len(rest); i++ {
		switch rest[i] {
		case "-f":
			i++
			if i >= len(rest) {
				fatalf("-f needs one parameter")
			}
			cfg.OutDir = rest[i]
		case "-pkg":
			i++
			if i >= len(rest) {
				fatalf("-pkg needs one parameter")
			}
			cfg.PkgBase = rest[i]
		case "-only":
			i++
			if i >= len(rest) {
				fatalf("-only need specify with (resource/service/api)")
			}
			cfg.Only = rest[i]
		case "-h", "--help":
			fmt.Print(usage)
			os.Exit(0)
		default:
			fatalf("unknown parameter: %s", rest[i])
		}
	}

	if err := generator.Run(cfg); err != nil {
		fatalf("gen code failed: %v", err)
	}
}

func fatalf(format string, a ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", a...)
	os.Exit(1)
}
