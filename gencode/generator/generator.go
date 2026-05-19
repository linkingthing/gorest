package generator

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"
)

type Config struct {
	Name    string
	OutDir  string
	PkgBase string
	Only    string
}

type templateData struct {
	Names
	PkgBase string
}

type layer struct {
	Name    string // resource / service / api
	DirName string
	Tpl     string
}

var allLayers = []layer{
	{Name: "resource", DirName: "resource", Tpl: resourceTpl},
	{Name: "service", DirName: "service", Tpl: serviceTpl},
	{Name: "api", DirName: "api", Tpl: apiTpl},
}

func Run(cfg Config) error {
	outDir, err := filepath.Abs(cfg.OutDir)
	if err != nil {
		return fmt.Errorf("get path %q: failed: %w", cfg.OutDir, err)
	}

	pkgBase := cfg.PkgBase
	if pkgBase == "" {
		pkgBase, err = detectModulePath(outDir)
		if err != nil {
			return fmt.Errorf("unable to get go.mod, use -pkg to specify a package name: %w", err)
		}
	}

	names := NewNames(cfg.Name)
	data := templateData{
		Names:   names,
		PkgBase: pkgBase,
	}

	fmt.Printf("\nbase package: %s\n", pkgBase)
	fmt.Printf("output dir: %s\n", outDir)
	fmt.Printf("begin to genCode: %s → %s\n\n", names.Raw, names.Pascal)

	generated := 0
	skipped := 0

	for _, l := range allLayers {
		if cfg.Only != "" && cfg.Only != l.Name {
			continue
		}

		outPath, isNew, err := generateFile(outDir, l, data)
		if err != nil {
			return fmt.Errorf("[%s] %w", l.Name, err)
		}

		if isNew {
			fmt.Printf("%-10s %s\n", l.Name, outPath)
			generated++
		} else {
			fmt.Printf("%-10s %s  (dir exists，skiped)\n", l.Name, outPath)
			skipped++
		}
	}

	if generated+skipped == 0 {
		return fmt.Errorf("unknown layer: %q，options: resource / service / api", cfg.Only)
	}

	fmt.Printf("\ncode generate done：new %d，skip %d\n", generated, skipped)
	return nil
}

func generateFile(outDir string, l layer, data templateData) (path string, isNew bool, err error) {
	tpl, err := template.New(l.Name).Parse(l.Tpl)
	if err != nil {
		return "", false, fmt.Errorf("parse template failed: %w", err)
	}

	var buf bytes.Buffer
	if err := tpl.Execute(&buf, data); err != nil {
		return "", false, fmt.Errorf("execute template failed: %w", err)
	}

	dir := filepath.Join(outDir, l.DirName)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", false, fmt.Errorf("create directory failed: %w", err)
	}

	fullPath := filepath.Join(dir, data.Snake+"_"+l.Name+".go")
	if _, err := os.Stat(fullPath); err == nil {
		return fullPath, false, nil
	}

	if err := os.WriteFile(fullPath, buf.Bytes(), 0644); err != nil {
		return "", false, fmt.Errorf("write to file failed: %w", err)
	}

	return fullPath, true, nil
}

func detectModulePath(startDir string) (string, error) {
	dir := startDir
	for {
		gomod := filepath.Join(dir, "go.mod")
		if _, err := os.Stat(gomod); err == nil {
			module, err := readModuleLine(gomod)
			if err != nil {
				return "", err
			}
			return module, nil
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", fmt.Errorf("not found go.mod from %s", startDir)
}

func readModuleLine(gomodPath string) (string, error) {
	f, err := os.Open(gomodPath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "module ") {
			return strings.TrimSpace(strings.TrimPrefix(line, "module")), nil
		}
	}
	return "", fmt.Errorf("not found module int go.mod module: %s", gomodPath)
}
