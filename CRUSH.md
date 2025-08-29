CRUSH.md — Project playbook for agents

Project type
- Language: Go (module github.com/marcpaquette/crdbpool-tester)
- Entry point: main.go (small CLI/test harness)
- No existing Cursor/Copilot rules found (no .cursor/rules, .cursorrules, or .github/copilot-instructions.md)

Build/run
- Ensure Go ≥ 1.22 is installed (module file declares 1.25; use latest stable if available)
- Install deps: go mod tidy
- Build binary: go build ./...
- Run program: go run .
- Environment: set CRDB_CONN_STRING before running if needed

Tests
- No test files currently present. Add *_test.go files to enable tests.
- Run all tests: go test ./...
- Run a single package: go test ./path/to/pkg
- Run a single test: go test -run ^TestName$ ./path/to/pkg
- With race detector: go test -race ./...

Lint/format/typecheck
- Formatting: gofmt -w . (or go fmt ./...)
- Imports: goimports -w . (install: go install golang.org/x/tools/cmd/goimports@latest)
- Static analysis: go vet ./...
- Module tidy check: go mod tidy (commit resulting changes)

Code style guidelines
- Imports: standard library first, then external modules, grouped and sorted; no unused imports
- Naming: CamelCase for exported identifiers, lowerCamelCase for unexported, ALL_CAPS only for well-known constants if idiomatic
- Errors: return error values, wrap with fmt.Errorf("context: %w", err); avoid panics in library-style code
- Logging: prefer log.Printf or a structured logger if added; no sensitive data in logs
- Concurrency: always pass contexts (context.Context) to I/O/DB operations; honor cancellation; avoid goroutine leaks
- Types: prefer concrete types at package boundaries; use interfaces only when multiple implementations are required
- Packages: keep packages small and focused; avoid importing internal/ private packages from other modules

Commit hygiene
- Run: go fmt ./... && go vet ./... && go test ./... before committing
- Keep go.mod/go.sum tidy; do not commit secrets (e.g., CRDB connection strings)
