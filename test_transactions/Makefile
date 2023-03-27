BINARY_NAME = testtransactions
build:
	@echo "  >  Building binary..."
	go build -o ${BINARY_NAME} main.go
run:
	@echo "  >  Run..."
	@echo "  >  Request examples: "

	@echo "  >  http://localhost:3333/add?id=1&money=150"
	@echo "  >  http://localhost:3333/sub?id=1&money=11"
	@echo "  >  http://localhost:3333/check?id=1"
	./${BINARY_NAME}
clean:
	@echo "  >  Cleaning build cache"
	go clean
	rm ${BINARY_NAME}