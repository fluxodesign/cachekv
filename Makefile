PROTO_DIR := proto
GEN_DIR := gen

.PHONY: proto
proto:
	protoc \
	  --proto_path=$(PROTO_DIR) \
	  --go_out=$(GEN_DIR) --go_opt=paths=source_relative \
	  --go-grpc_out=$(GEN_DIR) --go-grpc_opt=paths=source_relative \
	  $(PROTO_DIR)/cachekv/v1/cachekv.proto
