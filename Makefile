.PHONY: format format-check config-release config-debug

BUILD_DIR := build
CLANG_FORMAT ?= clang-format

SRC_DIRS := src
TEST_DIRS := test

SRC_FILES := $(shell find $(SRC_DIRS) -type f \( -name '*.cc' -o -name '*.h' \))
TEST_FILES := $(shell find $(TEST_DIRS) -type f \( -name '*_test.cc' -o -name '*.h' \))

FORMAT_FILES := $(SRC_FILES) $(TEST_FILES)

# default target
all: release

format-check:
	@$(CLANG_FORMAT) --dry-run --Werror $(FORMAT_FILES)

format:
	@$(CLANG_FORMAT) -i $(FORMAT_FILES)

config-release:
	@cmake -DCMAKE_BUILD_TYPE=Release \
	       -DCMAKE_EXPORT_COMPILE_COMMANDS=TRUE \
	       -B $(BUILD_DIR) -S . -G Ninja
config-debug:
	@cmake -DCMAKE_BUILD_TYPE=Debug \
	       -DCMAKE_EXPORT_COMPILE_COMMANDS=TRUE \
	       -B $(BUILD_DIR) -S . -G Ninja

release: config-release
	@ninja idlekv -C $(BUILD_DIR) -j $(shell nproc)

debug: config-debug
	@ninja idlekv -C $(BUILD_DIR) -j $(shell nproc)

clean:
	@rm -rf $(BUILD_DIR)