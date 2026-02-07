.PHONY: format format-check

CLANG_FORMAT ?= clang-format

SRC_DIRS := src
TEST_DIRS := test

SRC_FILES := $(shell find $(SRC_DIRS) -type f \( -name '*.cc' -o -name '*.h' \))
TEST_FILES := $(shell find $(TEST_DIRS) -type f \( -name '*_test.cc' -o -name '*.h' \))

FORMAT_FILES := $(SRC_FILES) $(TEST_FILES)

format-check:
	@$(CLANG_FORMAT) --dry-run --Werror $(FORMAT_FILES)

format:
	@$(CLANG_FORMAT) -i $(FORMAT_FILES)
