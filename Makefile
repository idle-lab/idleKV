.PHONY: format format-check

CLANG_FORMAT ?= clang-format

SRC_DIRS := src
FILES := main.cc

FORMAT_FILES := $(shell find $(SRC_DIRS) -type f \( -name '*.cc' -o -name '*.h' \)) $(FILES)

format-check:
	@$(CLANG_FORMAT) --dry-run --Werror $(FORMAT_FILES)

format:
	@$(CLANG_FORMAT) -i $(FORMAT_FILES)
