ifeq ($(OS),Windows_NT)
  ifeq ($(shell uname -s),) # not in a bash-like shell
	CLEANUP = rmdir /S /Q
	MKDIR = mkdir
  else # in a bash-like shell, like msys
	CLEANUP = rm -r -f
	MKDIR = mkdir -p
  endif
  	MATH=
	PYTHON=python
	TARGET_EXTENSION=exe
else
	MATH = -lm
	CLEANUP = rm -r -f
	MKDIR = mkdir -p
	TARGET_EXTENSION=out
	PYTHON=python3
endif

CC  = gcc
CXX = g++

.PHONY: clean
.PHONY: test

PATHU = lib/Unity-Desktop/src/
PATHS = src/
PATH_EMBEDDB = src/embedDB/
PATHSPLINE = src/spline/
PATH_QUERY = src/query-interface/
PATH_SORT = src/query-interface/sort/
PATH_UTILITY = lib/EmbedDB-Utility/
PATH_FILE_INTERFACE = lib/Desktop-File-Interface/
PATH_DISTRIBUTION = lib/Distribution/

PATHT = test/
PATHB = build/
PATHD = build/depends/
PATHO = build/objs/
PATHR = build/results/
PATHA = build/artifacts/

BUILD_PATHS = $(PATHB) $(PATHD) $(PATHO) $(PATHR) $(PATHA)

EMBEDDB_OBJECTS = $(PATHO)embedDB.o $(PATHO)spline.o $(PATHO)embedDBUtility.o
EMBEDDB_FILE_INTERFACE = $(PATHO)desktopFileInterface.o
QUERY_OBJECTS = $(PATHO)schema.o $(PATHO)advancedQueries.o $(PATHO)activeRules.o $(SORT_OBJECTS)
SORT_OBJECTS = $(PATHO)sortWrapper.o $(PATHO)adaptive_sort.o $(PATHO)flash_minsort_sublist.o $(PATHO)flash_minsort.o $(PATHO)in_memory_sort.o $(PATHO)no_output_heap.o
EMBEDDB_DESKTOP = $(PATHO)desktopMain.o
DISTRIBUTION_OBJECTS = $(PATHO)distribution.o

DIST_TEST_OBJECTS = $(DISTRIBUTION_OBJECTS) $(EMBEDDB_FILE_INTERFACE) $(PATHO)unity.o
DEV_TEST_OBJECTS = $(EMBEDDB_OBJECTS) $(QUERY_OBJECTS) $(EMBEDDB_FILE_INTERFACE) $(SORT_OBJECTS) $(PATHO)unity.o


TEST_FLAGS = -I. -I$(PATHU) -I $(PATHS) -I$(PATH_UTILITY) -I$(PATH_FILE_INTERFACE) -D TEST
EXAMPLE_FLAGS = -I. -I$(PATHS) -I$(PATH_UTILITY) -I$(PATH_FILE_INTERFACE) -I$(PATH_DISTRIBUTION)
TEST_DIST_FLAGS = -I. -I$(PATHS) -I$(PATHU) -I$(PATH_FILE_INTERFACE) -I$(PATH_DISTRIBUTION) -I$(PATH_UTILITY) -DDIST -D TEST

override CFLAGS += $(if $(filter test-dist,$(MAKECMDGOALS)), $(TEST_DIST_FLAGS), $(if $(filter test,$(MAKECMDGOALS)),$(TEST_FLAGS),$(EXAMPLE_FLAGS)) )

SRCT = $(wildcard $(PATHT)*/*.cpp)

COMPILE_C   = $(CC)  -c
COMPILE_CPP = $(CXX) -c
LINK_C      = $(CC)
LINK_CPP    = $(CXX)
DEPEND=gcc -MM -MG -MF

RESULTS = $(patsubst $(PATHT)test%.cpp,$(PATHR)test%.testpass,$(SRCT))

build: $(BUILD_PATHS) $(PATHB)desktopMain.$(TARGET_EXTENSION)
	@echo "Running EmbedDB Desktop Build File"
	-./$(PATHB)desktopMain.$(TARGET_EXTENSION)
	@echo "Finished EmbedDB Desktop Build"

$(PATHB)desktopMain.$(TARGET_EXTENSION): $(EMBEDDB_OBJECTS) $(QUERY_OBJECTS) $(EMBEDDB_DESKTOP) $(EMBEDDB_FILE_INTERFACE)
	$(LINK_C) -o $@ $^ $(MATH)

dist: $(BUILD_PATHS) $(PATHB)distributionMain.$(TARGET_EXTENSION)
	@echo "Running EmbedDB Distribution Desktop Build File"
	-./$(PATHB)distributionMain.$(TARGET_EXTENSION)
	@echo "Finished EmbedDB Distribution Desktop Build"

$(PATHB)distributionMain.$(TARGET_EXTENSION): $(DISTRIBUTION_OBJECTS) $(EMBEDDB_DESKTOP) $(EMBEDDB_FILE_INTERFACE)
	$(LINK_C) -o $@ $^ $(MATH)

test: $(BUILD_PATHS) $(RESULTS)
	pip install -r requirements.txt -q
	$(PYTHON) ./scripts/stylize_as_junit.py

test-dist: $(BUILD_PATHS) $(RESULTS:.testpass=.dist.testpass)
	pip install -r requirements.txt -q
	$(PYTHON) ./scripts/stylize_as_junit.py

$(PATHR)%.testpass: $(PATHB)%.$(TARGET_EXTENSION)
	$(MKDIR) $(@D)
	-./$< > $@ 2>&1

$(PATHR)%.dist.testpass: $(PATHB)%.dist.$(TARGET_EXTENSION)
	$(MKDIR) $(@D)
	-./$< > $@ 2>&1

$(PATHB)test%.$(TARGET_EXTENSION): $(PATHO)test%.o $(DEV_TEST_OBJECTS)
	$(MKDIR) $(@D)
	$(LINK_CPP) -o $@ $^ $(MATH)

$(PATHB)%.dist.$(TARGET_EXTENSION): $(PATHO)%.o $(DIST_TEST_OBJECTS)
	$(MKDIR) $(@D)
	$(LINK_CPP) -o $@ $^ $(MATH)

$(PATHO)%.o:: $(PATHT)%.cpp
	$(MKDIR) $(@D)
	$(COMPILE_CPP) $(CFLAGS) $< -o $@

$(PATHO)distribution.o: $(PATH_DISTRIBUTION)embedDB.c
	$(COMPILE_C) -I$(PATH_UTILITY) -I$(PATH_FILE_INTERFACE) -DPRINT_ERRORS -I$(PATH_DISTRIBUTION) $< -o $@

$(PATHO)%.o:: $(PATHS)%.c
	$(COMPILE_C) $(CFLAGS) $< -o $@

$(PATHO)%.o:: $(PATHSPLINE)%.c
	$(COMPILE_C) $(CFLAGS) $< -o $@

$(PATHO)%.o:: $(PATH_EMBEDDB)%.c
	$(COMPILE_C) $(CFLAGS) $< -o $@

$(PATHO)%.o:: $(PATH_UTILITY)%.c
	$(COMPILE_C) $(CFLAGS) $< -o $@

$(PATHO)%.o:: $(PATH_FILE_INTERFACE)%.c
	$(COMPILE_C) $(CFLAGS) $< -o $@

$(PATHO)%.o:: $(PATH_QUERY)%.c
	$(COMPILE_C) $(CFLAGS) $< -o $@

$(PATHO)%.o:: $(PATH_SORT)%.c
	$(COMPILE_C) $(CFLAGS) $< -o $@

$(PATHO)%.o:: $(PATHU)%.c $(PATHU)%.h
	$(COMPILE_C) $(CFLAGS) $< -o $@

$(PATHB)%.$(TARGET_EXTENSION): $(PATHO)%.o $(PATHO)unity.o
	$(MKDIR) $(@D)
	$(LINK_CPP) -o $@ $^ $(MATH)

$(PATHO)%.o: $(PATHT)%.cpp
	$(MKDIR) $(@D)
	$(COMPILE_CPP) $(CFLAGS) $< -o $@

$(PATHD)%.d:: $(PATHT)%.c
	$(DEPEND) $@ $<

$(PATHB):
	$(MKDIR) $(PATHB)

$(PATHD):
	$(MKDIR) $(PATHD)

$(PATHO):
	$(MKDIR) $(PATHO)

$(PATHR):
	$(MKDIR) $(PATHR)

$(PATHA):
	$(MKDIR) $(PATHA)

clean:
	$(CLEANUP) $(PATHB)

.PRECIOUS: $(PATHB)Test%.$(TARGET_EXTENSION)
.PRECIOUS: $(PATHD)%.d
.PRECIOUS: $(PATHO)%.o
.PRECIOUS: $(PATHR)%.testpass
