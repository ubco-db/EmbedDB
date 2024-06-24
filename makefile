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

.PHONY: clean
.PHONY: test

PATHU = lib/Unity-Desktop/src/
PATHS = src/
PATH_EMBEDDB = src/embedDB/
PATHSPLINE = src/spline/
PATH_QUERY = src/query-interface/
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

EMBEDDB_OBJECTS = $(PATHO)embedDB.o $(PATHO)spline.o $(PATHO)radixspline.o $(PATHO)embedDBUtility.o
EMBEDDB_FILE_INTERFACE = $(PATHO)desktopFileInterface.o
QUERY_OBJECTS = $(PATHO)schema.o $(PATHO)advancedQueries.o
EMBEDDB_DESKTOP = $(PATHO)desktopMain.o
DISTRIBUTION_OBJECTS = $(PATHO)distribution.o

TEST_FLAGS = -I. -I$(PATHU) -I $(PATHS) -I$(PATH_UTILITY) -I$(PATH_FILE_INTERFACE) -D TEST
EXAMPLE_FLAGS = -I. -I$(PATHS) -I$(PATH_UTILITY) -I$(PATH_FILE_INTERFACE) -I$(PATH_DISTRIBUTION) -DPRINT_ERRORS
TEST_DIST_FLAGS = -I. -I$(PATHU) -I$(PATH_FILE_INTERFACE) -I$(PATH_DISTRIBUTION) -DPRINT_ERRORS

override CFLAGS += $(if $(filter test-dist,$(MAKECMDGOALS)), $(TEST_DIST_FLAGS), $(if $(filter test,$(MAKECMDGOALS)),$(TEST_FLAGS),$(EXAMPLE_FLAGS)) )

SRCT = $(wildcard $(PATHT)*/*.cpp)



COMPILE=gcc -c
LINK=gcc
DEPEND=gcc -MM -MG -MF

# Transform to results filenames
RESULTS = $(patsubst $(PATHT)test%.cpp,$(PATHR)test%.testpass,$(SRCT))

build: $(BUILD_PATHS) $(PATHB)desktopMain.$(TARGET_EXTENSION)
	@echo "Running EmbedDB Desktop Build File"
	-./$(PATHB)desktopMain.$(TARGET_EXTENSION)
	@echo "Finished EmbedDB Desktop Build"

$(PATHB)desktopMain.$(TARGET_EXTENSION): $(EMBEDDB_OBJECTS) $(QUERY_OBJECTS) $(EMBEDDB_DESKTOP) $(EMBEDDB_FILE_INTERFACE)
	$(LINK) -o $@ $^ $(MATH)

dist: $(BUILD_PATHS) $(PATHB)distributionMain.$(TARGET_EXTENSION)
	@echo "Running EmbedDB Distribution Desktop Build File"
	-./$(PATHB)distributionMain.$(TARGET_EXTENSION)
	@echo "Finished EmbedDB Distribution Desktop Build"

$(PATHB)distributionMain.$(TARGET_EXTENSION): $(DISTRIBUTION_OBJECTS) $(EMBEDDB_DESKTOP) $(EMBEDDB_FILE_INTERFACE)
	$(LINK) -o $@ $^ $(MATH)

test: $(BUILD_PATHS) $(RESULTS)
	pip install -r requirements.txt -q
	$(PYTHON) ./scripts/stylize_as_junit.py

test-dist: $(BUILD_PATHS) $(RESULTS)

$(PATHR)%.testpass: $(PATHB)%.$(TARGET_EXTENSION)
	$(MKDIR) $(@D)
	-./$< > $@ 2>&1

$(PATHB)test%.$(TARGET_EXTENSION): $(PATHO)test%.o $(if $(filter test-dist,$(MAKECMDGOALS)), $(DISTRIBUTION_OBJECTS), $(EMBEDDB_OBJECTS) $(QUERY_OBJECTS)) $(EMBEDDB_FILE_INTERFACE) $(PATHO)unity.o #$(PATHD)Test%.d
	$(MKDIR) $(@D)
	$(LINK) -o $@ $^ $(MATH)

$(PATHO)%.o:: $(PATHT)%.cpp
	$(MKDIR) $(@D)
	$(COMPILE) $(CFLAGS) $< -o $@

$(PATHO)distribution.o:: $(PATH_DISTRIBUTION)embedDB.c
	$(COMPILE) $(CFLAGS) $< -o $@

$(PATHO)%.o:: $(PATHS)%.c
	$(COMPILE) $(CFLAGS) $< -o $@
	
$(PATHO)%.o:: $(PATHSPLINE)%.c
	$(COMPILE) $(CFLAGS) $< -o $@

$(PATHO)%.o:: $(PATH_EMBEDDB)%.c
	$(COMPILE) $(CFLAGS) $< -o $@

$(PATHO)%.o:: $(PATH_DISTRIBUTION)%.c
	$(COMPILE) -I$(PATH_UTILITY) -I$(PATH_FILE_INTERFACE) -DPRINT_ERRORS -I$(PATH_DISTRIBUTION) $< -o $@

$(PATHO)%.o:: $(PATH_UTILITY)%.c
	$(COMPILE) $(CFLAGS) $< -o $@

$(PATHO)%.o:: $(PATH_FILE_INTERFACE)%.c
	$(COMPILE) $(CFLAGS) $< -o $@

$(PATHO)%.o:: $(PATH_QUERY)%.c
	$(COMPILE) $(CFLAGS) $< -o $@

$(PATHO)%.o:: $(PATHU)%.c $(PATHU)%.h
	$(COMPILE) $(CFLAGS) $< -o $@

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
