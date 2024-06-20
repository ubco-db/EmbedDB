ifeq ($(OS),Windows_NT)
  ifeq ($(shell uname -s),) # not in a bash-like shell
	CLEANUP = del /F /Q
	MKDIR = mkdir
  else # in a bash-like shell, like msys
	CLEANUP = rm -f
	MKDIR = mkdir -p
  endif
  	MATH=
	PYTHON=python
	TARGET_EXTENSION=exe
else
	MATH = -lm
	CLEANUP = rm -f
	MKDIR = mkdir -p
	TARGET_EXTENSION=out
	PYTHON=python3
endif

.PHONY: clean
.PHONY: test

PATHU = lib/Unity/src/
PATHS = src/
PATH_EMBEDDB = src/embedDB/
PATHSPLINE = src/spline/
PATH_QUERY = src/query-interface/
PATH_UTILITY = lib/EmbedDB-Utility/
PATH_FILE_INTERFACE = lib/Desktop-File-Interface/

PATHT = test/
PATHB = build/
PATHD = build/depends/
PATHO = build/objs/
PATHR = build/results/
PATHA = build/artifacts/

BUILD_PATHS = $(PATHB) $(PATHD) $(PATHO) $(PATHR) $(PATHA)

EMBEDDB_OBJECTS = $(PATHO)embedDB.o $(PATHO)spline.o $(PATHO)radixspline.o $(PATHO)embedDBUtility.o $(PATHO)desktopFileInterface.o 

QUERY_OBJECTS = $(PATHO)schema.o $(PATHO)advancedQueries.o

TEST_FLAGS = -I. -I $(PATHU) -I $(PATHS) -I$(PATH_UTILITY) -I$(PATH_FILE_INTERFACE) -D TEST

EXAMPLE_FLAGS = -I. -I$(PATHS) -I$(PATH_UTILITY) -I$(PATH_FILE_INTERFACE) -D PRINT_ERRORS

override CFLAGS += $(if $(filter test,$(MAKECMDGOALS)),$(TEST_FLAGS),$(EXAMPLE_FLAGS))

SRCT = $(wildcard $(PATHT)*/*.cpp)

EMBEDDB_DESKTOP = $(PATHO)desktopMain.o

COMPILE=gcc -c
LINK=gcc
DEPEND=gcc -MM -MG -MF

# Strip directories and change extensions
BASES = $(notdir $(SRCT))

# Transform to results filenames
RESULTS = $(patsubst test%.cpp,$(PATHR)test%.testpass,$(BASES))

desktop: $(BUILD_PATHS) $(PATHB)desktopMain.$(TARGET_EXTENSION)
	@echo "Running Desktop File"
	-./$(PATHB)desktopMain.$(TARGET_EXTENSION)
	@echo "Finished running EmbedDB example file"

$(PATHB)desktopMain.$(TARGET_EXTENSION): $(EMBEDDB_OBJECTS) $(QUERY_OBJECTS) $(EMBEDDB_DESKTOP)
	$(LINK) -o $@ $^ $(MATH)

test: $(BUILD_PATHS) $(RESULTS)
	pip install -r requirements.txt -q
	$(PYTHON) ./scripts/stylize_as_junit.py

$(PATHR)%.testpass: $(PATHB)%.$(TARGET_EXTENSION)
	-./$< > $@ 2>&1

$(PATHB)test%.$(TARGET_EXTENSION): $(PATHO)test%.o $(EMBEDDB_OBJECTS) $(QUERY_OBJECTS) $(PATHO)unity.o #$(PATHD)Test%.d
	$(LINK) -o $@ $^ $(MATH)

$(PATHO)%.o:: $(PATHT)%.cpp
	$(COMPILE) $(CFLAGS) $< -o $@

$(PATHO)%.o:: $(PATHS)%.c
	$(COMPILE) $(CFLAGS) $< -o $@

$(PATHO)%.o:: $(PATHSPLINE)%.c
	$(COMPILE) $(CFLAGS) $< -o $@

$(PATHO)%.o:: $(PATH_EMBEDDB)%.c
	$(COMPILE) $(CFLAGS) $< -o $@

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
	$(CLEANUP) $(PATHO)*.o
	$(CLEANUP) $(PATHB)*.$(TARGET_EXTENSION)
	$(CLEANUP) $(PATHR)*.testpass
	$(CLEANUP) $(PATHA)*.png
	$(CLEANUP) $(PATHA)*.bin
	$(CLEANUP) $(PATHR)*.xml

.PRECIOUS: $(PATHB)Test%.$(TARGET_EXTENSION)
.PRECIOUS: $(PATHD)%.d
.PRECIOUS: $(PATHO)%.o
.PRECIOUS: $(PATHR)%.testpass
