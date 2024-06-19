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

PATHU = Unity/src/
PATHS = src/
PATHE = examples/
PATH_EMBEDDB = src/embedDB/
PATHSPLINE = src/spline/
PATH_QUERY = src/query-interface/

PATHT = test/
PATHB = build/
PATHD = build/depends/
PATHO = build/objs/
PATHR = build/results/
PATHA = build/artifacts/

BUILD_PATHS = $(PATHB) $(PATHD) $(PATHO) $(PATHR) $(PATHA)

EMBEDDB_OBJECTS = $(PATHO)embedDB.o $(PATHO)spline.o $(PATHO)radixspline.o $(PATHO)utilityFunctions.o 

QUERY_OBJECTS = $(PATHO)schema.o $(PATHO)advancedQueries.o

TEST_FLAGS = -I. -I $(PATHU) -I $(PATHS) -D TEST

EXAMPLE_FLAGS = -I. -I$(PATHS) -I$(PATHE) -D PRINT_ERRORS

CFLAGS = $(if $(filter test,$(MAKECMDGOALS)),$(TEST_FLAGS),$(EXAMPLE_FLAGS))

SRCT = $(wildcard $(PATHT)*.c)

EMBED_VARIABLE_EXAMPLE = $(PATHO)embedDBVariableDataExample.o
EMBEDDB_EXAMPLE = $(PATHO)embedDBExample.o
ADVANCED_QUERY = $(PATHO)advancedQueryInterfaceExample.o

COMPILE=gcc -c
LINK=gcc
DEPEND=gcc -MM -MG -MF

RESULTS = $(patsubst $(PATHT)test%.c,$(PATHR)test%.testpass,$(SRCT))

embedDBVariableExample: $(BUILD_PATHS) $(PATHB)embedDBVariableDataExample.$(TARGET_EXTENSION)
	@echo "Running EmbedDB variable data example"
	-./$(PATHB)embedDBVariableDataExample.$(TARGET_EXTENSION)
	@echo "Finished running EmbedDB variable data example"

$(PATHB)embedDBVariableDataExample.$(TARGET_EXTENSION): $(EMBEDDB_OBJECTS) $(EMBED_VARIABLE_EXAMPLE)
	$(LINK) -o $@ $^ $(MATH)

embedDBExample: $(BUILD_PATHS) $(PATHB)embedDBExample.$(TARGET_EXTENSION)
	@echo "Running EmbedDB Example"
	-./$(PATHB)embedDBExample.$(TARGET_EXTENSION)
	@echo "Finished running EmbedDB example file"

$(PATHB)embedDBExample.$(TARGET_EXTENSION): $(EMBEDDB_OBJECTS) $(EMBEDDB_EXAMPLE)
	$(LINK) -o $@ $^ $(MATH)

queryExample: $(BUILD_PATHS) $(PATHB)advancedQueryInterfaceExample.$(TARGET_EXTENSION)
	-./$(PATHB)advancedQueryInterfaceExample.$(TARGET_EXTENSION)

$(PATHB)advancedQueryInterfaceExample.$(TARGET_EXTENSION): $(EMBEDDB_OBJECTS) $(QUERY_OBJECTS) $(ADVANCED_QUERY)
	$(LINK) -o $@ $^ $(MATH)

test: $(BUILD_PATHS) $(RESULTS)
	pip install -r requirements.txt -q
	$(PYTHON) ./scripts/stylize_as_junit.py

$(PATHR)%.testpass: $(PATHB)%.$(TARGET_EXTENSION)
	-./$< > $@ 2>&1

$(PATHB)Test%.$(TARGET_EXTENSION): $(PATHO)Test%.o $(EMBEDDB_OBJECTS) $(QUERY_OBJECTS) $(PATHO)unity.o #$(PATHD)Test%.d
	$(LINK) -o $@ $^ $(MATH)

$(PATHO)%.o:: $(PATHT)%.c
	$(COMPILE) $(CFLAGS) $< -o $@

$(PATHO)%.o:: $(PATHE)%.c
	$(COMPILE) $(CFLAGS) $< -o $@

$(PATHO)%.o:: $(PATHSPLINE)%.c
	$(COMPILE) $(CFLAGS) $< -o $@

$(PATHO)%.o:: $(PATH_EMBEDDB)%.c
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
