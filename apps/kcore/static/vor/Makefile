CCOMPILE=mpic++
PLATFORM=darwin
CPPFLAGS= -I$(HADOOP_HOME)/src/c++/libhdfs -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/darwin -I ~/Desktop/pregelplus-code -I ~/Desktop/blogel-code
LIB = -L$(HADOOP_HOME)/c++/$(PLATFORM)/lib
LDFLAGS = -lhdfs -Wno-deprecated -O2

all: run

run: run.cpp
	$(CCOMPILE) run.cpp $(CPPFLAGS) $(LIB) $(LDFLAGS)  -o run

clean:
	-rm run
