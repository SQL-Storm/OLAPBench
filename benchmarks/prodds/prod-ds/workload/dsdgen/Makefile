CXX ?= g++
CXXFLAGS ?= -O3 -std=c++17 -Wall -Wextra -Werror -pthread
LDFLAGS ?= -lyaml-cpp -lsodium -pthread

TARGET = stringify_cpp
SOURCES = stringify_cpp.cpp

all: $(TARGET)

$(TARGET): $(SOURCES)
	$(CXX) $(CXXFLAGS) -o $(TARGET) $(SOURCES) $(LDFLAGS)

clean:
	rm -f $(TARGET)
