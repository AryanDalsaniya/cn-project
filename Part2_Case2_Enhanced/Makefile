CXX ?= g++
CXXFLAGS ?= -O2 -std=c++17 -Wall -Wextra -Wpedantic -pthread
CPPFLAGS += -DCASE=3
.PHONY: all clean
all: server client
server: server.cpp common.hpp
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) server.cpp -o $@
client: client.cpp common.hpp
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) client.cpp -o $@
clean:
	rm -f server client
