CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra

# Auto-detect include paths
PQXX_INCLUDE := $(shell pkg-config --cflags libpqxx 2>/dev/null || echo "-I/opt/homebrew/include -I/usr/local/include")
PQXX_LIB := $(shell pkg-config --libs libpqxx 2>/dev/null || echo "-L/opt/homebrew/lib -L/usr/local/lib -lpqxx")

CXXFLAGS += $(PQXX_INCLUDE)
LDFLAGS = $(PQXX_LIB)

TARGET3 = query_loader_extended
SOURCES3 = solution3.cpp

$(TARGET3): $(SOURCES3)
	$(CXX) $(CXXFLAGS) -o $(TARGET3) $(SOURCES3) $(LDFLAGS)

clean:
	rm -f $(TARGET3)

.PHONY: clean