CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -I/opt/homebrew/include -I/usr/local/include
LDFLAGS = -L/opt/homebrew/lib -L/usr/local/lib -lpqxx
TARGET2 = query_loader
SOURCES2 = solution2.cpp

# JSON library flags
JSONFLAGS = -I/opt/homebrew/include -I/usr/local/include

$(TARGET2): $(SOURCES2)
	$(CXX) $(CXXFLAGS) $(JSONFLAGS) -o $(TARGET2) $(SOURCES2) $(LDFLAGS)

# Run solution2 with example query
run-query: $(TARGET2)
	./$(TARGET2) --query query.json

# Install JSON library dependency
install-json:
	brew install nlohmann-json

# Check compiler version
check-compiler:
	$(CXX) --version

.PHONY: clean run-query install-json check-compiler