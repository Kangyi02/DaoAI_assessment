CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -I/opt/homebrew/include -I/usr/local/include
LDFLAGS = -L/opt/homebrew/lib -L/usr/local/lib -lpqxx
TARGET = data_loader
SOURCES = solution1.cpp

# Default target
$(TARGET): $(SOURCES)
	$(CXX) $(CXXFLAGS) -o $(TARGET) $(SOURCES) $(LDFLAGS)

# Run with your specific data path
run: $(TARGET)
	./$(TARGET) --data_directory /Users/kyi/Downloads/assessment1/data/1

# Debug build
debug: CXXFLAGS += -g
debug: $(TARGET)

# Clean build artifacts
clean:
	rm -f $(TARGET)

# Install dependencies (run this first if needed)
deps:
	brew install postgresql libpqxx

# Check if required files exist
check-data:
	@echo "Checking if data files exist..."
	@test -f /Users/kyi/Downloads/assessment1/data/0/points.txt || echo "Missing: points.txt"
	@test -f /Users/kyi/Downloads/assessment1/data/0/categories.txt || echo "Missing: categories.txt"
	@test -f /Users/kyi/Downloads/assessment1/data/0/groups.txt || echo "Missing: groups.txt"
	@echo "Data check complete."

.PHONY: clean run debug deps check-data