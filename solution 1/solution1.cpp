// Arthor: Kangyi Zhang
// Last modified date: Oct. 23, 2025

// 0: Key Concepts Understanding
// =====================================================
// PostgreSQL: a powerful, open source object-relational database system that uses and extends the SQL language 
////////////// combined with many features that safely store and scale the most complicated data workloads. [1]
// libpqxx: the official PostgreSQL C++ client library — it lets your C++ code connect to a PostgreSQL server, 
/////////// run SQL commands, and manipulate data. [2]
// pgAdmin: a management tool for PostgreSQL and derivative relational databases such as EnterpriseDB's EDB Advanced Server. 
// gflags: The gflags package contains a C++ library that implements commandline flags processing. It includes 
////////// built-in support for standard types such as string and the ability to define flags in the source file in which they are used.
// boost::program_options: A library to obtain program options via conventional methods such as command line and config file. [3]

// 1: Data Structure
// =====================================================
// Inspection Regions: 2D points with coordinates, category, and group 
// Data Files (line i corresponds to same region):
// - points.txt: x y coordinates
// - categories.txt: category IDs
// - groups.txt: group IDs

// 2: Database Schema
// =====================================================
// Table: inspection_region
// ===============================================================
// | Column Name |   Type   | Meaning                            |
// ---------------------------------------------------------------
// | id          | BIGINT   | Unique region ID                   |
// | group_id    | BIGINT   | Which group this region belongs to |
// | coord_x     | FLOAT    | x coordinate (from points.txt)     |
// | coord_y     | FLOAT    | y coordinate (from points.txt)     |
// | category    | INTEGER  | category ID (from categories.txt)  |
// ===============================================================
// =====================================================
// Table: inspection_group                             |
// =====================================================
// | Column Name |   Type   | Meaning                  |
// -----------------------------------------------------
// | id          | BIGINT   | Unique group ID          |
// =====================================================

// 3: Program Requirements
// =====================================================
// Processing:
//  - Parse command line arguments
//  - Read and parse the three text files
//  - Establish PostgreSQL connection using libpqxx
//  - Insert data into database tables
// Technical Stack:
//  - C++ with libpqxx for PostgreSQL access
//  - JSON library (nlohmann/rapidjson)
//  - Command line parser (gflags/boost::program_options)
//  - Build system: CMake/Ninja/Makefile/Visual Studio

// 4: Setup Dependencies
// =====================================================
// - PostgreSQL DBMS installation
//      - brew install postgresql
//      - brew services start postgresql
// - pgAdmin4 for database management and debugging
//      - brew install --cask pgadmin4
// - libpqxx C++ client library
//      - brew install libpqxx

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <filesystem>
#include <pqxx/pqxx>

namespace fs = std::filesystem;

class DataLoader {
public:
    DataLoader(const std::string& conn_str) : connection_string_(conn_str) {}

    bool loadData(const std::string& data_directory) {
        try {
            // Read all three files
            std::vector<std::pair<double, double>> points = readPointsFile(data_directory + "/points.txt");
            std::vector<int> categories = readCategoriesFile(data_directory + "/categories.txt");
            std::vector<long> groups = readGroupsFile(data_directory + "/groups.txt");

            // Verify all files have same number of lines
            if (points.size() != categories.size() || points.size() != groups.size()) {
                std::cerr << "Error: Files have different number of lines" << std::endl;
                std::cerr << "Points: " << points.size() << ", Categories: " << categories.size() << ", Groups: " << groups.size() << std::endl;
                return false;
            }

            // Connect to database and load data
            return insertIntoDatabase(points, categories, groups);

        } catch (const std::exception& e) {
            std::cerr << "Error loading data: " << e.what() << std::endl;
            return false;
        }
    }

private:
    std::string connection_string_;

    std::vector<std::pair<double, double>> readPointsFile(const std::string& filename) {
        std::vector<std::pair<double, double>> points;
        std::ifstream file(filename);
        std::string line;

        if (!file.is_open()) {
            throw std::runtime_error("Cannot open file: " + filename);
        }

        while (std::getline(file, line)) {
            if (line.empty()) continue;
            
            std::istringstream iss(line);
            double x, y;
            if (iss >> x >> y) {
                points.emplace_back(x, y);
            }
        }
        std::cout << "Read " << points.size() << " points from " << filename << std::endl;
        return points;
    }

    std::vector<int> readCategoriesFile(const std::string& filename) {
        std::vector<int> categories;
        std::ifstream file(filename);
        std::string line;

        if (!file.is_open()) {
            throw std::runtime_error("Cannot open file: " + filename);
        }

        while (std::getline(file, line)) {
            if (line.empty()) continue;
            categories.push_back(std::stoi(line));
        }
        std::cout << "Read " << categories.size() << " categories from " << filename << std::endl;
        return categories;
    }

    std::vector<long> readGroupsFile(const std::string& filename) {
        std::vector<long> groups;
        std::ifstream file(filename);
        std::string line;

        if (!file.is_open()) {
            throw std::runtime_error("Cannot open file: " + filename);
        }

        while (std::getline(file, line)) {
            if (line.empty()) continue;
            groups.push_back(std::stol(line));
        }
        std::cout << "Read " << groups.size() << " groups from " << filename << std::endl;
        return groups;
    }

    bool insertIntoDatabase(const std::vector<std::pair<double, double>>& points,
                           const std::vector<int>& categories,
                           const std::vector<long>& groups) {
        try {
            pqxx::connection conn(connection_string_);
            
            if (!conn.is_open()) {
                std::cerr << "Cannot open database connection" << std::endl;
                return false;
            }

            pqxx::work txn(conn);

            // First, ensure tables exist by executing the schema
            createTables(txn);

            // Insert data - line i in all files corresponds to the same region
            for (size_t i = 0; i < points.size(); ++i) {
                // Use line number (1-based) as the region ID
                long region_id = i + 1;
                long group_id = groups[i];
                double coord_x = points[i].first;
                double coord_y = points[i].second;
                int category = categories[i];

                // First ensure the group exists
                txn.exec_params(
                    "INSERT INTO inspection_group (id) VALUES ($1) ON CONFLICT (id) DO NOTHING",
                    group_id
                );

                // Then insert the region
                txn.exec_params(
                    "INSERT INTO inspection_region (id, group_id, coord_x, coord_y, category) "
                    "VALUES ($1, $2, $3, $4, $5) ON CONFLICT (id) DO NOTHING",
                    region_id, group_id, coord_x, coord_y, category
                );
            }

            txn.commit();
            std::cout << "Successfully loaded " << points.size() << " regions into database" << std::endl;
            return true;

        } catch (const std::exception& e) {
            std::cerr << "Database error: " << e.what() << std::endl;
            return false;
        }
    }

    void createTables(pqxx::work& txn) {
        // Execute the provided schema exactly
        txn.exec(
            "CREATE TABLE IF NOT EXISTS inspection_group ("
            "    id BIGINT NOT NULL,"
            "    PRIMARY KEY (id))"
        );

        txn.exec(
            "CREATE TABLE IF NOT EXISTS inspection_region ("
            "    id BIGINT NOT NULL,"
            "    group_id BIGINT,"
            "    PRIMARY KEY (id))"
        );

        txn.exec("ALTER TABLE inspection_region ADD COLUMN IF NOT EXISTS coord_x FLOAT");
        txn.exec("ALTER TABLE inspection_region ADD COLUMN IF NOT EXISTS coord_y FLOAT");
        txn.exec("ALTER TABLE inspection_region ADD COLUMN IF NOT EXISTS category INTEGER");
        
        std::cout << "Database tables created/verified" << std::endl;
    }
};

int main(int argc, char* argv[]) {
    // Simple command line argument parsing for --data_directory
    std::string data_directory;
    
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--data_directory" && i + 1 < argc) {
            data_directory = argv[++i];
        }
    }

    if (data_directory.empty()) {
        std::cerr << "Usage: " << argv[0] << " --data_directory <path>" << std::endl;
        return 1;
    }

    if (!fs::exists(data_directory)) {
        std::cerr << "Error: data_directory does not exist: " << data_directory << std::endl;
        return 1;
    }

    // Check if required files exist
    for (const auto& file : {"points.txt", "categories.txt", "groups.txt"}) {
        if (!fs::exists(data_directory + "/" + file)) {
            std::cerr << "Error: Required file not found: " << file << std::endl;
            return 1;
        }
    }

    // Database connection parameters - adjust as needed for your setup
    std::string connection_string = "dbname=inspection_db user=postgres password=password host=localhost port=5432";
    
    try {
        DataLoader loader(connection_string);
        
        if (loader.loadData(data_directory)) {
            std::cout << "Data loading completed successfully!" << std::endl;
            return 0;
        } else {
            std::cerr << "Data loading failed!" << std::endl;
            return 1;
        }

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}


// References
// [1] https://www.postgresql.org/about/
// [2] https://libpqxx.readthedocs.io/stable/
// [3] https://cpp.libhunt.com/compare-gflags-vs-boost-program_options

